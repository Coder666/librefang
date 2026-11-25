//! [`kernel_handle::TaskQueue`] — durable task posting / claiming /
//! completion against the memory substrate. Each lifecycle change publishes
//! a [`SystemEvent`] so triggers and dashboards observe the transition.

use std::str::FromStr;

use librefang_runtime::kernel_handle;
use librefang_types::agent::AgentId;

use super::super::LibreFangKernel;

impl LibreFangKernel {
    pub(in crate::kernel) async fn handoff_task_scope(
        &self,
        task_id: &str,
        owner: &str,
        worker: AgentId,
        manager: &str,
        note: &str,
        allow_pending: bool,
    ) -> Result<String, kernel_handle::KernelOpError> {
        use kernel_handle::KernelOpError;
        let entry = match AgentId::from_str(manager) {
            Ok(id) => self.agents.registry.get(id),
            Err(_) => self.agents.registry.find_by_name(manager),
        }
        .ok_or_else(|| KernelOpError::AgentNotFound(manager.to_owned()))?;
        if entry.id == worker
            || ["task_claim", "task_complete", "task_status"]
                .iter()
                .any(|tool| {
                    !super::super::triggers_and_workflow::task_tool_allowed(&entry.manifest, tool)
                })
            || note.trim().is_empty()
            || note.len() > 4096
        {
            return Err(KernelOpError::Conflict(
                "A split requires a separate manager with task_claim, task_status, and task_complete, plus a reason of 1 to 4096 bytes".into(),
            ));
        }
        let manager_id = entry.id.to_string();
        let (id, created) = self
            .memory
            .substrate
            .task_request_split(task_id, owner, &manager_id, note.trim(), allow_pending)
            .await?;
        if created {
            self.publish_decomposition_task(&id, "Decompose task scope", &manager_id)
                .await;
        }
        Ok(id)
    }

    async fn publish_decomposition_task(&self, id: &str, title: &str, assignee: &str) {
        use librefang_types::event::{Event, EventPayload, EventTarget, SystemEvent};
        self.publish_event(Event::new(
            AgentId::new(),
            EventTarget::Broadcast,
            EventPayload::System(SystemEvent::TaskPosted {
                task_id: id.to_owned(),
                title: title.to_owned(),
                assigned_to: Some(assignee.to_owned()),
                created_by: None,
            }),
        ))
        .await;
    }
}

#[async_trait::async_trait]
impl kernel_handle::TaskQueue for LibreFangKernel {
    async fn task_post(
        &self,
        title: &str,
        description: &str,
        assigned_to: Option<&str>,
        created_by: Option<&str>,
        opts: &kernel_handle::TaskPostOptions,
    ) -> Result<String, kernel_handle::KernelOpError> {
        use kernel_handle::KernelOpError;
        // The one place `[queue] max_depth_per_agent` / `max_depth_global` reach the enqueue.
        // Read per post rather than captured at boot so `POST /api/config/reload` moves the cap without a restart, which is how the section's other knobs already behave.
        let caps = librefang_memory::TaskQueueCaps::from(&self.config_ref().queue);

        // An assignee that resolves to nothing is rejected here rather than
        // stored: `task_claim` already refuses an unknown agent with
        // `AgentNotFound`, so accepting one at post time only produced a row
        // that stayed `pending` forever with nothing to say why (the sweeper
        // does not touch `pending`, and the assignee wake has no agent to
        // wake). Validating both ends closes that asymmetry.
        //
        // Both spellings are accepted because both are stored and matched:
        // `task_claim` matches `assigned_to` against the canonical UUID *or*
        // the display name (issue #2841), so narrowing to one here would
        // reject assignments the claim path handles correctly.
        //
        // "Known" means registered, not running: a stopped agent is accepted
        // on purpose, so a task posted for it waits for the agent to come
        // back rather than being refused because the worker happens to be
        // down at post time.
        if let Some(assignee) = assigned_to.filter(|a| !a.is_empty()) {
            let known = match AgentId::from_str(assignee) {
                Ok(parsed) => self.agents.registry.get(parsed).is_some(),
                Err(_) => self.agents.registry.find_by_name(assignee).is_some(),
            };
            if !known {
                return Err(KernelOpError::AgentNotFound(assignee.to_string()));
            }
        }

        let task_id = self
            .memory
            .substrate
            .task_post(
                title,
                description,
                assigned_to,
                created_by,
                opts.priority,
                opts.timeout_secs,
                caps,
            )
            .await
            // A depth cap being reached is the caller's answer, not a kernel fault: flattening it into `Internal` would reach the client as a scrubbed 500 and invite the retry the cap just refused.
            // `QuotaExceeded` maps to 429 in `ApiErrorResponse`.
            .map_err(|e| match e {
                quota @ KernelOpError::QuotaExceeded(_) => quota,
                other => KernelOpError::Internal(format!("Task post failed: {other}")),
            })?;

        let event = librefang_types::event::Event::new(
            AgentId::new(), // system-originated
            librefang_types::event::EventTarget::Broadcast,
            librefang_types::event::EventPayload::System(
                librefang_types::event::SystemEvent::TaskPosted {
                    task_id: task_id.clone(),
                    title: title.to_string(),
                    assigned_to: assigned_to.map(String::from),
                    created_by: created_by.map(String::from),
                },
            ),
        );
        self.publish_event(event).await;

        Ok(task_id)
    }

    async fn task_claim(
        &self,
        agent_id: &str,
    ) -> Result<Option<serde_json::Value>, kernel_handle::KernelOpError> {
        use kernel_handle::KernelOpError;
        // Resolve `agent_id` to a canonical UUID and also capture the name.
        // Both are forwarded to `memory.task_claim` so that tasks whose
        // `assigned_to` field was stored as either a UUID *or* a name string
        // are correctly matched (issue #2841).
        let (resolved, resolved_name) = match librefang_types::agent::AgentId::from_str(agent_id) {
            Ok(parsed_id) => {
                // Caller passed a UUID — look up the name from the registry.
                let name = self.agents.registry.get(parsed_id).map(|e| e.name.clone());
                (agent_id.to_string(), name)
            }
            Err(_) => match self.agents.registry.find_by_name(agent_id) {
                Some(entry) => (entry.id.to_string(), Some(agent_id.to_string())),
                None => {
                    return Err(KernelOpError::AgentNotFound(agent_id.to_string()));
                }
            },
        };
        let result = self
            .memory
            .substrate
            .task_claim(&resolved, resolved_name.as_deref())
            .await
            .map_err(|e| KernelOpError::Internal(format!("Task claim failed: {e}")))?;

        if let Some(ref task) = result {
            let task_id = task["id"].as_str().unwrap_or("").to_string();
            // The claimed task record carries the original poster so
            // `TaskClaimed` triggers can scope via `creator_match`. The
            // substrate stores an unset creator as the empty string — map
            // that to `None` so an empty value never satisfies a filter.
            let created_by = task["created_by"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from);
            let event = librefang_types::event::Event::new(
                AgentId::new(), // system-originated
                librefang_types::event::EventTarget::Broadcast,
                librefang_types::event::EventPayload::System(
                    librefang_types::event::SystemEvent::TaskClaimed {
                        task_id,
                        claimed_by: resolved.clone(),
                        created_by,
                    },
                ),
            );
            self.publish_event(event).await;
        }

        Ok(result)
    }

    async fn task_complete(
        &self,
        agent_id: &str,
        task_id: &str,
        result: &str,
    ) -> Result<(), kernel_handle::KernelOpError> {
        use kernel_handle::KernelOpError;
        let resolved = match librefang_types::agent::AgentId::from_str(agent_id) {
            Ok(_) => agent_id.to_string(),
            Err(_) => match self.agents.registry.find_by_name(agent_id) {
                Some(entry) => entry.id.to_string(),
                None => {
                    return Err(KernelOpError::AgentNotFound(agent_id.to_string()));
                }
            },
        };
        // Capture the original poster before completing so `TaskCompleted`
        // triggers can scope via `creator_match`. `task_complete` only flips
        // status/result, so reading the record first is equivalent to reading
        // it after. The substrate stores an unset creator as the empty string
        // — map that to `None` so an empty value never satisfies a filter.
        let created_by = self
            .memory
            .substrate
            .task_get(task_id)
            .await
            .ok()
            .flatten()
            .and_then(|task| {
                task["created_by"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(String::from)
            });

        // The substrate only completes a row whose claim `resolved` holds, so another agent (or a stale worker whose claim the sweeper re-issued) cannot overwrite the result.
        // Its typed misses are the caller's answer, not a kernel fault: pass them through so they reach the client as 404 / 409 instead of a scrubbed 500.
        self.memory
            .substrate
            .task_complete(task_id, &resolved, result)
            .await
            .map_err(|e| match e {
                typed @ (KernelOpError::Conflict(_) | KernelOpError::ResourceNotFound { .. }) => {
                    typed
                }
                other => KernelOpError::Internal(format!("Task complete failed: {other}")),
            })?;

        let event = librefang_types::event::Event::new(
            AgentId::new(), // system-originated
            librefang_types::event::EventTarget::Broadcast,
            librefang_types::event::EventPayload::System(
                librefang_types::event::SystemEvent::TaskCompleted {
                    task_id: task_id.to_string(),
                    completed_by: resolved,
                    result: result.to_string(),
                    created_by,
                },
            ),
        );
        self.publish_event(event).await;

        Ok(())
    }

    async fn task_request_split(
        &self,
        agent_id: &str,
        task_id: &str,
        note: &str,
    ) -> Result<String, kernel_handle::KernelOpError> {
        use kernel_handle::KernelOpError;
        let worker = AgentId::from_str(agent_id)
            .map_err(|_| KernelOpError::AgentNotFound(agent_id.to_owned()))?;
        let manager = self
            .config_ref()
            .task_board
            .scheduler_plugin
            .as_ref()
            .and_then(|plugin| plugin.decomposition_manager.clone())
            .ok_or_else(|| KernelOpError::Conflict("No decomposition manager configured".into()))?;
        self.handoff_task_scope(task_id, agent_id, worker, &manager, note, false)
            .await
    }

    async fn task_finish_split(
        &self,
        agent_id: &str,
        ticket_id: &str,
        children: Vec<kernel_handle::TaskSplitChild>,
    ) -> Result<Vec<String>, kernel_handle::KernelOpError> {
        use kernel_handle::KernelOpError;
        if children.is_empty()
            || children.len() > 8
            || children.iter().any(|child| {
                [
                    (child.title.as_str(), 200),
                    (child.description.as_str(), 12000),
                    (child.acceptance_check.as_str(), 4000),
                ]
                .iter()
                .any(|(value, max)| value.trim().is_empty() || value.len() > *max)
            })
        {
            return Err(KernelOpError::Conflict(
                "Supply one to eight children with a title, description, and acceptance check"
                    .into(),
            ));
        }
        let children = children
            .into_iter()
            .map(|child| {
                (
                    child.title,
                    format!(
                        "{}\n\nAcceptance check: {}",
                        child.description, child.acceptance_check
                    ),
                )
            })
            .collect();
        let caps = librefang_memory::TaskQueueCaps::from(&self.config_ref().queue);
        let (ids, created) = self
            .memory
            .substrate
            .task_finish_split(ticket_id, agent_id, children, caps)
            .await?;
        if created {
            for id in &ids {
                if let Ok(Some(task)) = self.memory.substrate.task_get(id).await {
                    if let (Some(title), Some(assignee)) =
                        (task["title"].as_str(), task["assigned_to"].as_str())
                    {
                        self.publish_decomposition_task(id, title, assignee).await;
                    }
                }
            }
        }
        Ok(ids)
    }

    async fn task_list(
        &self,
        status: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, kernel_handle::KernelOpError> {
        self.memory
            .substrate
            .task_list(status)
            .await
            .map_err(|e| kernel_handle::KernelOpError::Internal(format!("Task list failed: {e}")))
    }

    /// Overrides the trait's filter-and-truncate default by pushing `WHERE` / `LIMIT` / `OFFSET` into the statement, so a paged request costs one materialised row per row returned rather than one per row in the table (#8219).
    async fn task_list_page(
        &self,
        status: Option<&str>,
        assigned_to: Option<&str>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<(Vec<serde_json::Value>, u64), kernel_handle::KernelOpError> {
        self.memory
            .substrate
            .task_list_page(status, assigned_to, limit, offset)
            .await
            .map_err(|e| kernel_handle::KernelOpError::Internal(format!("Task list failed: {e}")))
    }

    /// Overrides the trait's list-and-count default with a `GROUP BY`, so the summary costs one row per distinct status instead of one per task.
    async fn task_status_counts(&self) -> Result<Vec<(String, u64)>, kernel_handle::KernelOpError> {
        self.memory
            .substrate
            .task_status_counts()
            .await
            .map_err(|e| {
                kernel_handle::KernelOpError::Internal(format!("Task status counts failed: {e}"))
            })
    }

    async fn task_delete(&self, task_id: &str) -> Result<bool, kernel_handle::KernelOpError> {
        self.memory
            .substrate
            .task_delete(task_id)
            .await
            .map_err(|e| kernel_handle::KernelOpError::Internal(format!("Task delete failed: {e}")))
    }

    async fn task_retry(&self, task_id: &str) -> Result<bool, kernel_handle::KernelOpError> {
        self.memory
            .substrate
            .task_retry(task_id)
            .await
            .map_err(|e| kernel_handle::KernelOpError::Internal(format!("Task retry failed: {e}")))
    }

    async fn task_get(
        &self,
        task_id: &str,
    ) -> Result<Option<serde_json::Value>, kernel_handle::KernelOpError> {
        self.memory
            .substrate
            .task_get(task_id)
            .await
            .map_err(|e| kernel_handle::KernelOpError::Internal(format!("Task get failed: {e}")))
    }

    async fn task_update_status(
        &self,
        task_id: &str,
        new_status: &str,
    ) -> Result<bool, kernel_handle::KernelOpError> {
        self.memory
            .substrate
            .task_update_status(task_id, new_status)
            .await
            .map_err(|e| {
                kernel_handle::KernelOpError::Internal(format!("Task update status failed: {e}"))
            })
    }
}
