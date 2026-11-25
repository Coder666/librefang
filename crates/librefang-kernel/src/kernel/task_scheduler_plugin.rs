//! External policy seam for task-board wake scheduling.
//!
//! The kernel remains responsible for eligibility, persistence and dispatch.
//! A plugin may only select and order eligible candidates, or change a wake
//! into a decomposition request. This keeps an untrusted policy executable
//! from inventing task or agent identifiers.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::process::Stdio;
use std::time::Duration;

use librefang_types::config::TaskSchedulerPluginConfig;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::triggers::{TriggerMatch, TriggerMatchSource};

const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize)]
pub(super) struct SchedulerCandidate {
    pub agent_id: String,
    pub task_id: String,
    pub title: String,
    pub description: String,
    pub pending_count: usize,
}

#[derive(Debug, Serialize)]
struct SchedulerRequest<'a> {
    protocol_version: u32,
    status_counts: &'a BTreeMap<String, u64>,
    candidates: &'a [SchedulerCandidate],
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerResponse {
    protocol_version: u32,
    decisions: Vec<SchedulerDecision>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerDecision {
    agent_id: String,
    task_id: String,
    action: SchedulerAction,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SchedulerAction {
    Wake,
    Split,
}

pub(super) fn candidates(
    matches: &[TriggerMatch],
    pending_tasks: &[serde_json::Value],
) -> Vec<SchedulerCandidate> {
    let details: HashMap<&str, (&str, &str)> = pending_tasks
        .iter()
        .filter_map(|task| {
            Some((
                task["id"].as_str()?,
                (
                    task["title"].as_str().unwrap_or(""),
                    task["description"].as_str().unwrap_or(""),
                ),
            ))
        })
        .collect();
    matches
        .iter()
        .filter_map(|matched| match &matched.source {
            TriggerMatchSource::TaskBoardAssigneeWake { task_id } => {
                let (title, description) =
                    details.get(task_id.as_str()).copied().unwrap_or(("", ""));
                Some(SchedulerCandidate {
                    agent_id: matched.agent_id.to_string(),
                    task_id: task_id.clone(),
                    title: title.to_string(),
                    description: description.to_string(),
                    pending_count: parse_pending_count(&matched.message).unwrap_or(1),
                })
            }
            TriggerMatchSource::Registered(_) => None,
        })
        .collect()
}

fn parse_pending_count(message: &str) -> Option<usize> {
    message.split_whitespace().find_map(|word| {
        word.trim_matches(|c: char| !c.is_ascii_digit())
            .parse()
            .ok()
    })
}

pub(super) async fn apply(
    config: &TaskSchedulerPluginConfig,
    matches: Vec<TriggerMatch>,
    pending_tasks: &[serde_json::Value],
    status_counts: &BTreeMap<String, u64>,
) -> Result<Vec<TriggerMatch>, String> {
    validate_config(config)?;
    let scheduler_candidates = candidates(&matches, pending_tasks);
    if scheduler_candidates.is_empty() {
        return Ok(matches);
    }

    let request = SchedulerRequest {
        protocol_version: PROTOCOL_VERSION,
        status_counts,
        candidates: &scheduler_candidates,
    };
    let input = serde_json::to_vec(&request)
        .map_err(|error| format!("cannot serialize scheduler request: {error}"))?;

    let mut child = tokio::process::Command::new(&config.command)
        .args(&config.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("cannot start scheduler plugin: {error}"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or("scheduler plugin stdin unavailable")?;
    let stdout = child
        .stdout
        .take()
        .ok_or("scheduler plugin stdout unavailable")?;
    let response_limit = config.max_response_bytes;

    let exchange = async move {
        let send_request = async move {
            stdin
                .write_all(&input)
                .await
                .map_err(|error| format!("cannot write scheduler request: {error}"))
        };
        let read_response = async move {
            let mut output = Vec::new();
            stdout
                .take(response_limit.saturating_add(1))
                .read_to_end(&mut output)
                .await
                .map_err(|error| format!("cannot read scheduler response: {error}"))?;
            if output.len() as u64 > response_limit {
                return Err(format!(
                    "scheduler plugin response exceeded configured limit of {response_limit} bytes"
                ));
            }
            Ok(output)
        };
        let (_, output) = tokio::try_join!(send_request, read_response)?;
        let status = child
            .wait()
            .await
            .map_err(|error| format!("cannot wait for scheduler plugin: {error}"))?;
        if !status.success() {
            return Err(format!("scheduler plugin exited with {status}"));
        }
        Ok::<_, String>(output)
    };

    let output = tokio::time::timeout(Duration::from_secs(config.timeout_secs), exchange)
        .await
        .map_err(|_| {
            format!(
                "scheduler plugin exceeded configured timeout of {} seconds",
                config.timeout_secs
            )
        })??;
    apply_response(&matches, &output)
}

fn validate_config(config: &TaskSchedulerPluginConfig) -> Result<(), String> {
    if config.command.trim().is_empty() {
        return Err("scheduler plugin command is empty".into());
    }
    if config.timeout_secs == 0 {
        return Err("scheduler plugin timeout_secs must be greater than zero".into());
    }
    if config.max_response_bytes == 0 {
        return Err("scheduler plugin max_response_bytes must be greater than zero".into());
    }
    Ok(())
}

fn apply_response(matches: &[TriggerMatch], output: &[u8]) -> Result<Vec<TriggerMatch>, String> {
    let response: SchedulerResponse = serde_json::from_slice(output)
        .map_err(|error| format!("invalid scheduler plugin response: {error}"))?;
    if response.protocol_version != PROTOCOL_VERSION {
        return Err(format!(
            "unsupported scheduler plugin protocol version {}",
            response.protocol_version
        ));
    }

    let available: HashMap<(String, String), &TriggerMatch> = matches
        .iter()
        .filter_map(|matched| match &matched.source {
            TriggerMatchSource::TaskBoardAssigneeWake { task_id } => {
                Some(((matched.agent_id.to_string(), task_id.clone()), matched))
            }
            TriggerMatchSource::Registered(_) => None,
        })
        .collect();
    let mut seen = HashSet::new();
    let mut selected = Vec::with_capacity(response.decisions.len());
    for decision in response.decisions {
        let key = (decision.agent_id, decision.task_id);
        if !seen.insert(key.clone()) {
            return Err(format!(
                "scheduler plugin returned duplicate decision for agent {} task {}",
                key.0, key.1
            ));
        }
        let original = available.get(&key).ok_or_else(|| {
            format!(
                "scheduler plugin selected ineligible agent {} task {}",
                key.0, key.1
            )
        })?;
        let mut matched = (*original).clone();
        if matches!(decision.action, SchedulerAction::Split) {
            matched.message = split_prompt(&key.1);
        }
        selected.push(matched);
    }
    Ok(selected)
}

fn split_prompt(task_id: &str) -> String {
    format!(
        "Task {task_id} is too broad for reliable execution. Claim only this task, break it into small independently verifiable child tasks with explicit acceptance criteria and dependencies, post those child tasks to the Task Board, then complete this parent task with the child task IDs. Do not implement the parent in this turn."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use librefang_types::agent::AgentId;

    fn matched(agent: AgentId, task: &str) -> TriggerMatch {
        TriggerMatch {
            agent_id: agent,
            message: "3 pending tasks".into(),
            session_mode_override: None,
            workflow_id: None,
            source: TriggerMatchSource::TaskBoardAssigneeWake {
                task_id: task.into(),
            },
        }
    }

    fn response(agent: AgentId, task: &str, action: &str) -> Vec<u8> {
        format!(
            r#"{{"protocol_version":1,"decisions":[{{"agent_id":"{agent}","task_id":"{task}","action":"{action}"}}]}}"#
        )
        .into_bytes()
    }

    #[test]
    fn rejects_empty_command() {
        assert!(validate_config(&TaskSchedulerPluginConfig::default()).is_err());
    }

    #[test]
    fn rejects_zero_timeout() {
        let mut config = TaskSchedulerPluginConfig::default();
        config.command = "plugin".into();
        config.timeout_secs = 0;
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn rejects_zero_output_limit() {
        let mut config = TaskSchedulerPluginConfig::default();
        config.command = "plugin".into();
        config.max_response_bytes = 0;
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn valid_wake_is_selected() {
        let agent = AgentId::new();
        let input = vec![matched(agent, "task-a")];
        let result = apply_response(&input, &response(agent, "task-a", "wake")).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].message, "3 pending tasks");
    }

    #[test]
    fn split_replaces_prompt() {
        let agent = AgentId::new();
        let input = vec![matched(agent, "task-a")];
        let result = apply_response(&input, &response(agent, "task-a", "split")).unwrap();
        assert!(result[0].message.contains("break it into small"));
        assert!(result[0].message.contains("task-a"));
    }

    #[test]
    fn empty_decisions_intentionally_suppress_cycle() {
        let agent = AgentId::new();
        let input = vec![matched(agent, "task-a")];
        let result = apply_response(&input, br#"{"protocol_version":1,"decisions":[]}"#).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn rejects_unknown_candidate() {
        let agent = AgentId::new();
        let input = vec![matched(agent, "task-a")];
        assert!(apply_response(&input, &response(agent, "task-b", "wake")).is_err());
    }

    #[test]
    fn rejects_duplicate_candidate() {
        let agent = AgentId::new();
        let input = vec![matched(agent, "task-a")];
        let one = format!(r#"{{"agent_id":"{agent}","task_id":"task-a","action":"wake"}}"#);
        let output = format!(r#"{{"protocol_version":1,"decisions":[{one},{one}]}}"#);
        assert!(apply_response(&input, output.as_bytes()).is_err());
    }

    #[test]
    fn rejects_unknown_fields() {
        let input = Vec::new();
        assert!(apply_response(
            &input,
            br#"{"protocol_version":1,"decisions":[],"surprise":true}"#,
        )
        .is_err());
    }

    #[test]
    fn rejects_protocol_version_mismatch() {
        let input = Vec::new();
        assert!(apply_response(&input, br#"{"protocol_version":2,"decisions":[]}"#).is_err());
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(apply_response(&[], br#"{"protocol_version":1"#).is_err());
    }

    #[test]
    fn rejects_unknown_action() {
        let agent = AgentId::new();
        let input = vec![matched(agent, "task-a")];
        assert!(apply_response(&input, &response(agent, "task-a", "invent")).is_err());
    }

    #[test]
    fn preserves_plugin_decision_order() {
        let first_agent = AgentId::new();
        let second_agent = AgentId::new();
        let input = vec![
            matched(first_agent, "task-a"),
            matched(second_agent, "task-b"),
        ];
        let output = format!(
            r#"{{"protocol_version":1,"decisions":[{{"agent_id":"{second_agent}","task_id":"task-b","action":"wake"}},{{"agent_id":"{first_agent}","task_id":"task-a","action":"wake"}}]}}"#
        );
        let selected = apply_response(&input, output.as_bytes()).unwrap();
        assert_eq!(selected[0].agent_id, second_agent);
        assert_eq!(selected[1].agent_id, first_agent);
    }

    #[test]
    fn candidates_ignore_registered_triggers() {
        let agent = AgentId::new();
        let mut registered = matched(agent, "task-a");
        registered.source = TriggerMatchSource::Registered(crate::triggers::TriggerId::new());
        assert!(candidates(&[registered], &[]).is_empty());
    }

    #[test]
    fn candidates_include_task_text() {
        let agent = AgentId::new();
        let tasks = vec![serde_json::json!({
            "id": "task-a",
            "title": "Small change",
            "description": "One file"
        })];
        let result = candidates(&[matched(agent, "task-a")], &tasks);
        assert_eq!(result[0].title, "Small change");
        assert_eq!(result[0].description, "One file");
        assert_eq!(result[0].pending_count, 3);
    }

    #[cfg(unix)]
    fn executable_plugin(
        body: &str,
        args: Vec<String>,
    ) -> (tempfile::TempDir, TaskSchedulerPluginConfig) {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("scheduler-plugin");
        std::fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = TaskSchedulerPluginConfig {
            command: path.to_string_lossy().into_owned(),
            args,
            timeout_secs: 2,
            max_response_bytes: 1024,
            ..TaskSchedulerPluginConfig::default()
        };
        (directory, config)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_protocol_round_trip() {
        let agent = AgentId::new();
        let payload = String::from_utf8(response(agent, "task-a", "wake")).unwrap();
        let (_directory, config) =
            executable_plugin("cat >/dev/null\nprintf '%s' \"$1\"", vec![payload]);
        let selected = apply(
            &config,
            vec![matched(agent, "task-a")],
            &[],
            &BTreeMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(selected.len(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_can_write_before_consuming_a_large_request() {
        let agent = AgentId::new();
        let payload = String::from_utf8(response(agent, "task-a", "wake")).unwrap();
        let (_directory, mut config) = executable_plugin(
            "head -c 262144 /dev/zero | tr '\\000' ' '\ncat >/dev/null\nprintf '%s' \"$1\"",
            vec![payload],
        );
        config.max_response_bytes = 512 * 1024;
        let pending =
            [serde_json::json!({ "id": "task-a", "description": "x".repeat(256 * 1024) })];
        let selected = apply(
            &config,
            vec![matched(agent, "task-a")],
            &pending,
            &BTreeMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(selected.len(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_timeout_is_reported() {
        let agent = AgentId::new();
        let (_directory, mut config) = executable_plugin("cat >/dev/null\nsleep 2", vec![]);
        config.timeout_secs = 1;
        let error = apply(
            &config,
            vec![matched(agent, "task-a")],
            &[],
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(error.contains("configured timeout"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_output_limit_is_enforced() {
        let agent = AgentId::new();
        let (_directory, mut config) =
            executable_plugin("printf '12345678901'\nexec sleep 10", vec![]);
        config.max_response_bytes = 10;
        let error = apply(
            &config,
            vec![matched(agent, "task-a")],
            &[],
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(error.contains("configured limit"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_nonzero_exit_is_reported() {
        let agent = AgentId::new();
        let (_directory, config) = executable_plugin("cat >/dev/null\nexit 7", vec![]);
        let error = apply(
            &config,
            vec![matched(agent, "task-a")],
            &[],
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(error.contains("exited with"));
    }
}
