# Completion-first task scheduler plugin

This reference policy keeps the number of active tasks bounded, selects agents
round-robin, and routes broad tasks through a decomposition-only turn. LibreFang
validates every returned agent/task pair against the eligible candidates before
dispatch.

Configure LibreFang:

```toml
[task_board.scheduler_plugin]
command = "python3"
args = ["/opt/librefang/plugins/completion_first.py", "--config", "/etc/librefang/completion-first.json"]
timeout_secs = 30
max_response_bytes = 1048576
on_failure = "builtin"
decomposition_manager = "project-manager"
```

Every timeout and policy threshold is in configuration. Use `on_failure =
"suppress"` when no work should dispatch unless the plugin succeeds.


Set `decomposition_manager` to a registered agent with `task_claim`,
`task_status`, and `task_complete`. A plugin `split` decision on a pending
task creates a persistent ticket for that manager. A worker that has already
claimed a task can hand it off by calling `task_complete` with `task_id` and
`split_reason`. The reason should include any work already done.

The parent moves to `needs_split` and no longer occupies a claim. The manager
claims the ticket, reviews the original task, and sends `split_children` in
one `task_complete` call on the ticket. Each child needs a title, description,
and acceptance check. LibreFang saves all children and moves the parent to
`superseded` in one transaction. The children inherit the parent’s assignee.
Repeating a successful submission returns the saved child IDs. Failed tickets
can be retried from the board with the manager assignment intact.

When `decomposition_manager` is omitted, a plugin `split` continues to send
the original decomposition prompt to the worker.
