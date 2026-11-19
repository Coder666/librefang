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
```

Every timeout and policy threshold is in configuration. Use `on_failure =
"suppress"` when no work should dispatch unless the plugin succeeds.
