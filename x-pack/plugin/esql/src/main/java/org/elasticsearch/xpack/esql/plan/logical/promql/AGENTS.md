# PromQL planning

If you add support for a PromQL construct, or add a new restriction or rejection (a `ParsingException` or
unsupported-construct error in the parser or in these plan classes), update the list of unsupported constructs in
`docs/reference/query-languages/promql/_snippets/promql-unsupported-constructs.md`. See
`docs/reference/query-languages/promql/AGENTS.md` for how that list is used and maintained.
