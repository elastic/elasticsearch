# Elasticsearch review guidance

- Require every pull request to be the minimal changeset necessary to complete its stated issue. Flag unrelated changes, opportunistic cleanup, and other work outside that scope.
- Prefer designs consistent with the surrounding package. Do not suggest drive-by refactors.
- For `Writeable` and transport changes, require a named `TransportVersion.fromName(...)` and version guards as described in `AGENTS.md`.
- Do not add entitlement-policy entries without a concrete justification, ideally an observed `NotEntitledException`.
- Prefer real test classes over mocks. Flag new mocks that are not documented.
- Use parameterized logging only. Flag string concatenation in log calls.
- Do not report formatting, import order, or Spotless issues.
