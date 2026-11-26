## Scope and signal

- Identify concrete defects or maintainability risks. Cite the changed code or relevant pinned source and include enough reasoning that a human reviewer can confirm or dismiss the finding quickly.
- Keep the review focused on the pull request's stated scope. Flag unrelated changes, opportunistic cleanup, and drive-by refactors when they make the PR harder to review or risk changing behavior outside the stated goal.
- Do not speculate. If the changed code does not demonstrate a real problem and pinned source does not prove one, withhold the finding.
- Do not report formatting, import ordering, Spotless-only issues, or subjective style preferences unless the issue affects behavior, maintainability, or an enforced repository rule.

## Elasticsearch focus areas

Prioritize findings that affect correctness, long-term maintainability, operational reliability, security boundaries, or test reliability. In particular, look for accidental behavior changes during refactors, wrong ordering/timing/lifecycle assumptions, missing edge cases, stale configuration or muted-test entries, fixes that overshoot their stated scope, overly broad names/globs/privileges, temporary code without a clear cleanup path, and tests that do not exercise the changed behavior.

Verify that important context is recorded where future maintainers will find it: PR description, code comments, docs, release notes, changelog, or TODOs. Do not rely on review discussion as the only durable record for important reasoning.

## PR description and documentation

- Report a stale or misleading PR title or description when it materially hinders review, backporting, release-note decisions, or future archaeology.
- Report typos or documentation issues when they affect user-visible docs, generated docs, release notes, technical accuracy, or PR clarity.
- Do not report subjective documentation style preferences or minor wording polish that does not affect understanding.
