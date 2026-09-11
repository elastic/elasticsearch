# Configuring coverage

Three independent things decide what happens: **when it runs**, **what it measures**, and **which
tests it runs**. Each is configured in one place, and each has its own syntax. Nothing is hardcoded
in the scripts.

One area per file. `test-coverage.yml` measures ES|QL; another area is another file with a different
label and a different scope, sharing the same scripts.

---

## 1 · When it runs — the pipeline's `config:` block

```yaml
config:
  allow-labels: test-coverage
  trigger-phrase: '.*run\W+elasticsearch-ci/test-coverage.*'
  touched-regions:
    - ^x-pack/plugin/esql
    - ^libs/arrow/
```

**`allow-labels`** is the trigger. Nothing runs without it. Apply the label to a PR, or comment the
trigger phrase.

**`touched-regions`** narrows *where* it is offered: the pipeline appears only if **any** changed
file matches. Omit it and the label works on any PR.

> **`touched-regions` is not `included-regions`.** `included-regions` requires **every** changed
> file to match — it detects docs-only-style PRs. `touched-regions` matches if **any** does, which
> is what an area-scoped pipeline wants. Both exist; pick the one that means what you intend.

**Include the coverage tooling's own paths.** Otherwise a PR that only changes the coverage scripts
is never offered the pipeline that measures them.

### Label timing — the one real gotcha

CI starts within seconds of a PR being opened, and the generator reads labels **at that moment**. A
label applied afterwards is not seen by a build already running, and the step simply will not
appear. Either label before opening, or re-trigger with a comment or a push.

---

## 2 · What it measures — the pipeline's `env:` block

```yaml
env:
  COVERAGE_PROJECTS: ":x-pack:plugin:esql*"
  COVERAGE_INCLUDES: "org.elasticsearch.xpack.esql.*:org.elasticsearch.compute.*"
```

**`COVERAGE_PROJECTS`** — a Gradle project-path pattern. Decides which projects are instrumented and
whose tests run.

**`COVERAGE_INCLUDES`** — a JaCoCo class-include pattern, colon-separated. Decides which classes are
recorded, and therefore the denominator.

Each is its own tool's native syntax. There is no coverage DSL.

**These two are independent, and that is useful.** Measure engine classes while running data-source
tests and you learn what data-source tests exercise in the engine. Measure data-source classes while
running engine tests and you learn how much of data-sources is covered by somebody else's suites —
which is how a module's coverage can look low while its code is well tested elsewhere.

**But for a headline number they must agree.** Running data-source projects against an ES|QL-wide
class filter puts engine classes in the denominator that those tests never touch. The number drops,
and it reads as a coverage problem when it is a scoping mistake.

**`COVERAGE_EXCLUDE_PROJECTS`** removes projects from the scope. Defaults to excluding parquet-rs,
which is being retired.

---

## 3 · Which tests run — layers

| Layer | Tasks | Where product code runs |
|---|---|---|
| `unit` | `test` | the forked test JVM |
| `internal-cluster` | `internalClusterTest` | the forked test JVM (cluster is in-process) |
| `cluster` | `javaRestTest`, `yamlRestTest`, `csvSpecTests`, `javaRestTestSecure` | **separate ES node processes** |

All three run by default, one leg each, in parallel. Set `COVERAGE_LAYERS` on the build to select —
`unit,internal-cluster` for the fast pair. Matrices are static, so unselected legs exit immediately
rather than measuring.

**Task names come from Gradle, not from a list.** A guessed list was wrong four ways: 5 `csvSpecTests`
instead of 7, zero `yamlRestTest` instead of 4, one `internalClusterTest` instead of 2, and three task
types missed entirely. **A task the layer mapping does not cover fails the run** rather than being
silently skipped.

**Excluded by default**, each with a reason: BWC-versioned suites and `bcUpgradeTest` (old-version
nodes, whose coverage cannot map onto current classfiles), and `perfSmokeTest` (measures speed, not
behaviour).

---

## What you get back

Four reports from one run: one per layer, plus **merged**.

**Layers are merged, never averaged.** A line covered by two layers is one covered line, so the union
is computed from the execution data itself. On `esql-datasource-s3`: unit 69.1%, cluster 47.0%,
merged **78.7%**. Adding gives 116%, averaging 58%; both are wrong.

Differencing a layer against merged answers *"if we dropped this suite, what goes dark"*.

The merged step runs with `allow_dependency_failure`, so a failed leg still yields a partial result,
reported as a lower bound rather than lost.

---

## Two rules it is built around

**Report, never gate.** No thresholds, no failing a build on a number. A coverage gate is satisfiable
by tests that execute without asserting, which is the wrong pressure to create.

**Fail loudly on zero.** An exec file recording nothing fails the build before anything publishes. An
empty file reports as 0% and reads as a finding when it actually means the instrument is broken. For
the cluster layer the check distinguishes *no agent connected* from *connected but recorded nothing*,
because those have different fixes.

---

## Adding another area

Copy `test-coverage.yml`, then change three things: the label in `allow-labels`, the paths in
`touched-regions`, and the scope in `env:`. Create the label. Nothing else — no script changes.
