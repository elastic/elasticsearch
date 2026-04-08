# ES|QL Ergonomics: Issue-by-Issue Analysis

**Date**: 2026-04-07
**Scope**: All issues tagged `ES|QL-ergonomics` plus similar untagged issues (excluding unmapped fields)

---

## The Tension

ES|QL was designed as a typed, SQL-family language. A growing user base — primarily from SPL and KQL — finds the type strictness and NULL semantics surprising and counterproductive. The question for each issue below is: does fixing this improve the language for everyone, or does it bend the language toward one audience at the expense of another?

---

## NULL and Three-Value Logic

### #138512 — Alternative to 3-value logic in boolean operators

This is the single most impactful ergonomics issue. In SQL 3-value logic, `WHERE user != 'lvovitch'` silently excludes rows where `user` is NULL, because `NULL != 'lvovitch'` evaluates to NULL, and NULL in WHERE is treated as false. SPL users expect those rows to be included — to them, a missing field is "not equal to" any specific value.

The impact is real and measurable. Users report this as "data loss" and "correctness bugs." The issue explicitly acknowledges we don't want to relitigate whether 3-value logic is correct — it is. The question is whether we can offer a safe alternative.

**Trade-offs**: Changing `!=` globally would break every SQL user's expectations and make ES|QL inconsistent with every SQL database. But doing nothing means every SPL migrant has to learn to write `WHERE (user != 'lvovitch') OR (user IS NULL)` for every single comparison, which is both verbose and error-prone — they'll forget the NULL guard and silently lose data.

**Opinion**: Two-phase approach. First, add `IS` / `IS NOT` operators in core (SQLite and PostgreSQL's `IS DISTINCT FROM` precedent). These are null-safe comparisons that don't change existing `==`/`!=` behavior. `WHERE user IS NOT 'lvovitch'` includes NULL rows. This is a clean, non-breaking addition that any user can learn. Second, for relaxed mode, add `SET null_comparison = "two_value"` that rewrites `!=` to be null-safe under the hood. This is the SPL experience without changing the core language.

The IS/IS NOT work is ~1w and should ship in core. The SET option is ~2w additional and should be gated behind relaxed mode since it fundamentally alters SQL semantics.

**Related untagged issues**: #142537 (NULL type in expressions — `null + 1` errors), #141383 (nulls in arrays not representable), #140575 (systemic NULL handling gaps). These are all symptoms of the same underlying tension. They don't need separate fixes if IS/IS NOT and the SET option ship.

---

### #145786 — Partial stats merges / NULL in GROUP BY

The user tried to replicate `INLINE STATS` using `FORK` + `STATS`, expecting NULL grouping keys to "match everything." Standard SQL NULL semantics applied — NULL formed its own group. The user found that `INLINE STATS` does what they wanted.

**Trade-offs**: Changing NULL grouping semantics would violate SQL standards and break legitimate use cases where NULL-as-distinct-group is intentional (e.g., "how many records have no value for this field?"). No major query engine treats NULL as a universal matcher in GROUP BY — not Trino, not Spark, not DuckDB, not ClickHouse.

**Opinion**: This is a documentation issue, not a product change. The user's confusion arose from trying to replicate a JOIN operation (INLINE STATS) using a UNION operation (FORK). Better docs explaining FORK (union of rows) vs INLINE STATS (join-back enrichment) would prevent this. No code changes warranted. Changing NULL grouping would be dangerous and wrong.

---

## Type Strictness and Implicit Casting

### #138509 — More permissive automatic coercion in union types

When the same field is mapped as `long` in some indices and `double` in others (extremely common with OTel metrics), ES|QL errors: "Cannot use field [jvm.memory.used] due to ambiguities being mapped as [2] incompatible types." Users must explicitly cast every reference to the field.

**Trade-offs**: Auto-widening numeric types is what every SQL database does. Java does it implicitly. The `commonType()` method in `EsqlDataTypeConverter` already knows that `long` and `double` have a common type (`double`), and `integer` and `long` have a common type (`long`). This logic exists but isn't wired into `ResolveUnionTypes`. The risk is precision loss (long → double loses precision for values > 2^53), but this is the same trade-off Java makes and users universally accept.

**Opinion**: Fix in core, no question. This is an unambiguous improvement. The current behavior is overly strict for no benefit — `byte`/`short`/`float`/`half_float`/`scaled_float` already auto-widen via `widenSmallNumeric`. The gap is specifically `integer→long` and `long→double` cross-index mismatches. Wire `commonType()` into `ResolveUnionTypes` and `ResolveUnionTypesInUnionAll`. ~1.5w. Not a relaxed mode candidate — this is just fixing a gap in the type system.

**Related untagged issues**: #144870 (date/date_nanos union type cast discards implicit conversion), #121890 (implicit typecasting missing in CASE), #112691 (data types not always widened), #105162 (date function argument types inconsistent), #99575 (avg() overflow on large data). These are all instances of the same root problem: the implicit casting system is too conservative. Fixing #138509 addresses the most painful case; the others should be evaluated individually but most are straightforward extensions of the same logic.

---

### #145781 — CONCAT auto-cast to string

CONCAT only accepts keyword/text arguments. Users must write `CONCAT(@timestamp::string, message, client.ip::string, http.request.bytes::string)` instead of `CONCAT(@timestamp, message, client.ip, http.request.bytes)`. The function's output is always a string, so the intent is unambiguous.

**Trade-offs**: The `ImplicitCasting` rule in the analyzer explicitly skips string-target functions because implicit casting was designed for the opposite direction (casting string literals to dates/IPs). Adding auto-to-string for CONCAT is safe because (a) the output type is always string, (b) `TO_STRING()` is well-defined for every ES|QL type, and (c) every competing query language does this — Trino, Spark SQL, PostgreSQL all auto-coerce in CONCAT. The risk is minimal: there's no ambiguity about what `CONCAT(42, "hello")` should produce.

**Opinion**: Fix in core. Extend `ImplicitCasting` to wrap non-string arguments with `ToString` for functions whose parameters are exclusively string types. This is a ~0.5w change that eliminates daily friction for users who work with mixed-type data. Not a relaxed mode candidate — auto-to-string in concatenation is universally expected behavior.

---

### #145784 — LHS cast for division

The user wants `eval error_rate_pct::double = errors/total_events` to trigger floating-point division. The idea is that declaring the output type on the left side of an assignment should propagate into the right-hand expression.

**Trade-offs**: LHS type annotation requires backward type propagation — the `::double` on the left must change how `/` resolves on the right. This is semantically ambiguous: does `x::double = a / b` mean "cast the integer division result to double" (giving 2.0 for 5/2) or "perform the division as double" (giving 2.5)? The user wants the latter, but a post-hoc cast gives the former. No major query language does backward type propagation from assignment targets. The grammar changes would be non-trivial and the semantic complexity would create long-term maintenance burden.

**Opinion**: Close as duplicate of #138516, which already tracks the integer division problem with better analysis. The LHS syntax specifically is the wrong approach. The underlying problem — integer division surprising users — is real, but the fix should be either (a) better docs for `errors / TO_DOUBLE(total_events)` or `errors / total_events::double` (which already works), or (b) a SET option that changes division behavior globally (`SET division_mode = "floating"`). The SET approach is a clean relaxed-mode candidate if we decide the problem is painful enough.

---

### #138516 — Integer division producing truncated results

`5 / 2` produces `2`, not `2.5`. This is standard integer arithmetic (same as Java, C, SQL) but surprises users who expect calculator-style math.

**Trade-offs**: Auto-promoting integer division to double breaks legitimate use cases. `SUBSTRING(message, 0, LENGTH(message)/2)` needs an integer result. `array_index / page_size` needs an integer result. Changing the default would silently break these. But for observability users doing `errors / total_events`, integer truncation produces meaningless results and they don't know why.

**Opinion**: This is a relaxed mode candidate. The current behavior is correct for SQL users. A `SET division_mode = "floating"` that promotes integer division to double would serve SPL users without breaking the core language. Alternatively, a new operator (e.g., `//` for integer division, `/` for float) could work but would break existing queries. The SET approach is cleaner. Until relaxed mode ships, the workaround `errors / total_events::double` is available and should be better documented. ~1w for the SET implementation.

---

## Syntax and Parser

### #145785 — Reserved keywords in field names

Dotted field names like `interface.bytes.in.max` fail because the lexer tokenizes `in` as the `IN` keyword before the parser sees it as part of an identifier. This affects any ECS-style field name containing a reserved word (`in`, `not`, `by`, `on`, `is`, `or`, `like`, `rlike`, `with`, `as`, `desc`, `first`, `last`, `null`, `true`, `false`).

**Trade-offs**: The workaround (backtick-quoting the whole name) exists but is non-discoverable and burdensome for users with many such fields. The fix is non-trivial — it requires either moving dot-separation out of ANTLR into Java visitor code, or adding a keyword-as-identifier fallback rule that expands every time a new keyword is added.

**Opinion**: This is a duplicate of #129795 (filed by Costin, June 2025), which describes the exact same root cause and proposes two approaches. Close #145785 as dup. The fix should happen — it's a real bug affecting real field names — but the design decision on approach belongs with #129795. Not a relaxed mode issue — this is a parser bug.

**Related untagged issues**: #145291 (better management of reserved words as identifiers), #122165 (backtick-quoted fields break RENAME), #129795 (the root issue), #110964 (synthetic vs real attribute name collisions). All symptoms of the same parser design limitation.

---

### #145782 — WHERE operator consistency

The user raises several complaints: MATCH is a function instead of infix operator, the `:` match operator doesn't support `NOT` in infix position, and IN and LIKE have different list syntax. These are mostly not valid criticisms, but one is.

**Trade-offs**: MATCH is intentionally a function because it accepts optional parameters (fuzziness, analyzer) that don't fit an infix model. The `:` operator exists as the simpler infix form. This design is reasonable. The `IN LIKE` fusion request mixes two fundamentally different operations. However, `WHERE abc NOT : "demo"` genuinely doesn't work while `WHERE abc NOT LIKE "pattern"` does — this is a real inconsistency in the grammar.

**Opinion**: The only actionable item is adding `NOT` support to the `:` match operator. The grammar rule `matchBooleanExpression` needs a `(NOT)?` token, and the visitor needs to wrap with `Not()`. This is a 3-line grammar change plus visitor handling, ~0.25w. Fix in core — it's a minor inconsistency, not a semantic question. The rest of the issue (MATCH as function, IN LIKE) should be closed as by-design.

---

### #145783 — Sorting defaults (NULLS LAST for DESC)

When a user writes `| SORT abc DESC`, they expect the highest value on top. Instead, nulls sort first because SQL convention says DESC defaults to NULLS FIRST. The workaround is `SORT abc DESC NULLS LAST`.

**Trade-offs**: The SQL standard is clear: ASC → NULLS LAST, DESC → NULLS FIRST. PostgreSQL, Oracle, SQL Server all follow this. However, analytical engines targeting observability users (Spark, ClickHouse, DuckDB in some modes) default to NULLS LAST always because their users care about "show me the top values" not SQL pedantry. Splunk has no concept of nulls sorting to the top.

**Opinion**: This is a textbook relaxed mode candidate. The SQL default is correct for SQL users. The observability default is NULLS LAST always. A `SET null_sort_order = "last"` would change the parser default in `visitOrderExpression` without touching the compute engine — the `Order` node, `TopNOperator`, and Lucene pushdown all already support both positions. ~0.5w. Do not change the core default.

---

## Query Parameters and API

### #144289 — IN clause with list-valued parameters

`WHERE status IN (?statuses)` where `?statuses = ["open", "pending", "review"]` doesn't work. The parser wraps the list as a single Literal, producing either a type error or wrong results. The workaround is `MV_CONTAINS(?statuses, status)` which has its own problems (wrong null semantics, no Lucene pushdown, swapped argument order).

**Trade-offs**: This is broken, not "different." A list-valued Literal inside IN has no valid interpretation today. SQL doesn't have list parameters at all, so there's no semantic precedent to violate. PostgreSQL uses `= ANY(array)`, BigQuery uses `IN UNNEST(array)` — both address the same need with explicit syntax. The implicit expansion proposed here is simpler and safe because the current behavior is just wrong.

**Opinion**: Fix in core, highest priority. PR #144290 already implements this. In `visitLogicalIn`, detect list-valued Literals on the rhs and expand into individual scalar Literals. ~30 lines of parser code. The fix unblocks Kibana multi-select controls (filters with multi-select dropdowns generate IN clauses with list params). This is the most impactful fix-to-effort ratio in the entire ergonomics backlog. ~0.5w including tests.

---

### #138261 — Request-level ignore_unavailable

The `_search` API supports `ignore_unavailable=true` to silently skip missing indices. The `_query` API does not. Users querying data streams for accounts that may have been deleted get errors instead of empty results.

**Trade-offs**: ES|QL's `IndexResolver.DEFAULT_OPTIONS` already uses `ALLOW_UNAVAILABLE_TARGETS` internally for wildcard resolution. The workaround is using wildcards (`FROM logs-account-123*` instead of `FROM logs-account-123`), but this is fragile and could match unintended indices. The risk of adding the parameter is low — it's standard operational infrastructure that every Elasticsearch API supports.

**Opinion**: Fix in core. Add `ignore_unavailable` parameter to `EsqlQueryRequest`, thread it through to `IndexResolver` where `IndicesOptions` are constructed. ~1w of straightforward plumbing. This is not a language semantic — it's a runtime parameter about error handling. Every SQL database has connection-level settings that affect error behavior. Not a relaxed mode candidate — this is standard infrastructure.

---

## Multivalue Semantics

### #139435 — MV_CONTAINS null as empty set

`MV_CONTAINS(superset, subset)` returns `true` when `subset` is null, treating null as an empty set. Mathematically correct (empty set is a subset of any set), but surprising when `subset` is a field that might be missing. Users expect null to propagate.

**Trade-offs**: The current behavior is internally consistent with set theory. Changing it would make MV_CONTAINS inconsistent with its mathematical definition. However, the practical impact is that documents with missing fields are silently included in results — which users experience as a correctness bug. The team (Alex Spies) is already planning a broader MV semantics review to avoid creating a "patchwork of different solutions."

**Opinion**: Wait for the broader MV review, especially since fixing #144289 (IN with list params) eliminates the primary use case that drives users to MV_CONTAINS as a workaround. Once IN works with list params, the Kibana multi-select controls use case is solved without touching MV_CONTAINS. If the MV review decides to change this, the right approach is either an optional parameter or a new null-propagating function (`MV_HAS`). ~1-2w depending on scope. Not a relaxed mode candidate — it's a design decision about MV function semantics.

---

## Search and Full-Text Integration

### #145788 — Wildcards in field names in WHERE

The user wants `WHERE *ip* == "192.168.0.1"` to search across all fields matching a name pattern, like KQL's `*.ip: value`.

**Trade-offs**: KQL can do this because it's a loosely-typed search language. ES|QL is strongly typed — `*ip*` could match `source.ip` (ip type), `destination.ip` (ip type), and `description_of_ip_policy` (keyword type). Expanding wildcards into a disjunction of typed comparisons raises hard questions: what happens when the types are incompatible with the comparison? Silent skip? Error? Type-filtered expansion? The `UnresolvedNamePattern` + `CharacterRunAutomaton` infrastructure already exists for KEEP/DROP/RENAME, so the pattern-matching machinery is available.

**Opinion**: This is an interesting feature with real value for security and observability workflows, but it needs design work before implementation. The type safety questions must be answered: I'd recommend type-filtered expansion (only match fields where the comparison is type-compatible) with a clear warning when no fields match. This is a 2-4w design + implementation effort and should not be rushed. It could ship in core (it doesn't violate SQL principles — SQL just doesn't have this concept) but it needs careful design to avoid creating a footgun where wildcard expansion silently matches unexpected fields. Not a relaxed mode candidate — if we do it, we do it right for everyone.

**Related untagged issues**: #119149 (partial field/column name matching — same concept applied to projections beyond WHERE).

---

## UI/Kibana (out of scope for language changes)

### #145789 — Execute query up to selected line

The user wants "run up to line N" in the ES|QL editor. The server already executes any valid truncated query — this is purely about the Kibana editor truncating the query string at the right pipe boundary. Transfer to elastic/kibana.

### #145790 — Field autocomplete after STATS

After STATS, the sidebar only shows aggregation output fields. The user wants to see fields from the previous pipeline stage. The analyzer already tracks this information — it just needs to be surfaced. Transfer to elastic/kibana.

---

## Similar Untagged Issues Worth Pulling In

These aren't tagged `ES|QL-ergonomics` but address the same friction:

### Type strictness cluster
- **#121890** — CASE function lacks implicit type promotion. `CASE WHEN true THEN 1 WHEN false THEN 1.5 END` fails because integer and double don't auto-unify. Same root cause as #138509. Fix: wire `commonType()` into CASE expression type resolution.
- **#144870** — Explicit cast on date/date_nanos union type discards implicit conversion. Another instance of union type strictness.
- **#105162** — Date function argument types inconsistent across functions. Users must guess which date form is accepted.
- **#99575** — `avg()` overflows on large long data. Should auto-promote accumulator to wider type.

### Error messages cluster
- **#100047, #100639** — Syntax error messages don't point to the right location or suggest fixes. Major friction amplifier for SPL migrants who are learning the syntax.
- **#142584** — Multiple verification errors should be presented in bulk, not one at a time.
- **#134469** — Parameters in qualified names give unhelpful parse error.

### KQL integration friction
- **#139385** — KQL date math doesn't work inside ES|QL WHERE. Users writing `WHERE kql("@timestamp > now-1h")` get errors.
- **#142710** — `:` operator produces misleading verification exceptions.
- **#142554** — Wildcard patterns in `_source` config cause automaton explosion (TooComplexToDeterminizeException). 20+ infix wildcards blow the 10,000 effort limit.

### Syntax convenience
- **#134173** — Trailing commas cause parse errors. Common friction point for interactive editing — users add/remove fields from KEEP/STATS and constantly hit this.

---

## Summary: What to Do

### Fix now in core (~5.75w total)

| # | Title | Effort | Rationale |
|---|-------|--------|-----------|
| 144289 | IN clause list params | 0.5w | Broken behavior, PR exists, unblocks Kibana multi-select |
| 138509 | Union type auto-coercion | 1.5w | `commonType()` exists, just not wired in. OTel production impact. |
| 138512 | IS / IS NOT operators | 1.0w | SQLite/PostgreSQL precedent. Non-breaking null-safe comparison. |
| 145781 | CONCAT auto-cast | 0.5w | Every query language does this. Output is always string. |
| 138261 | ignore_unavailable | 1.0w | Standard operational parameter. Plumbing only. |
| 145782 | NOT : consistency | 0.25w | 3-line grammar fix. Real inconsistency with LIKE/RLIKE. |
| 145785 | Reserved keywords | — | Close as dup of #129795. Fix belongs there. |
| 145784 | LHS cast | — | Close as dup of #138516. Wrong approach. |
| 145786 | NULL in GROUP BY | — | Docs only. User found INLINE STATS. |

### Design for relaxed mode (~3.5w, requires SET infrastructure)

| # | Title | Effort | SET option |
|---|-------|--------|-----------|
| 138512 | Two-value NULL logic | 2.0w | `SET null_comparison = "two_value"` |
| 145783 | NULLS LAST default | 0.5w | `SET null_sort_order = "last"` |
| 138516 | Floating division | 1.0w | `SET division_mode = "floating"` |

These three SETs define the minimum viable relaxed mode. They're the top three things SPL users complain about, and they all directly conflict with SQL semantics.

### Design carefully, don't rush

| # | Title | Effort | Why wait |
|---|-------|--------|---------|
| 145788 | Wildcards in WHERE | 2-4w | Type safety design needed |
| 139435 | MV_CONTAINS null | 1-2w | Wait for broader MV review, #144289 reduces urgency |

### Transfer to Kibana

| # | Title |
|---|-------|
| 145789 | Execute up to line |
| 145790 | Field autocomplete after STATS |
