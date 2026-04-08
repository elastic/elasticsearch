# ES|QL Ergonomics Issues Review

All 8 issues filed 2026-04-07 by Philipp Kahr (@philippkahr), labeled `ES|QL-ergonomics`.

---

## #145785 — Reserved keywords in field names (`in`, `not`, `by`)

**What it is**: Dotted field names like `interface.bytes.in.max` fail to parse because the lexer tokenizes `in` as the `IN` keyword before the parser sees it as part of an identifier. Affects any ECS-style field name containing a reserved word.

**Duplicate**: Yes — **#129795** (filed by Costin, June 2025). Exact same root cause and analysis. Costin proposed two fixes: (a) parse dotted names as opaque tokens at the lexer level, (b) add keyword-as-identifier fallback rule.

**Verdict**: Real bug, already tracked. Close as dup of #129795.

---

## #145789 — Execute query up to selected line

**What it is**: User wants a "run up to line N" feature in the ES|QL editor to inspect intermediate results without manually deleting pipeline stages.

**Duplicate**: No.

**Verdict**: Pure Kibana/UI feature. The server executes any valid truncated query — someone just needs to truncate the string at the right pipe boundary in the editor. Transfer to elastic/kibana.

---

## #145790 — Field autocomplete after STATS

**What it is**: After STATS, the sidebar only shows aggregation output fields. User wants to see fields from the previous pipeline stage to help build GROUP BY clauses.

**Duplicate**: No.

**Verdict**: Pure Kibana/UI feature. The analyzer already tracks the full input schema per command — the information exists, it just needs to be surfaced in the sidebar. Transfer to elastic/kibana.

---

## #145781 — CONCAT should auto-cast to string

**What it is**: `CONCAT(@timestamp, message, http.request.bytes)` fails because CONCAT only accepts keyword/text. User must wrap every non-string arg with `::string` or `TO_STRING()`.

**Duplicate**: No (related to #138509 about numeric type coercion, but that's a different scope).

**Verdict**: Legitimate ergonomics gap. Most query languages (Trino, Spark SQL, PostgreSQL) auto-coerce to string in CONCAT. The `ImplicitCasting` rule in the analyzer explicitly skips this case today. Fix: extend `ImplicitCasting` to wrap non-string args with `ToString` for functions whose parameters are exclusively string types. ~1-2 days. Worth doing.

---

## #145782 — WHERE consistency (NOT with `:` operator)

**What it is**: Mixes several complaints. The only actionable one: `WHERE abc NOT : "demo"` doesn't work (you must write `WHERE NOT abc : "demo"`), while `WHERE abc NOT LIKE "pattern"` does work. The grammar for the `:` match operator has no `NOT` token.

**Duplicate**: No.

**Verdict**: The `NOT :` gap is a minor but real inconsistency with LIKE/RLIKE. The complaints about MATCH being a function (intentional design) and wanting `IN LIKE` fusion (mixing two different operations) are not actionable. Fix for `NOT :`: add `(NOT)?` to the `matchBooleanExpression` grammar rule, handle in visitor. ~0.5 day. Low priority but clean.

---

## #145784 — LHS cast for division (`error_rate::double = errors/total`)

**What it is**: User wants to declare the output type on the left side of an EVAL assignment to trigger implicit type promotion in the right side expression, specifically to get floating-point division from integer operands.

**Duplicate**: Near-dup of **#138516** (filed by you). That issue covers the same integer-division problem with better tradeoff analysis, including the counter-case where auto-promotion would be harmful (`SUBSTRING(msg, 0, LENGTH(msg)/2)` needs integer result).

**Verdict**: The underlying problem (integer division surprises) is real, but the proposed LHS syntax is the wrong fix. It requires backward type propagation, is semantically ambiguous (cast-after vs compute-as-double), and no major query language does this. Close as dup of #138516.

---

## #145786 — Partial stats merges / NULL in GROUP BY

**What it is**: User tried to replicate `INLINE STATS` using `FORK` + `STATS`, expecting NULL grouping keys to "match everything." Standard SQL semantics applied instead (NULL is its own group). User found that `INLINE STATS` does what they wanted.

**Duplicate**: No.

**Verdict**: Documentation issue, not a product bug. The user's mental model (NULL = universal matcher) violates SQL standard and every major query engine. Changing this would be dangerous. The user already found the correct tool (`INLINE STATS`). Fix: improve docs to explain FORK (union of rows) vs INLINE STATS (join-back enrichment) and NULL behavior in GROUP BY. No code changes.

---

## #145788 — Wildcards in field names in WHERE

**What it is**: User wants `WHERE *ip* == "192.168.0.1"` to expand to searching across all fields matching the name pattern, like KQL's `*.ip: value`.

**Duplicate**: No.

**Verdict**: Interesting feature with real value for security/observability. But non-trivial design challenges:
- **Type safety**: ES|QL is strongly typed. `*ip*` could match `source.ip` (ip type) and `description_of_ip_policy` (keyword type) — type mismatch in `==`.
- **Semantics**: OR across all matches? AND? What if no fields match?
- **Performance**: could silently expand to hundreds of fields.

The `UnresolvedNamePattern` + `CharacterRunAutomaton` infrastructure already exists for KEEP/DROP/RENAME — the main work is wiring it into expression resolution and handling type heterogeneity. ~2-4 weeks if pursued. Worth designing carefully, not a quick fix. Could be gated behind an ergonomics/lenient mode.

---

## Summary

| # | Title | Duplicate? | Action | Effort |
|---|-------|-----------|--------|--------|
| **145785** | Reserved keywords in field names | Dup of #129795 | Close as dup | — |
| **145789** | Execute up to line | No | Transfer to kibana | — |
| **145790** | Field autocomplete after STATS | No | Transfer to kibana | — |
| **145781** | CONCAT auto-cast | No | Fix — extend ImplicitCasting | 1-2 days |
| **145782** | WHERE NOT : consistency | No | Fix — grammar `NOT` for `:` | 0.5 day |
| **145784** | LHS cast for division | Near-dup of #138516 | Close as dup | — |
| **145786** | NULL in GROUP BY / FORK vs INLINE STATS | No | Docs improvement only | 0.5 day |
| **145788** | Wildcards in WHERE field names | No | Design needed — future | 2-4 weeks |

**Worth fixing now**: #145781 (CONCAT), #145782 (NOT :) — small, clean, real value.
**Worth doing but not urgent**: #145788 (wildcards in WHERE) — needs design work.
**Close/transfer**: #145785 (dup), #145784 (dup), #145789 (kibana), #145790 (kibana), #145786 (docs).
