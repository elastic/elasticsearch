# The union type and the untyped constant — specification

This document specifies **behaviour**: what a query returns, what it warns about, and what it refuses.
It is written against worked examples, because the behaviour is the deliverable and prose about the
behaviour is not. `UNION-TYPES-ARCHITECTURE.md` beside it describes the mechanism.

---

## 1. The problem

A field whose indices disagree about its type is today `UNSUPPORTED`. `KEEP` returns nulls, and every
other reference to it fails the query. The user's recourse is `duration::long`, which requires an
opinion about what `duration` is — and the reason the field is conflicted is usually that nobody has
one. This is the normal condition of a log corpus, not an edge case.

Two things are missing, and they are separate:

1. **A column may hold values of more than one type**, and should still be readable, filterable,
   groupable and aggregable rather than erased.
2. **A user may want to compare against a value without committing to its type** — to ask "is this
   two hundred" of a column that stores two hundred as `200`, `200.0` and `"200"` in three different
   indices.

The first is a *type*. The second is a *constant*. They are implemented together and specified
separately, because they answer to different authorities: the mapping decides the first, the user
decides the second.

---

## 2. Principles

### 2.1 The union type

> A union column is, in every value, a value of exactly one of a fixed set of **leg** types, and it
> has **no semantics of its own**. An operation on a union is that operation on each leg as the
> single-typed column it already is, and results combine across legs only by a rule the engine
> already has for those types. Where no such rule exists the result is **unknown** (null, warned).
> The operation is **refused** only when it is undefined on *every* leg — which is exactly when the
> single-typed column would refuse it.

Everything in §4 is derived from that sentence. Nothing in this specification adds a rule about how
two types relate; where such a rule is needed it is the engine's existing one, consulted as an
expression and never as a table.

### 2.2 The untyped constant

> `<literal>::any` is an expression with **no committed type**. In an operation against an expression
> of type `T` it is the engine's own conversion of that literal to `T`, where one exists; where none
> exists, that operand is undefined. Against a union it is converted independently per leg.

"The engine's own conversion" means `EsqlDataTypeConverter.converterFunctionFactory` — the same
registry `::long` goes through. The constant introduces no conversion of its own, which is what stops
it inventing a relation between types.

### 2.3 Invariants

| # | Invariant |
|---|---|
| I1 | **Monotone narrowing.** Adding a leg can only *remove* capability, never add or alter it. For leg sets L ⊆ L′, every query that works on L′ works on L and returns the same answer; removing the leg restores it exactly. A query that stops working does so **loudly**, with an error naming the conflict — never by returning a different answer. |
| I2 | **No existing semantics change.** No query that works today fails. More queries succeed; none change their answer. |
| I3 | **Interchangeability.** A `long` inside a union behaves exactly like a `long`. A union of one leg *is* that leg's column. |
| I4 | **No invented ordering.** Nothing may make `400 < "100"` true. |

I1 replaces an earlier, stronger claim that no query which worked could ever stop working. That was
unsatisfiable — `SORT` already broke it — and it was also the wrong goal. A field whose indices have
started to disagree **should** fail the queries that have become meaningless, because failing is how
the divergence gets noticed. What I1 forbids is the quiet version: the same query still succeeding over
a narrower answer.

Three tiers follow, and every operation in §4 sits in exactly one:

| Tier | Consults the legs? | Operations |
|---|---|---|
| always allowed | no | render, `TYPEOF`, `COUNT`, `IS NULL`/`IS NOT NULL`, leg selection, cast, grouping |
| conditional | **yes** — the declared leg set, at analysis | comparison |
| always refused | no | `SORT`, `MIN`, `MAX`, `SUM`, `AVG` |

The conditional tier is decided by the legs a column *may* hold, never by the values it happens to
hold, so the same query on the same mapping always has the same fate.

Grouping is unconditional because `(leg, value)` identity needs no rule relating two types — which is
why §4.10 can ship ungated where ClickHouse gates it behind `allow_suspicious_types_in_group_by`.

### 2.4 Four layers, each delegating

| Layer | Who decides | Written as | Status |
|---|---|---|---|
| The **type** | the mapping | nothing — it appears | new |
| The **untyped constant** | the user, about the *literal* | `200::any` | new |
| The **cast** | the user, about the *data* | `duration::long` | exists |
| The **leg selector** | the user, about *which values* | `TYPED(duration, "long")` | exists on this branch |

The last two are the escape hatch from every refusal in §5, and every refusal names one of them.

---

## 3. The worked corpus

Three indices matched by `logs-*`, disagreeing about `duration`:

| index | `duration` mapped as | values |
|---|---|---|
| `logs-a` | `long` | `200`, `250`, `300` |
| `logs-b` | `double` | `100.0` |
| `logs-c` | `keyword` | `"200"`, `"150"`, `"unknown"` |

Seven rows. The column's legs, in the canonical order (widened, de-duplicated, sorted by type name):
**`double`, `keyword`, `long`**.

That order is a function of the leg types alone, so the block and the response derive it independently
and cannot drift. It is a wire contract, not a user-visible order.

---

## 4. Behaviour

### 4.1 Reading

```esql
FROM logs-* | KEEP duration
```

| `duration` |
|---|
| `200` |
| `250` |
| `300` |
| `100.0` |
| `"200"` |
| `"150"` |
| `"unknown"` |

Seven rows, every value rendered in its own type. **A union column is always renderable** — this is a
hard requirement, not a goal. The response declares the column as `union` and carries the leg list, so
a client can tell `200` the long from `"200"` the keyword.

### 4.2 Equality against an ordinary literal

```esql
FROM logs-* | WHERE duration == 200
```
→ **`200`** (one row), with a warning.

Per leg: `long == 200` matches `200`. `double == 200` is defined and matches nothing. `keyword == 200`
is **undefined** — ES|QL has no number↔string conversion, `commonType` returns null for the pair, and
`EsqlBinaryComparison.toEvaluator` throws on it today. So the keyword leg's rows are null and a warning
names the leg.

```esql
FROM logs-* | WHERE duration == "200"
```
→ **`"200"`** (one row), with a warning.

The mirror image. The keyword leg matches; the numeric legs are undefined. Note that the literal
`"200"` is *not* implicitly read as a number — a foldable string literal adopts the other operand's
type only for `datetime`, `date_nanos`, `ip`, `version` and `boolean`, per
`EsqlDataTypeConverter.isStringImplicitlyCastableTo`. That is existing behaviour and I2 keeps it.

### 4.3 Equality against an untyped constant — the point of the construct

```esql
FROM logs-* | WHERE duration == 200::any
```
→ **`200`** and **`"200"`** (two rows). **No warning.**

Per leg, the literal `200` is converted by the engine's own conversion and then compared under that
leg's own rules:

| leg | literal becomes | compared | matches |
|---|---|---|---|
| `double` | `200.0` | `100.0 == 200.0` | — |
| `keyword` | `"200"` | `"200" == "200"` | `"200"` |
| `long` | `200` | `200 == 200` | `200` |

This is `"200" = 200 = 200.0` asked once. No leg is undefined, because the *literal* converts even
where the *column* would not — which is the whole difference between a constant and a cast.

```esql
FROM logs-* | WHERE duration == 100::any
```
→ **`100.0`** (one row). The double leg is reached; `100` does not match the long leg and `"100"` does
not match the keyword leg.

### 4.4 `::any` against the cast — what each one changes

```esql
FROM logs-* | WHERE duration::long == 200 | KEEP duration
```
→ **`200`**, **`200`** (two rows), with a warning for `"unknown"`.

```esql
FROM logs-* | WHERE duration == 200::any | KEEP duration
```
→ **`200`**, **`"200"`** (two rows). No warning.

The same two rows, rendered differently, and that is the distinction:

- **`duration::long`** changes the **data**. Every leg is converted, the column becomes a `long`,
  `"unknown"` becomes null with a warning, and `100.0` becomes `100`. The user has asserted what
  `duration` *is*.
- **`200::any`** changes only the **literal**. The column is untouched and still renders `"200"` as a
  keyword. The user has asserted nothing about `duration`.

A cast is lossy and needs an opinion. The constant is lossless and needs none. They are not
substitutes.

### 4.5 Ordering comparison

```esql
FROM logs-* | WHERE duration > 200
```
→ **`250`**, **`300`**, with a warning.

Defined on `long` and on `double`; undefined on `keyword`, exactly as it is undefined on a keyword
column today. The numeric legs answer, the keyword leg is null, the warning names it.

```esql
FROM logs-* | WHERE duration > 200::any
```
→ **refused.**

```
cannot use [>] with an untyped constant: converting [200] into [keyword] would compare it
lexically, so ["50" > "100"] would be true. Cast the column instead: [duration::long > 200],
or select one type: [TYPED(duration, "long") > 200]
```

This is I4. Equality is safe under any conversion the engine has, because equality needs only that the
conversion be *injective enough* to not create a false match; ordering additionally needs it to be
**monotonic**, and nothing in the tree records which conversions are. So `::any` is **equality and
`IN` only**.

The restriction is the missing metadata, not the semantics: on a `long｜double` union every conversion
*is* monotonic and `duration > 200::any` would be well defined. It is refused anyway, because a refusal
can be relaxed into a definition later and a definition cannot be withdrawn.

### 4.6 `SORT`

```esql
FROM logs-* | SORT duration
```
→ **refused.**

```
cannot sort on [duration]: its types [keyword] and [double, long] have no order between them.
Cast it: [duration::long], or select one type: [TYPED(duration, "long")]
```

```esql
FROM logs-ab-* | SORT duration     // legs: double, long
```
→ allowed, and ordered exactly as `long` against `double` is ordered today.

A sort over legs that span comparability classes cannot be expressed. `NULLS FIRST`/`NULLS LAST` place
null *values*; "incomparable with some other rows" is not a property of a value, and it is not even
transitive — `200` against `"150"` is undefined while `"150"` against `"200"` is defined. TopN encodes
sort keys to bytes and has no three-valued comparator to put an unknown into.

**Rejected alternative:** ordering by `(leg, value)` as a tuple. It is principled, it is total, and it
puts `"unknown"` between two numbers for no reason a user could predict. Principled and
mind-boggling is not a trade worth making for a sort.

### 4.7 `MIN` and `MAX`

```esql
FROM logs-* | STATS MAX(duration)
```
→ **null**, with a warning.

```
[MAX(duration)] has no value: the maximum of the [long] values and the maximum of the [keyword]
values have no order between them. Cast it: [MAX(duration::long)]
```

Every leg *has* a maximum — `300` and `"unknown"` and `100.0`. What has no answer is the combination,
because there is no rule relating `300` to `"unknown"`. The query does not fail; it returns the unknown
that the absence of a rule produces. Null is a meaningful answer here, and it is available.

```esql
FROM logs-ab-* | STATS MAX(duration)     // legs: double, long
```
→ **`300`**. Every pair is comparable because the engine relates `long` to `double`, so the extremum
exists. The result's declared type is `union` — the type cannot depend on which leg happens to win —
and it carries one value, tagged `long`.

```esql
FROM logs-* | STATS MAX(duration::long)
```
→ **`300`**, typed `long`, with a warning for `"unknown"`. The escape hatch, and it works today.

### 4.8 `SUM`, `AVG` and arithmetic

```esql
FROM logs-* | STATS SUM(duration)
```
→ **`850.0`**, with a warning.

`long` sums to `750`, `double` sums to `100.0`, and the engine's own `long + double` gives `850.0`. The
`keyword` leg has no sum — undefined, warned, contributes nothing. Same rule as §4.2, applied to a
different operation.

```esql
FROM logs-c | STATS SUM(duration)     // keyword only
```
→ **refused**, with the message a keyword column gives today. Undefined on every leg is a refusal (I3).

### 4.9 `COUNT`

```esql
FROM logs-* | STATS COUNT(duration), COUNT(*)
```
→ **`7`**, **`7`**.

`COUNT` needs no per-leg fan-out at all: it reads the tag, which says whether a value is present
without saying what type it is.

### 4.10 Grouping

```esql
FROM logs-* | STATS COUNT(*) BY duration
```

| `duration` | `COUNT(*)` |
|---|---|
| `100.0` | 1 |
| `"150"` | 1 |
| `"200"` | 1 |
| `"unknown"` | 1 |
| `200` | 1 |
| `250` | 1 |
| `300` | 1 |

Grouping identity is **`(leg, value)`**. The long `200` and the keyword `"200"` are **different
groups**.

This is derived, not chosen. Per leg, the long column groups its `200` and the keyword column groups
its `"200"`; merging them presupposes converting one, which is a cast and therefore the user's call.
A `double` column holds no long `200` to collapse, so "group them as doubles" is not the neutral
reading — it is an opinion about the data.

**The asymmetry this owns:** `WHERE duration == 200::any` matches both rows, and `STATS BY duration`
puts them in different buckets. Equality is a *relation*, which the user asked for by writing `::any`;
identity is *representation*, which nobody asked about. The two already differ in ES|QL today:
`X-MultivalueDedupe` hashes doubles through `Double.doubleToLongBits`, which separates `-0.0` from
`0.0`, while `Equals.processDoubles` is `lhs == rhs`, which unifies them. A user who wants one bucket
writes `STATS ... BY duration::long`.

### 4.11 Null

```esql
FROM logs-* | WHERE duration IS NOT NULL
```

Answered from the tag alone. A value with no tag is null; `IS NULL` and `IS NOT NULL` never consult a
leg, and they are defined on every union without exception.

A field **unmapped** in some indices gets a `keyword` leg, because that is what those indices load it
as from `_source`. The leg is always declared even when `unmapped_fields` leaves it empty: an empty leg
costs a null block, whereas a missing leg would have nowhere to put a value.

### 4.12 Two unions compared

```esql
FROM logs-* | WHERE duration == timeout
```

Per leg *pair*. A row compares by the two legs its two values actually occupy: `long == long` is
defined, `long == keyword` is undefined and null, and so on across the cross product. Refused only if
no pair of legs is defined. There is no "compare the unions" rule — there is no union to compare, only
two values whose types each row decides.

### 4.13 Multi-valued columns

A union **value** is single-typed. A union **row** need not be.

Loaded data is single-typed per row, because within one index a field path has one mapper and the
reader picks the loader per document's shard. But computed values are not: `VALUES()`, `MV_APPEND` and
`TOP` can put a long and a keyword in the same row. The logical model is therefore **per value, not per
row**, and so is the physical layout — the tag is indexed by sub-block position, not by row, so it
already carries both shapes and needs no change.

Every operation above applies per value. Nothing in §4 distinguishes the single- and multi-valued cases.

### 4.14 `TYPED` — selecting a leg

```esql
FROM logs-* | KEEP duration, TYPED(duration, "long")
```

| `duration` | `TYPED(duration, "long")` |
|---|---|
| `200` | `200` |
| `250` | `250` |
| `300` | `300` |
| `100.0` | null |
| `"200"` | null |
| `"150"` | null |
| `"unknown"` | null |

`TYPED` returns the values whose *source mapped them as* that type and null for every other, typed as
that type. It **selects**; it does not convert. `duration::long` converts `"200"` to `200` and
`100.0` to `100`; `TYPED(duration, "long")` discards both.

Because its result is a single-typed column, it lifts every restriction in §5 at once — and, unlike a
cast, it does so without changing a single value.

---

## 5. Refusals — the complete list

| Refused | Because | Escape |
|---|---|---|
| `SORT` on legs spanning comparability classes | a permutation cannot carry an unknown (§4.6) | `::`, `TYPED` |
| an ordering comparison against `::any` | no monotonicity metadata exists (§4.5) | `::`, `TYPED` |
| an operation undefined on **every** leg | the single-typed column refuses it too (I3) | — |
| `::any` applied to anything but a literal | a computed value has already committed; there is nothing to decline | drop it |
| `::any` on both sides of an operator | no type for either to resolve against | type one side |

Nothing else. In particular `KEEP`, `EVAL`, `WHERE`, `STATS`, `STATS … BY`, `MV_EXPAND`, `FORK` and
rendering are **defined on every union**. Where the current implementation refuses one of those it is
a defect to fix, not behaviour to specify — see §8.

---

## 6. The undefined-leg policy: strict

Where an operation is defined on one leg and **undefined** on another — `duration > 200` on
`long｜keyword` — **the query is refused.** The refusal is computed from the declared leg set at
analysis, so it does not depend on which values the column turns out to hold.

```
cannot use [>] on [duration]: it holds [keyword] and [long] values, which have no common type.
Cast it: [duration::long > 200], or select one type: [TYPED(duration, "long") > 200]
```

The alternative was to null the undefined leg's values and warn, letting the query run. It is rejected
on I1: the query would keep succeeding over a silently narrower answer, which is the one failure mode
I1 exists to forbid. A user whose mapping has started to disagree needs the error, because the error is
how they find out.

This is also what ClickHouse does, and the measurement separating the two possible readings is in
`POC-PLAN.md` § "What ClickHouse does, measured": `WHERE v = 200` is refused on
`Variant(UInt64, Float64, String)` with `NO_COMMON_TYPE` and **works** on `Variant(UInt64, Float64)`.
So the refusal is not about the union type; it is the absence of a common type across the legs,
which is §2.1's delegation with this policy.

Strict also remains the reversible direction: it can be relaxed to lenient later additively, and
lenient cannot be tightened without breaking queries.

## 7. Syntax and naming

The untyped constant is written as an **inline cast**, `200::any`, and not as a function.

`::` is the one idiom ES|QL already has for saying something about a type at a point in an expression,
and for a *literal* it already means "read this as `T`" rather than "narrow this to `T`" —
`"2024-01-01"::datetime` reinterprets a literal, it does not convert a column. `200::any` is that
gesture generalised to "read this as whatever type the context needs". It also keeps a comparison
shaped like a comparison: `duration == 200::any` rather than `duration == UNTYPED(200)`.

A function was rejected for two reasons. It would sit in `EsqlFunctionRegistry` with a `@FunctionInfo`
declaring a user-visible `returnType`, and there is no honest value for an expression whose point is
that it has no type until something asks. And `TYPED` is already taken on this branch and means
something else entirely (§4.14), so `UNTYPED` would read as its inverse — "the other legs" — which is
not remotely what it is.

`any` rather than `untyped` for the same reason: it collides with neither `TYPED` nor the `union` type
name, and it says the true thing.

**Feasibility.** `ExpressionBuilder.castToType` already special-cases names that appear on the right of
`::` without being real data types — `DataType.COUNTER_CAST_NAME` and `GAUGE_CAST_NAME` are matched
*before* the converter-function lookup and turned into dedicated nodes, and without such an arm the
cast fails at parse time with `Unsupported conversion to type`. `any` is a third arm in that same
method. Because `::` binds to any `primaryExpression`, `foo::any` and `(a+b)::any` will parse and must
be refused by the verifier (§5) rather than accepted as no-ops.

---

## 8. What is behaviour and what is a defect

Three current refusals are **mechanical**, not specified:

- **Across subqueries (`FORK`, `MergePlan`).** `MergePlan.output` is a constructor field fixed during
  resolution, so a column that becomes a union afterwards keeps its stale `UNSUPPORTED`. The fix is to
  have the conflict declare `UNION` at field resolution, not to specify a refusal. I1 requires it.
- **`MV_EXPAND`.** `Analyzer.resolveMvExpand` captures `resolved.dataType()` into a
  `ReferenceAttribute` at resolution time — the same freeze, the same fix.
- **Pushdown.** `LucenePushdownPredicates.isQueryable` has a multi-type branch that checks
  `isIndexed && hasDocValues` with **no type check**, while the line below it checks the attribute's
  type. This must be closed before any per-leg attribute carries the real field name.

### What of this specification exists

**§4 is prescriptive, not descriptive.** Almost none of it is implemented, and the spec is written in
the present tense throughout, so this section is the only place that says so.

On the branch today: `DataType.UNION`; `UnionEsField`; the analyzer rule that turns a conflict into a
union behind the `union_typed_fields` pragma; `UnionBlock` / `UnionArrayBlock` / `UnionBlockBuilder`
with their `ElementType`, `BlockFactory`, `BlockUtils` and `ConstantNullBlock` wiring; rendering hooks
in `ResponseValueUtils` and `PositionToXContent`; and the refusals in `MergePlan` and `Join`.

Not on the branch: **anything that produces a union block** — no field loader builds one, so with the
pragma on, reading such a field fails on the data node. And **no comparison or conversion is
union-aware**: `EsqlBinaryComparison`, `EsqlDataTypeConverter` (which owns `commonType`) and
`DataTypeConverter` contain no occurrence of `UNION` at all. Also absent: per-leg aggregation fan-out
(§4.7, §4.8), grouping on a union (§4.10), population of the response's per-column leg list
(`ColumnInfoImpl.originalTypes`, without which §4.1 cannot render), and `::any` (§4.3).

So the per-leg delegation in §2.1 is the substantial unbuilt piece, and every behaviour in §4.2 through
§4.13 rides on it. Nothing here should be read as a statement about what a query does today.

---

## 9. Open

- **`!=`.** Three-valued (`null` where `==` is null) or un-bracketed, so that a keyword never
  "differs from" a long? Three-valued is the consistent reading and is assumed above.
- **Lexical ordering within a leg.** `duration >= "150"` on the keyword leg is defined, and compares
  lexically. That is what a keyword column does today, so I2 keeps it — but it is worth confirming
  that a user reading a mixed column finds it intuitive rather than surprising.
- **`UNSIGNED_LONG` beside `LONG`.** Both are legs, and the engine relates them only through
  `areTypesCompatible`'s both-unsigned arm. Whether the pair is comparable needs settling before a
  corpus produces it.
