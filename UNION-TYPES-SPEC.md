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
> single-typed column it already is. The operation is **refused** unless it resolves on *every* leg.

**The oracle is the operation's own type resolution, evaluated per leg against the other operand** —
never a table of type pairs, and never a common type computed across the leg set. For comparison it is
`EsqlBinaryComparison.areTypesCompatible(leg, operandType)`, which `checkCompatibility` and
`Verifier.validateBinaryComparison` both call. For `COUNT` it is `Count.resolveType`. For `IS NULL` it
is the always-resolved `UnaryScalarFunction`. For `SORT` it is `DataType.isSortable`. Adding an
operation therefore adds no union rule, and adding a type adds no operation rule.

This matters because the two framings disagree on real inputs, and because the obvious table is the
wrong one:

- `EsqlDataTypeConverter.commonType` is **not** the comparison oracle. It is the promotion oracle, used
  by `In.toEvaluator` and `Cast`. It says `UNSIGNED_LONG` and `LONG` are compatible where the operator
  refuses them, and it returns null for `DATETIME`×`DATE_NANOS` where the operator accepts them.
- Per-leg-against-the-operand admits leg sets that no common type covers. `ts == "2024-01-01"` on
  `datetime｜keyword` resolves on both legs — the datetime leg via the implicit string-literal cast in
  `isStringImplicitlyCastableTo`, the keyword leg as a string comparison — while
  `commonType(DATETIME, KEYWORD)` is null.

Under `areTypesCompatible` the comparability classes over the leg types are
`{integer, long, double}`, `{unsigned_long}` **alone** — islanded deliberately, since unsigned longs
travel as longs and a conversion to double cannot distinguish the upper range — `{keyword, text}`,
`{datetime, date_nanos}` (the operator carries dedicated millis/nanos evaluators), and `boolean`, `ip`,
`version`, `flattened` each alone.

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
| always allowed | no | render, `TYPEOF`, `COUNT`, `IS NULL`/`IS NOT NULL`, leg selection, cast |
| conditional | **yes** — every leg against the operand, at analysis | comparison |
| always refused | no | `SORT`, `MIN`, `MAX`, `SUM`, `AVG`, grouping |

The conditional tier is decided by the legs a column *may* hold, never by the values it happens to
hold, so the same query on the same mapping always has the same fate.

Grouping on a bare union is **deferred**, not impossible — see §4.10. It was briefly in the allowed
tier, on the reasoning that `(leg, value)` identity needs no rule relating two types. That reasoning
still holds; what moved it is scope. It is the one place this design would go beyond every other
engine, under multi-value it is the most expensive remaining increment, and deferring is the
reversible direction, since I1 forbids later *changing* grouping semantics but not later adding them.

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
→ **refused.**

```
cannot use [==] on [duration]: its [keyword] values cannot be compared with [integer].
Cast it: [duration::long == 200], or select one type: [TYPED(duration, "long") == 200]
```

Per leg: `areTypesCompatible(LONG, INTEGER)` is true, `(DOUBLE, INTEGER)` is true, and
`(KEYWORD, INTEGER)` is **false** — ES|QL has no number↔string conversion. One leg refuses, so the
operation refuses (§6). The same query on a `long｜double` corpus returns `200`.

```esql
FROM logs-* | WHERE duration == "200"
```
→ **refused**, the mirror image: the keyword leg resolves, the numeric legs do not.

The literal `"200"` is *not* implicitly read as a number. A foldable string literal adopts the other
operand's type only for `datetime`, `date_nanos`, `ip`, `version` and `boolean`, per
`EsqlDataTypeConverter.isStringImplicitlyCastableTo` — which is also why the same query shape
**resolves** on `datetime｜keyword`: there the string literal becomes a datetime on the datetime leg and
stays a string on the keyword leg, so every leg resolves and the comparison runs. That asymmetry is
existing per-type behaviour, not a union rule, and I2 keeps it.

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
→ **refused**, for the reason in §4.2: the keyword leg does not resolve against an integer.

```esql
FROM logs-ab-* | WHERE duration > 200     // legs: double, long
```
→ **`250`**, **`300`**.

Every leg resolves, so the comparison runs — and it runs as the *same Lucene query a typed field would
produce*. `PushFiltersToSource` translates it to a range on the field's real name, and each shard's own
`MappedFieldType` parses the bound, so a long-mapped shard gets exactly the query it would have got had
the field never conflicted. No conversion is inserted and no precision is lost, which an earlier sketch
of this (`duration::double > 200`) would have done on both counts: a cast attribute carries a synthetic
name that is not pushable, and `long`→`double` loses precision above 2^53.

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
→ **refused.**

```
cannot group by [duration]: it holds [double], [keyword] and [long] values.
Group by type: [BY TYPEOF(duration)], select one type: [BY TYPED(duration, "long")],
or cast it: [BY duration::long]
```

```esql
FROM logs-* | STATS rows = COUNT(*) BY type = TYPEOF(duration)
```

| `type` | `rows` |
|---|---|
| `double` | 1 |
| `keyword` | 3 |
| `long` | 3 |

The second query is the one that matters in practice, and it needs nothing from this design beyond
`TYPEOF`: the grouping key is a `keyword`, hashed by the ordinary block hash. "What is actually in this
field" is answered inside the always-allowed tier.

**The semantics, recorded because they are settled and only the scope is not.** Were grouping on a bare
union allowed, identity would be `(leg, value)` — the long `200` and the keyword `"200"` in different
buckets. That is derived rather than chosen: per leg, the long column groups its `200` and the keyword
column groups its `"200"`, and merging them presupposes converting one, which is a cast and so the
user's call. ClickHouse agrees where it can be forced, and its own documented example keeps `1::UInt32`
and `1::Int64` in separate groups — two integers holding the same number. The asymmetry it would own is
that `(leg, value)` identity diverges from `==`; that is not a new kind of inconsistency, since
`X-MultivalueDedupe` already hashes doubles through `Double.doubleToLongBits`, separating `-0.0` from
`0.0`, while `Equals.processDoubles` is `lhs == rhs` and unifies them.

Why it is deferred rather than shipped: it is the one place this design would go beyond every engine
measured; under multi-value it needs a dedicated hash, because a packed hash over the tag and leg
channels cross-products them — a position holding `[long 200, keyword "x"]` yields tag `[0,1]`, long
`[200,null]` and keyword `[null,"x"]`, so eight combinations where two are wanted; and I1 forbids later
*changing* grouping semantics but not later adding them, so deferring is the reversible direction.

### 4.11 Null

```esql
FROM logs-* | WHERE duration IS NOT NULL
```

Answered from the tag alone. A value with no tag is null; `IS NULL` and `IS NOT NULL` never consult a
leg, and they are defined on every union without exception.

A field **unmapped** in some indices gets a `keyword` leg, because that is what those indices load it
as from `_source`. The leg is always declared even when `unmapped_fields` leaves it empty: an empty leg
costs a null block, whereas a missing leg would have nowhere to put a value.

**Declared is not loaded, and today it is only declared.** `TypeConflictedField.unionLegTypes` appends
the keyword leg, but `UnionEsField.from` drops `isPotentiallyUnmapped()`, `FieldAttribute`'s
potentially-unmapped test is false for a `UnionEsField`, and `PlannerUtils`' unmapped-field collector
does not see it — so `blockLoaderAndConverter` returns constant nulls for a source-read field whose
attribute type is not `keyword`. Either the flag travels on `UnionEsField` and the leg is loaded from
`_source`, or this specification promises a leg that is always empty. The POC takes the first; until it
does, the leg is declared and unloaded.

### 4.12 Two unions compared

```esql
FROM logs-* | WHERE duration == timeout
```

Per leg *pair*. A row compares by the two legs its two values actually occupy: `long == long` is
defined, `long == keyword` is undefined and null, and so on across the cross product. Refused only if
no pair of legs is defined. There is no "compare the unions" rule — there is no union to compare, only
two values whose types each row decides.

### 4.13 Multi-valued columns

**In scope, and a prerequisite rather than a follow-up.** Any multi-valued mapped field — an array of
strings in one index, an array of longs in another — produces a multi-valued union column as soon as a
loader exists. Refusing it would refuse ordinary data, and the rule is to narrow the operations, never
the input the engine must survive.

A union **value** is single-typed. A union **row** need not be. Loaded data happens to be single-typed
per row, because within one index a field path has one mapper and the reader picks the loader per
document's shard; computed values are not, since `VALUES()`, `MV_APPEND` and `TOP` can put a long and a
keyword in the same row. The logical model is therefore **per value, not per row** — and so is the
layout, because the tag is indexed by sub-block position and the invariant never requires equal tags
within a position. **No layout change is needed.**

Every operation in §4 applies per value, and nothing in this section distinguishes the single- and
multi-valued cases.

What it costs is implementation, not design: `AbstractDelegatingCompoundBlock` throws
`"Not yet implemented for multi-valued blocks"` from `filter`, `slice` and `keepMask`, and
`UnionArrayBlock.expand` with them, so the first `FilterOperator` or `LimitOperator` after the loader
would throw. `deepCopy` already handles multi-value. Fixing the base class lifts `TDigestArrayBlock`
and `ExponentialHistogramArrayBlock` at the same time, which throw the identical message.

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
| a comparison that does not resolve on **every** leg | the single-typed column refuses it too (I3) | `::`, `TYPED` |
| `SORT` on a union | a permutation cannot carry an unknown (§4.6) | `::`, `TYPED` |
| `MIN`, `MAX`, `SUM`, `AVG` | per-leg fan-out across the reduce boundary, for an answer that is usually unknown (§4.7) | `MAX(x::long)` |
| `STATS … BY` a bare union | deferred on scope (§4.10) | `BY TYPEOF(x)`, `BY TYPED(x, "t")`, `BY x::t` |
| `::any` applied to anything but a literal | a computed value has already committed | drop it |
| `::any` on both sides of an operator | no type for either to resolve against | type one side |
| an ordering comparison against `::any` | no monotonicity metadata exists (§4.5) | `::`, `TYPED` |

**A multi-valued union column is not on this list, and must not be.** Any multi-valued mapped field
produces one the moment a loader exists, so refusing it refuses ordinary data rather than an ordinary
operation. The rule is to narrow the *operations*, never the input the engine must survive. What this
costs is stated in §8: `AbstractDelegatingCompoundBlock` refuses multi-valued `filter`, `slice` and
`keepMask` today, and `UnionArrayBlock.expand` with them, so the support is a prerequisite of the
loader and not a follow-up to it.

`KEEP`, `RENAME`, `EVAL`-free projection, `WHERE` on a resolvable comparison, `MV_EXPAND`, `FORK` and
rendering are **defined on every union**. Where the implementation refuses one of those it is a defect
to fix, not behaviour to specify — see §8.

## 6. The undefined-leg policy: strict

Where an operation is defined on one leg and **undefined** on another — `duration > 200` on
`long｜keyword` — **the query is refused.** The refusal is computed from the declared leg set at
analysis, so it does not depend on which values the column turns out to hold.

```
cannot use [>] on [duration]: its [keyword] values cannot be compared with [integer].
Cast it: [duration::long > 200], or select one type: [TYPED(duration, "long") > 200]
```

The message names **the leg that refuses and what it was compared against**, because that is what the
rule actually consulted. An earlier wording said "which have no common type", which described a
different and wrong rule: the oracle is per leg against the operand (§2.1), so a leg set with no common
type can still resolve, and one with a common type can still refuse.

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

**And strict is load-bearing for pushdown correctness, not only for I1.** A comparison that survives
analysis is pushed to each shard under the field's real name, where that shard's own `MappedFieldType`
parses the literal. That is only sound because every leg resolved against the operand: a leg set that
refused — `long｜keyword` against `200` — would otherwise push `term duration:200` to a keyword-mapped
shard and match the string `"200"`, which is `::any` semantics rather than `==` semantics. Relaxing to
lenient later would therefore require per-leg pushdown first, not merely a changed warning.

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

Three runtime faults, each of which must be fixed before the behaviour above can be observed at all:

- **The loader throws on multi-segment pages only.** `ValuesReader.Run.initFinalBuilders` builds each
  final builder from `fields[f].info.type().newBlockBuilder(...)`, and `ElementType.UNION`'s builder
  supplier throws `"a union block builder needs the leg element types"` by construction, since the enum
  cannot know them. `ValuesFromSingleReader` bypasses `finalBuilders`, so single-segment pages would
  succeed and multi-segment pages would throw — an intermittent failure, which is the worst shape.
  `ValuesSourceReaderOperator.FieldInfo` has to carry a builder supplier, not only an `ElementType`.
- **The leg list travels with the field, so a renamed union cannot render.** `originalTypes` is filled
  only for an `UnsupportedAttribute`; `RENAME duration AS d`, `MV_EXPAND duration` and an aliased
  grouping each yield a `ReferenceAttribute` of type `UNION` carrying no leg list, and rendering then
  fails for want of one. The fix that keeps §5's promise is for the block to carry its leg type
  **names** — compute cannot name a `DataType` but can carry strings — which also retires the
  two-sided "both ends derive the canonical order independently" contract in favour of one side
  carrying it.
- **`SORT` on another column with a union in the projection throws.** `ValueExtractor.extractorFor`
  and `ResultBuilder.resultBuilderFor` have no `UNION` arm, so `FROM logs-* | SORT @timestamp | KEEP
  duration` — the Discover query — dies in `TopNOperator` even though the sort key is not the union.
  Refusing `SORT` **on** a union (§4.6) does not cover this and never did.

And `STATS … BY duration` passes analysis today, because `Aggregate.checkUnsupportedGroupingType` lists
counters, `aggregate_metric_double`, `date_period`, `date_range`, `exponential_histogram`,
`partial_agg`, `tdigest` and `time_duration` but not `union`. §4.10's refusal has to be added there, or
the query reaches `BlockHash` and fails at runtime instead of at analysis.

Three further refusals are **mechanical**, not specified:

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

## 9. Compatibility

The union type adds a wire type, a response shape and a set of refusals, and each of those is a
promise. Three of the four hazards below are about **planning**, not serialization, because the tree's
own position is that serialization is a backstop: `DataType.writeTo` throws for a type the remote node
does not understand, and the comment above that throw calls it "a bug, we failed to account for an old
node during planning".

### 9.1 A mixed cluster

A union may only be built when every node can read the type. The creation point therefore asks the
type for its own supported version against the cluster minimum, rather than testing a hardcoded
version, so the check cannot drift from the version `DataType.UNION` declares. Where the cluster is
too old the conflict is erased exactly as it is with the pragma off, so **an older cluster loses the
capability and changes no answer** — which is §2.3's I1 applied to versions instead of to legs.

Without that check the user gets a 500 that the code itself labels a bug, from the serialization
guard, part-way through shipping the plan. That is the condition this section exists to prevent, and
the gate beside it for `CompactMultiTypeEsField` is the precedent it follows.

### 9.2 The leg order is a wire contract, and it is carried rather than derived twice

A value's tag is an index into the leg list, so that list is a contract between whoever writes a tag
and whoever reads one. **It is computed once, on the coordinator, and carried.**
`TypeConflictedField.unionLegTypes()` — widen, de-duplicate, sort by type name — has exactly one
production caller, the analyzer's `UnionTypesCleanup`, and the list it returns is held on the
`UnionEsField` and serialized inside the plan. Both consumers then index that same carried list: the
data node through `PlannerUtils.unionLegElementTypes` to lay out the block's legs, and the response
through `ColumnInfoImpl.forAttribute` to declare them by name. So changing the ordering rule between
versions cannot make two nodes disagree about a single query, because only one node ever runs it.

*An earlier draft of this section asserted the opposite — that both ends computed the function — and
prescribed carrying the names in the block as the fix. That was wrong about the tree.*

What the two ends do still derive separately is the mapping from a leg's **type** to its physical
**element type** (the data node) and to its **renderer** (the coordinator). Those agree because both
walk the carried list in order and the block itself serializes its element types; `checkUnionLegCount`
catches a length disagreement, and a per-leg element-type disagreement would need `toElementType` to
change meaning for an existing type between versions, which would break every column rather than only
a union. The remaining exposure is therefore ordinary, not union-specific.

**One thing is genuinely published here and cannot be withdrawn:** the leg names go out in
`original_types`, in the mapping vocabulary field_caps uses (`date`, not `datetime`), in leg order. A
client may read that list positionally, which §9.4 is about.

### 9.3 Upgrade must not change answers

I1 governs adding a leg. There is a second axis: **which types are admitted as legs at all.** Widening
that set turns a conflict that was `UNSUPPORTED` into a union, and `KEEP duration` then goes from a
column of nulls to a column of values — an answer change caused by a version rather than by the data.

So I1 needs its version twin: **capability may grow with a version, but a query that worked must keep
its answer.** The nulls-to-values transition breaks that, which makes enabling the union a deliberate
one-time decision rather than an incremental one. The pragma defaulting off is what holds it for now,
and after it is on, the admitted-leg-type set may only grow in ways that leave working queries alone.

### 9.4 The response shape

`type: "union"` and the per-column leg list become a client contract on first release and cannot be
withdrawn. The sharper point is that `original_types` is **reused**: it exists today to describe a
column that failed, and it will now also describe one that works. A client treating its presence as
"this column is unusable" will misread a union, so the discriminator has to be `type`, not the presence
of `original_types`.

### 9.5 Refusals are the forward-safe direction

A refusal can become a definition later, because an error is not an answer: no working query changes
when one is relaxed. That is why §6 is strict and why `SORT`, extrema and grouping are refused rather
than approximated. The one relaxation that is not free is strict to lenient, which §6 shows would
require per-leg pushdown first — the refusal is currently what makes pushing a comparison under the
field's real name sound.

## 10. Open

- **`!=`.** Three-valued (`null` where `==` is null) or un-bracketed, so that a keyword never
  "differs from" a long? Three-valued is the consistent reading and is assumed above.
- **Lexical ordering within a leg.** `duration >= "150"` on the keyword leg is defined, and compares
  lexically. That is what a keyword column does today, so I2 keeps it — but it is worth confirming
  that a user reading a mixed column finds it intuitive rather than surprising.
- ~~**`UNSIGNED_LONG` beside `LONG`.**~~ **Settled: refused**, by the operator's own rule rather than
  by anything this design adds. `areTypesCompatible` admits unsigned_long only against another
  unsigned_long, for the reason its javadoc gives — unsigned longs travel through the engine as longs,
  negative values cannot be represented, and a conversion to double can no longer distinguish upper-range
  values. So `long｜unsigned_long` refuses every comparison, including against a plain integer literal.
  It is the sharpest case where the rule is right and the user will disagree: two columns of
  non-negative integers that will not compare. The escape is `::long`, and `ToLong` from `UNSIGNED_LONG`
  exists.
- **The operational cost of I1, which is accepted but should be measured.** A `long` field that gains
  one keyword-mapped index flips every `WHERE duration > 200` on it from working to refused, at
  analysis. That is I1 behaving as intended — loudly — but the realistic trigger is template drift
  adding a single index, and the blast radius is every saved query on that field. ClickHouse behaves
  the same way (`NO_COMMON_TYPE`), so this is not a divergence; it is still the thing most likely to
  be reported as a regression.
