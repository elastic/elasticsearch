# POC plan: the union type

Worktree `/Users/oleglvovitch/prompts/esql-union-typed-poc/elasticsearch`, branch `esql-union-typed-poc`,
draft PR 164982. Grounded at `c44c3faa3ff3`. Every claim below was read off the statements that
execute, not off a javadoc or a comment. Where a claim was not traced, it says so.

This plan is for the design in `UNION-TYPES-ARCHITECTURE.md`: a real data type, a block carrying
per-leg values with a tag, rendering, filtering, grouping and per-type aggregation. An earlier version
of this file planned the cheap alternative instead, two functions over the existing per-index
conversion machinery and no new type. That question is closed, so the increments and findings that
served only it are gone.

The PR stays a draft and its title and body stay short.

---

## 0. Where the increments stand

**A. The union data type. Done, `31b9bb7c49ed`.** `DataType.UNION`, `esType` `union`, under
construction against a transport version, never an expression value. The regression guard was
`CsvIT.*union_types*`, which passes, so nothing user-visible moved.

**A-fix. Representability sequenced with its renderer. Done, `c44c3faa3ff3`.** Increment A declared
the type representable in output while nothing could render it, which the tree's own invariants
forbid: `ArrowResponse`'s static initializer asserts a formatter is non-null exactly when
`DataType.isRepresentableInOutput`, and `ArrowResponseTests.testOutputTypesHaveTextSupport` requires a
non-null `ResponseValueUtils.valueExtractorFor`. That cost eight part-3 failures. Representability now
arrives with increment D, and `DataTypeConversionTests.testUnionIsNotAnExpressionValue` is the
assertion that fails when it does.

**B. The block. In progress.** `ElementType.UNION` (code 15), `UnionBlock`, `UnionArrayBlock`,
`UnionBlockBuilder`, `BlockFactory.newUnionBlockBuilder`, and `PlannerUtils.toElementType` mapping
`UNION` to it. §1 is what the code decided differently from the first draft.

**D. Rendering. Done, `e10184512a12`.** A union column renders each value as the type its own leg
declares. The renderer chain takes the column rather than the column's type, because only the column
carries the leg types; `PositionToXContent` already took the column, so the two renderers now agree.
JSON, the values iterator, the text table and csv and tsv all go through it, the text formats via
`response.values()`. The type is representable in output as of this increment, which is what
`DataTypeConversionTests` was waiting for. Still owed here: the Arrow sparse-union write-out, which
keeps the pending-formatter marker for now as aggregate_metric_double does, and a `CsvTestUtils.Type`
member so csv-spec tests can assert on a union column.

**C. Stop erasing. Partly done.** C1, the coordinator half, is done in `e9579adcc512`: under the
`union_typed_fields` pragma `UnionTypesCleanup` builds a `UnionEsField` instead of flagging the
conflict, so the attribute survives analysis. **C2, the data-node loader, and C3, the response's leg
list, are not started** — and without C2 nothing in the engine produces a union block, so with the
pragma on, reading such a field fails on the data node.

**E. Operations.** Not started. **F. Grouping.** Out of the POC, see §0a. **G. Datasets.** Not started.

**Done since, against §3's increment list:**

- **Increment 2, multi-value in the shared compound block.** `AbstractDelegatingCompoundBlock` now
  reshapes a multi-valued block for `filter` and `slice`, measuring each position's run by the value
  indexes rather than by `getValueCount` — a null holds no values but still occupies one sub-block
  position, so measuring by value counts would drop it and misalign every tag after it.
  `UnionBlockTests` covers a position spanning two legs, a kept null, a repeated position and both
  slice shapes. `keepMask` is still refused: it would have to turn a masked-off multi-valued position
  into a single null, which changes the sub-position count and so cannot be done by reshaping and
  delegating, and its only caller is the filtered aggregation path a union does not reach. The whole
  `compute.data` suite is green, so tdigest and exponential histogram are unaffected.
- **Grouping refused at analysis.** `Aggregate.checkUnsupportedGroupingType` had no `UNION` entry, so
  `STATS … BY` a union passed analysis and failed in `BlockHash` with an element type the user never
  wrote. It now fails during analysis with a message naming `TYPEOF`, `TYPED` and the cast.
- **The mixed-cluster gate.** `UnionTypesCleanup` built a union on the pragma alone, with no cluster
  minimum-version check, so a mixed cluster would have hit the `DataType.writeTo` guard — whose own
  comment calls that a planning bug rather than a user error. It now also requires the type's declared
  supported version against `context.minimumVersion()`, asking the type rather than hardcoding a
  version, and degrades to the pragma-off behaviour so an older cluster loses the capability and
  changes no answer.

**Deliberately not done: increment 1, deriving `isLegType`.** It delivers no user-visible capability,
and it widens leg admissibility to spatial, grid, histogram and counter types, which changes behaviour
and requires rewriting the tests that pin the current boundary. It is also the one increment whose
honest form is not the one proposed: the derivation as sketched would have `core/type/UnionEsField`
call `PlannerUtils` and the compute module, and that package imports neither today. The right shape is
a property the type's own author declares on the `DataType` builder, beside `isWholeNumber` and
`docValues`, with `canHoldLeg` as its physical counterpart and a test proving the two agree.

---

## 0a. The minimal POC — the product shape

**You can read a column whose indices disagree about its type, see what is in it, filter it where the
comparison resolves on every leg, and reduce it to one type. Ranking, totals and grouping need a cast.**

### What works

| | Query | Needs |
|---|---|---|
| see it | `FROM logs-* \| KEEP duration` | the loader, the leg list |
| rename it | `RENAME duration AS d` | the leg list on the block |
| inspect it | `STATS rows = COUNT(*) BY type = TYPEOF(duration)` | `TYPEOF` (new); groups on a **keyword** |
| count it | `STATS COUNT(duration)`, `WHERE duration IS NOT NULL` | the tag only |
| filter it | `WHERE duration > 200` where every leg resolves against the operand | per-leg resolution + native pushdown |
| escape it | `TYPED(duration, "long")`, `duration::long` | union wiring + tests; the cast exists |
| expand it | `MV_EXPAND duration` | multi-value, which is a prerequisite anyway |

### What is refused

| Refused | Escape named in the message |
|---|---|
| a comparison that does not resolve on every leg | `duration::long > 200`, `TYPED(duration, "long") > 200` |
| `SORT duration` | the same two |
| `MIN` / `MAX` / `SUM` / `AVG` | `MAX(duration::long)` |
| `STATS … BY duration` | `BY TYPEOF(duration)`, `BY TYPED(duration, "t")`, `BY duration::t` |
| `::any` | not in this POC |

**Not refused: a multi-valued union column.** It is produced by any multi-valued mapped field the
moment the loader exists, so it is a prerequisite, not a limit. The rule is to narrow the operations,
never the input.

### Why the line falls there

**Comparison is in, and it is cheaper and better than the earlier sketch.** The oracle is the
operator's own `areTypesCompatible`, per leg against the operand — not a common type across the leg
set. Where every leg resolves, the filter is pushed to each shard under the field's **real name** and
that shard's own `MappedFieldType` parses the literal, so a long-mapped shard receives precisely the
query it would have received had the field never conflicted. That is the performance parity the design
requires. The earlier `duration::double > 200` sketch would have failed it twice: a cast attribute
carries a synthetic `$$…$converted_to$…` name that is not pushed at all, and `long`→`double` loses
precision above 2^53 where the native per-shard range does not.

**Strict refusal is what makes that pushdown sound**, which is a second reason for it beyond I1: a leg
set that refused would otherwise push `term duration:200` to a keyword-mapped shard and match the
string `"200"`.

**Grouping is out, reversing the earlier call.** It is the one place this design would go beyond every
engine measured; under multi-value it needs a dedicated hash rather than the packed one, because a
packed hash cross-products the tag and leg channels; and `STATS … BY TYPEOF(duration)` already answers
the question grouping was wanted for, with no union machinery at all. I1 forbids later changing
grouping semantics but not later adding them, so deferring is reversible. The semantics stay recorded
in `UNION-TYPES-SPEC.md` §4.10.

**Extrema and sums are out** because `AggregateMapper.toDataType` throws for `UNION`, aggregators are
strongly typed with generated classes chosen at planning, a union aggregate must fan out per leg across
the reduce boundary, and the answer on a genuinely mixed field is usually unknown anyway. ClickHouse
refuses `max` even on an all-numeric Variant.

## 0b. What ClickHouse does, measured

Run against ClickHouse local 26.5.1, `Variant(UInt64, Float64, String)` holding
`200, 250, 300 (UInt64)`, `100.0 (Float64)`, `'unknown' (String)`, and a second all-numeric
`Variant(UInt64, Float64)` to separate "refuses on the type" from "refuses on the legs".

| | `Variant(UInt64, Float64, String)` | `Variant(UInt64, Float64)` |
|---|---|---|
| render the column | works | works |
| `variantType(v)` | works | works |
| `count(v)` | works | works |
| `v.UInt64` (subcolumn) | works | works |
| `CAST(v AS …)` | works | works |
| `WHERE v = 200` | refused, `NO_COMMON_TYPE` | **works** |
| `WHERE v > 200` | refused, `NO_COMMON_TYPE` | works |
| `max(v)` / `min(v)` / `sum(v)` | refused, `ILLEGAL_TYPE_OF_ARGUMENT` | **refused** |
| `ORDER BY v` | refused, `ILLEGAL_COLUMN` | refused |
| `GROUP BY v` | refused, `ILLEGAL_COLUMN` | refused |

**The all-numeric column is the informative one.** `WHERE v = 200` works there and is refused on the
mixed column, so ClickHouse's comparison rule is not a refusal on the Variant type — it is
`NO_COMMON_TYPE` computed across the legs. Comparison is delegated and refused exactly where no
supertype exists. That is §2.1's principle with the **strict** undefined-leg policy, in a shipping
product.

`max` is refused even on the all-numeric column, so extrema are refused at the type level rather than
per leg — cruder than §4.7, which would answer `300` there.

Both `ORDER BY` and `GROUP BY` are forceable, and what they then do is worth recording:

- `SET allow_suspicious_types_in_order_by = 1` orders `100 (Float64), unknown (String), 200, 250, 300
  (UInt64)` — by variant type index first, then value. The `(leg, value)` tuple order, behind a
  setting whose refusal message says "it can lead to unexpected results". Independent confirmation of
  both §4.6's rejected alternative and the reason for rejecting it.
- `SET allow_suspicious_types_in_group_by = 1` buckets by `(type, value)` — §4.10's identity, also
  gated.

One behaviour with no counterpart here: inserting `'200'::String` into a Variant puts it in the
**UInt64** leg. ClickHouse parses a literal into the best-fitting variant on write, so its Variant
columns are less mixed than ours will be. An Elasticsearch conflict is discovered, never created, so
there is no equivalent lever.

## 0c. Us against that

| | ClickHouse | This POC |
|---|---|---|
| render | yes | yes |
| type of each value | `variantType(v)` | `TYPEOF(x)` |
| count, null checks | yes | yes |
| select one leg | `v.UInt64` | `TYPED(x, "long")` |
| cast | yes | `x::long`, already works |
| filter | iff the legs share a supertype | **same rule** |
| `SORT` | refused, forceable | refused |
| `MIN`/`MAX`/`SUM` | refused | refused |
| group | refused, forceable | **allowed, ungated** |

**Parity everywhere, and one step beyond on grouping.** `STATS … BY x` on `(leg, value)` is the query
that makes a messy field legible, and unlike a forced `ORDER BY` its answer is never surprising: one
bucket per distinct type-and-value. ClickHouse considers it suspicious enough to gate; this is the one
place to disagree deliberately rather than by omission.

**Our filter rule is not theirs in mechanism, though it agrees on these cases.** Ours is per leg
against the operand, using the operator's own `areTypesCompatible`; a leg-set common type would be the
wrong rule, since `commonType` islands `unsigned_long` where the operator does and joins
`datetime`×`date_nanos` where the operator does not. See `UNION-TYPES-SPEC.md` §2.1. An earlier version
of this section proposed implementing the filter as `duration::double > 200` and claimed that was
already pushdown-capable; both halves were wrong, and §0a now states what replaces them.

---

## 0d. Review findings, judged

From `~/.claude/scratch/PR#164982.md`, polled against head `e9579adcc512`. Acted on, with the
reasoning, since several turn on decisions rather than defects.

**Fixed.**

- **A value index is not a position.** `UnionBlockBuilder.appendValue` guarded with
  `source.isNull(sourceValueIndex)`, where `Block.isNull` takes a position and the typed getters beside
  it take a value index. They coincide only for a single-valued source, so a source with position 0
  multi-valued and position 1 null would have refused a value that was plainly there. Now guarded only
  where the two coincide, with the contract stated: a null position holds no values, so no valid value
  index can refer to one, and the caller owns that. This mattered more than the review could know - the
  loader appends per value from natively-loaded blocks, which are routinely multi-valued.
- **The Java-object arm and the builder throw are now tested.** `BlockUtils.toJavaObject` over a union,
  and `ElementType.UNION.newBlockBuilder` throwing. The second is load-bearing rather than defensive:
  the multi-segment read path builds final builders from the element type alone, which is why the
  loader has to be handed the leg types instead.
- **Two in-code prose claims that were false.** The `Coalesce` comment asserted that a union is
  destructured before anything coalesces it; nothing destructures, `Coalesce` types itself off its
  first child with no representability test, and while `Eval` refuses a union return type a `WHERE`
  does not - so `WHERE COALESCE(u, u) IS NULL` passes analysis and reaches that throw. The comment now
  says that, and names the throw as a backstop rather than a contract. And a doubled javadoc in
  `AnalyzerTests` where the first block described the method after it.
- **A dead wrapper.** `UnionEsField.from` duplicated the analyzer's single construction site and
  recomputed `unionLegTypes`, with one test caller. Deleted; its test now pins the construction
  `UnionTypesCleanup` actually performs. `legOf` stays - the loader needs it to map a shard's mapped
  type to a leg.

**Already fixed before the poll:** the `canHoldLeg` javadoc, which now credits the test that holds the
two lists together rather than claiming the logical layer calls it.

**Recorded, not fixed.**

- **The transport version.** `DataType.UNION` is gated on `INDEX_SOURCE`, borrowed from an unrelated
  feature, where the sibling precedent is a constant named for the type and the two other reuses each
  carry a comment saying why they are safe. This now matters more than when it was written, because the
  mixed-cluster gate is built on it. A dedicated constant needs `generateTransportVersion`; until then
  the borrowing is the recorded decision, not an oversight.
- **Design documents at the repository root.** Three now. Internal design documents live in
  `docs/internal/`, and these carry local paths and review-process references. All of it leaves before
  the PR un-drafts.
- **`TYPED` loads the non-matching leg and discards it.** The result is known at planning time, so a
  constant-null loader would skip the read; a keyword or vector leg pays a full extraction for nothing.
  A cost finding, and this is not a performance increment.

**Standing finding.** The review's Q2 and the adversarial plan review found the same thing
independently: `TopNExec` maps `UNION` to an unsortable encoder, which says a union may ride through a
sort on another column, while `ResultBuilder` and `ValueExtractor` have no union arm and throw. Two
reviewers reaching it by different routes is why it is increment 7 rather than a footnote.

**Open and genuinely the user's call**, both offered as alternatives rather than defects: whether
`UnionEsField` should be renamed so it does not read as a sibling of `UnionTypeEsField`, which means
something else entirely; and whether a union's leg list belongs in `original_types` at all, given that
field is diagnostic for an unsupported column and would become a positional contract for a union.
`UNION-TYPES-SPEC.md` §9.4 records the consequence either way.

## 1. What the code decided for increment B

**The discriminator is an explicit tag sub-block, not an implicit one.**
`AbstractDelegatingCompoundBlock` derives nullity from `getSubBlocks().getFirst()` in `isNull`,
`mayHaveNulls`, `areAllValuesNull` and `getTotalValueCount`, all of them final. A leg cannot be that
sub-block, since each leg is null wherever the value has another type. So the tag is a sub-block of
its own, placed first, holding the leg index per value. In exchange the block inherits that base
class's `filter`, `slice`, `keepMask`, `deepCopy`, ref counting, breaker accounting and multivalue
serialization metadata. Moving the tag out of first position fails exactly four tests, so the ordering
is pinned rather than incidental.

**The block carries physical leg types, and the logical ones travel beside it.** `compute` depends on
`esql-core` only as `testImplementation`, and its main source has no import of `xpack.esql.core`, so a
block cannot name a `DataType`. The logical type per leg is therefore the leg order, agreed between
the plan and the response, and `ColumnInfoImpl.originalTypes` already ships that list to clients for a
conflicted column. Two legs may share an element type while differing logically, so a leg is addressed
by index and never by its element type.

**The block carries no transport version of its own, deliberately.** A block names its element type on
the wire as a code, and a node without the union code fails decoding that code before reading the
block's body, so there is nothing a version guard would add. Tdigest and exponential histogram guard
`writeMultiValueMetadata` only because they had readers in the field before multi-value support
reached them; the long-range and double-range blocks, both newer, have no guard. The commit message
for the block justified this by the data type's own under-construction gate, which is wrong: a page
does not always have a plan in front of it, and `DataType.writeTo` throws only toward a non-snapshot
peer.

**Declared leg types are serialized, not derived.** A leg can arrive as a `ConstantNullBlock` whose own
element type is `NULL`, which is what happens when a union is assembled from per-leg blocks and one leg
matched no row. So `writeTo` writes the declared types and `readFrom` reads them back. Deriving them
from the leg blocks instead fails exactly one test. A leg built through the builder does keep its own
element type even when all its positions are null, so only the assembled case needs this.

**Multi-valued union columns are refused late, not prevented.** A multi-valued union can be built,
serialized and shipped; `AbstractDelegatingCompoundBlock` throws
`Not yet implemented for multi-valued blocks` from `filter`, `slice`, `keepMask` and `expand` when
`firstValueIndexes != null`, so such a column fails at the first of those downstream rather than at
construction. That is loud but late, and it is what tdigest and exponential histogram do too, since
`beginPositionEntry` is final on the shared builder and none of them refuse it.
`UnionBlockTests.testMultivaluedFilterIsRefused` holds that line visible. Supporting multivalue
properly is a change to that shared base class, benefiting three block types, and it is its own
increment. An earlier version of this line said the block "is single-valued", which overstated what
anything enforces.

**A union block builder cannot come from `ElementType` alone**, since it needs the leg types, exactly
as `COMPOSITE` cannot. So `ElementType.UNION`'s builder supplier throws and
`BlockFactory.newUnionBlockBuilder` takes the leg types. Consequences, each a site that now refuses a
union rather than failing inside a builder: `FetchGather` (late materialization), `QueryList`
(lookup-join keys), `BlockUtils.appendValue`, `AggregateMapper.toDataType` (aggregator intermediate
state), `CsvTestUtils.Type.asType` (csv-spec assertions), and `RandomBlock` plus `BlockTestUtils`, so
no randomized suite generates a union block. `BlockUtils.valueAtOffset` does handle one, by reading
whichever leg the tag names, since the Java object it returns carries its own class.

---

## 2. Corrections to the earlier plan's findings

**F3 was wrong.** It claimed synthetic union attributes collapse on name and output type because
`CompactMultiTypeEsField` has no `equals` override, so that `TYPEOF(x)` and `TO_STRING(x)` would become
whichever was created first. `CompactMultiTypeEsField.equals` does override it and does compare
`typeToConversionExpressions` and `unmappedConversionExpression`, so two conversion functions with
different expression maps do not collapse. The increment that existed to fix this is gone.

**F2 was backwards about pushdown.** A union-typed attribute is pushable when `attr.name()` is a real
field name; it is the synthetic `$$<field>$converted_to$<type>` name that is not.
`LocalPhysicalPlanOptimizerTests.testToDateNanosPushDown` is the existing demonstration. Pushdown is a
naming question rather than a missing mechanism, which is why the design gives these attributes the
real field name.

---

## 3. Remaining increments

Each is independently committable, leaves the tree green, and ends with a Fable adversarial review of
the diff. Ordering is load-bearing in three places, noted inline.

**1. Derive `isLegType`; delete the exclusion map.** Three hand-maintained lists express one concept.
Replace `UnionEsField.isLegType`'s switch with a derivation — widened to itself, a mapping type, has an
element type, and `UnionBlockBuilder.canHoldLeg` accepts it — leaving `canHoldLeg` as the single
hand-written list, which is the one genuinely physical statement ("this element type has a scalar
append") and lives beside the builder's switch. `NOT_LEG_TYPES_ON_PURPOSE` in `UnionEsFieldTests` goes;
its test becomes a property over `DataType.values()`: every admitted type round-trips through
`UnionBlockBuilder`. This admits spatial, grid, histogram and counter legs, which is intended — legs
load with `FieldExtractPreference.NONE`, which fixes a spatial leg's element type, and a UNION-typed
attribute never enters the doc-values/centroid/bounds sets that would ask for another preference.
*Red on revert:* the new property test, plus `AnalyzerTests.testAConflictWithALegNoBlockCanHoldIsNotAUnion`,
which must be rewritten against a genuinely unholdable pair such as `keyword｜aggregate_metric_double`.

**2. Multi-value in `AbstractDelegatingCompoundBlock` — `filter`, `slice`, `keepMask`, and
`UnionArrayBlock.expand`.** *This must precede the loader*, not follow it: any multi-valued mapped field
produces a multi-valued union block, and the first `FilterOperator` or `LimitOperator` after it throws.
Pure compute, and it lifts `TDigestArrayBlock` and `ExponentialHistogramArrayBlock`, which throw the
identical message. *Red on revert:* `UnionBlockTests`' refusal tests, inverted.

**3. Randomized coverage of the reshaping. Done.** Not via `RandomBlock`: its `randomBlock` takes an
`ElementType` and nothing else, and a union needs its leg element types, so it throws for `UNION` by
construction and says so. `UnionBlockTests` therefore generates its own union blocks alongside the
leg-and-value pairs they must read back as, and property-tests `filter` with unordered and repeated
positions and `slice` with random bounds against that oracle. Legs are chosen per value rather than
per position, so a position may span legs - the shape only a computed value produces and the one most
likely to break reshaping.

*The tests were shown to discriminate rather than merely pass*: replacing the run measurement with
`getValueCount`, the plausible wrong implementation the javadoc warns about, fails both the randomized
filter test and the hand-written null case on every iteration.

**Not achievable as the plan had it**, and the reason is worth keeping: UNION cannot simply be removed
from the exclusion list in `BlockMultiValuedTests`, because that suite needs `lookup` - which is why
`EXPONENTIAL_HISTOGRAM` sits beside it under a TODO saying exactly that - and a union refuses `lookup`.
`TopNOperatorTests` needs increment 7 first. Both exclusions stay, and the suite's own neighbours
explain why.

**4. The loader, with the builder-supplier fix.** In `EsPhysicalOperationProviders.blockLoaderAndConverter`,
for a `FieldAttribute` over a `UnionEsField`: read the shard's mapped type exactly as the
`CompactMultiTypeEsField` arm does, compute the leg, load natively with the shard's own loader at
`NONE`, and convert by appending each value into a `UnionBlockBuilder` under that tag, multi-valued
positions included. A shard mapped as a type not among the legs — reachable if the mapping moved
between field-caps and execution — loads constant nulls with a warning. **`ValuesSourceReaderOperator.FieldInfo`
must carry a builder supplier**, because `ValuesReader.Run.initFinalBuilders` calls
`info.type().newBlockBuilder(...)` and `ElementType.UNION`'s supplier throws by construction; without
this, single-segment pages pass and multi-segment pages throw. *Red on revert:* the first csv-spec
fixture (6), and a two-shard reader test asserting tags and values.

**5. The leg list on the block, and the response.** `UnionArrayBlock` carries its leg type **names**,
written and read with the block. Renderers take them from the block, falling back to `originalTypes`
only for an all-null page; the transport actions fill `originalTypes` from `UnionEsField.legTypes()`
for a field attribute and from the first non-null block otherwise. This is what lets `RENAME` and
`MV_EXPAND` render, since both produce a `ReferenceAttribute` with no field behind it.

**And it is the fix for a cross-version fault, which is the stronger reason.** While both ends derive
the leg order by computing the same function, they agree only as long as the function is the same on
both — change the widening set, a type name or the unmapped-leg rule between versions and an old node
and a new node compute different orders, so a tag names a different leg on each. No version check
catches it, because both nodes understand the type. Carrying the names turns a derived agreement into
data. See `UNION-TYPES-SPEC.md` §9.2. `CsvTestUtils.Type` gains a union member. *Red on revert:* the response test
with a renamed union column.

**6. The first csv-spec fixture.** `union_types.csv-spec` already loads `sample_data`,
`sample_data_str`, `sample_data_ts_long` and `sample_data_ts_nanos`; a `KEEP` over two of them under the
pragma is the first query that renders a union. Gate on a new snapshot-only capability.

**7. TopN pass-through.** A union in the projection with the sort on another column currently throws in
`TopNOperator`, because `ValueExtractor` and `ResultBuilder` have no union arm — and that is the
Discover query. Encode the tag, delegate to the leg; the result builder needs the same richer
descriptor as 4. *Red on revert:* a csv-spec `SORT @timestamp | KEEP <union>`.

**8. `TYPEOF`, the tag-only tier, and the refusals.** `TYPEOF(x)` is a new scalar returning `keyword`,
reading the tag and naming the leg from the block's leg names, so it agrees with `original_types` by
construction. `COUNT` and `IS NULL`/`IS NOT NULL` already resolve and need csv-spec cases, not code —
except that `IsNotNull`'s pushdown must treat a `UnionEsField` like a plain field, or `WHERE x IS NOT
NULL` runs in compute where a typed field gets an exists query, which breaks the performance-parity
rule on the always-allowed tier. Add UNION to `Aggregate.checkUnsupportedGroupingType` so `STATS … BY`
fails at analysis rather than in `BlockHash`, and add the `SORT`/extrema refusals with their messages.

**9. The comparison tier.** Resolution: when an operand is a union, require every leg to satisfy
`areTypesCompatible` against the other operand, failing with the leg that refuses. Evaluation: per leg,
substitute the leg block into the page and run the existing typed evaluator, combining by tag — no new
per-type code. Pushdown: `LucenePushdownPredicates.isQueryable` gains a `UnionEsField` arm of the same
shape as the `UnionTypeEsField` one, and `isPushableValueAttribute`'s guard must exclude `UnionEsField`
too, since a value query over a union would compare the wrong type. *Red on revert:* an optimizer test
asserting the range reaches `EsQueryExec` under the real field name with no filter above it for a
resolvable leg set, and a refusal for one that is not.

**10. `TYPED`'s tests.** The standard scalar-function suite, which also generates the docs, plus a
csv-spec case. Note `TYPED` on a *conflicted field* already resolves, because `ResolveUnionTypes` runs
before `UnionTypesCleanup` and matches `TypeConflictedField`; what is missing is coverage, and `TYPED`
over a union-typed reference stays refused in the POC.

**11. After the POC:** grouping (§0a), Arrow sparse-union write-out, `::any`, datasets.

### The sixteen guards: no shared supertype, five explicit arms

`UnionTypeEsField` exists because such a column holds **converted** values whose type no shard holds.
A `UnionEsField` column holds **unconverted** values whose leg types are exactly what the shards hold.
They are different kinds of thing, and a shared supertype would hand one of them the other's reasoning
in `isQueryable`, `isPushableValueAttribute`, `IsNotNull.translatable` and the unmapped-field collector.
Handle those four plus the loader explicitly; the other eleven sites are unreachable for a union-typed
attribute, because a date check, an `aggregate_metric_double` check or a synthetic-name check precedes
each. If an abstraction is wanted later it is "field whose attribute name is the real field name", not
a type hierarchy.

## 4. Known limits this plan accepts

- ~~Multivalued union columns~~ — **no longer a limit; increment 2, and a prerequisite of the loader.**
- Late materialization of a union column, refused in `FetchGather`.
- A union as a lookup-join key, refused in `QueryList.createBlockValueReaderForType`.
- No randomized suite generates a union block, since `RandomBlock` cannot build one.
- `FORK` and `UNION ALL` branches go through `ResolveUnionTypesInUnionAll`, untraced here.
- The per-value coercion path for Parquet and ORC, untraced.
