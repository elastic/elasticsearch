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
conflict, so the attribute survives analysis. C2, the data-node loader, is done in `dab39ce2f1dd`
with its two-shard test in this increment: each shard reads the field with its own loader and the
values carry the leg that shard's mapping puts them in. **C3, the response's leg list, is done**
too, in `8664fdb67ed0`: `ColumnInfoImpl.forAttribute` is the one place a column's `original_types` is
decided, and for a union it publishes the leg names in leg order - read off the attribute as of §11,
so a column that is no longer a field still declares them.

**And the cross-version fault §9.2 of the spec claimed does not exist.** Checking it rather than
restating it: `TypeConflictedField.unionLegTypes()` has exactly one production caller, the analyzer,
and `UnionEsField.legTypes()` exactly one, `PlannerUtils.unionLegElementTypes`. The order is computed
once on the coordinator and serialized inside the plan, so both ends index one carried list and no
version can make them disagree about a single query. §9.2 is rewritten to say that, with the wrong
claim marked as such rather than quietly dropped.

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

**4a. The builder-supplier fix. Done.** `ValuesSourceReaderOperator.FieldInfo` now carries a supplier
for the destination block a multi-segment page is assembled into, with a four-argument constructor that
derives it from the element type exactly as before - so every existing field is untouched - and a
five-argument one for a shape the element type cannot describe. `ValuesReader.initFinalBuilders` uses
it. Without this the loader would have passed on single-segment pages and thrown on multi-segment ones,
which is the worst available failure shape. `FieldInfoBuilderSupplierTests` pins all three cases,
including that a union handed the default supplier throws with the element type's own message.

**4b. The loader. Done.** `blockLoaderAndConverter` gains a arm before the
multi-type one, since a `UnionEsField` is a sibling of `UnionTypeEsField` and matches none of its
guards: it reads the shard's own mapped family type, finds the leg, loads natively at `NONE`, and wraps
with `UnionLegConverter`, which converts nothing and only records which leg the values landed in. Two
arms return constant nulls - unmapped on this shard, and a shard whose type is not among the declared
legs because the mapping moved between field_caps and execution. The second returns nulls rather than
failing because the leg list is a wire contract fixed when the plan was built, and widening it here
would change what every other node's tags mean.

`PlannerUtils.unionLegElementTypes` is the single source of the leg element types, used by both the
converter and the `FieldInfo` builder supplier, so the block the loader builds and the block a
multi-segment page assembles into cannot disagree about what a tag indexes. Legs always load at
`NONE`, which is what makes a leg's element type a function of its type alone.

**The ownership contract had to be read rather than assumed**, and it is the trap here:
`TypeConverter`'s own loader hands the page's block straight through with no ref count and the
generated convert evaluators close it, so a `ConverterEvaluator` owns and must release what it is
handed. `UnionLegConverterTests` asserts that, and the assertion was shown to discriminate - dropping
the `try`-with-resources fails it.

**4b-e2e. The two shards of different mappings. Done.** `UnionFieldExtractionTests` writes two real
indices, one mapping `duration` as `long` and one as `keyword`, builds a `DefaultShardContext` over
each, and asks the real `EsPhysicalOperationProviders.extractFields` for the column's `FieldInfo` -
so the leg choice under test is production code rather than the test's own copy of it. One page
holding every document of both shards then goes through a real `ValuesSourceReaderOperator`, and the
assertion is that the result is one `UnionBlock` whose legs are `[BYTES_REF, LONG]`, with every
long-mapped document tagged to the long leg, every keyword-mapped one to the keyword leg, and every
value arriving unconverted.

**It discriminates, and the stack says which path it covers.** Forcing `unionLegLoader` to always
pick leg 0 fails it with `leg 0 holds BYTES_REF but the value came from a LONG block`, thrown from
`UnionBlockBuilder.appendValue` under `ValuesFromManyReader.loadForwardSequence` - the many-shard
reader, which is the path a single-shard test would never enter. The two constant-null arms are
pinned the same way, by identity against `LOAD_CONSTANT_NULLS`: a shard mapping the field as
`boolean`, which no declared leg has room for, and a shard that does not map it at all. The forced
leg kills that first one too.

**What it does not cover:** the block factory is the non-breaking one, so a block leaked on this path
would not show up here. The converter's own release is pinned separately.

**5. The response's leg list. Done.** `ColumnInfoImpl.forAttribute(attribute, meta)` now owns what a
column declares as `original_types`, replacing the same decision written out at both transport
actions. Two kinds of column fill that field for different reasons and the method is where the
difference is stated: an unsupported column lists what disagreed, sorted because nothing reads it by
position; a union column lists its legs, so the order is the contract and is reported rather than
imposed.

**The published vocabulary is `esType`, not the type's own name** — `date`, as field_caps spells it,
rather than `datetime`. The reason is that `original_types` already speaks that vocabulary for an
unsupported column, and a client should not have to know two. Both resolve through
`DataType.fromTypeName`, since `NAME_TO_TYPE` is keyed on the type name with `date` added by hand, so
this was a choice between two working options rather than the only one. `UnionEsFieldTests` asks the
round-trip question of **every** leg type rather than of the one that differs today, so a thirteenth
leg type whose names do not round-trip fails there.

**The seam test earns its keep, and its first version did not.** `EsqlQueryResponseTests` now builds
the column from a `UnionEsField` through `forAttribute` and the block's legs from the same field
through `PlannerUtils.unionLegElementTypes`, then renders. Reversing the block's leg element types is
the mutation that ought to fail it — and against the first leg set, a date, a keyword and a long, it
**passed**, because those element types read `[LONG, BYTES_REF, LONG]`, which is its own reverse. The
leg set is now a date, a double, a keyword and a long, whose element types are not, and both that
mutation and the vocabulary mutation fail it.

**4c. What the loader does not yet reach.** In `EsPhysicalOperationProviders.blockLoaderAndConverter`,
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

## 5. Datasets: answered, and the answer is "inert, by construction"

The question was whether the leg loader sitting in the index path breaks datasets. It cannot, and the
risk ran the other way from how I first framed it.

**Why nothing breaks.** `UnionTypesCleanup` rewrites `FieldAttribute`s whose `EsField` is a
`TypeConflictedField`. A dataset column is a `ReferenceAttribute` built by
`DeclaredSchemaResolver.declaredAttributes`, carrying a plain `DataType` and no `EsField` at all, and
every production site that constructs a conflicted field is inside `IndexResolver` —
`InvalidMappedField.potentiallyUnmapped`, `CompactInvalidMappedField`'s two factories and
`new PotentiallyUnmappedSingleTypeEsField` — and none of them is on a dataset path. (An earlier draft of
this named only two of them, which understated the enumeration without changing where it lands.) So the rule never matches a
dataset column and there is nothing for the loader to be asked for.

**Why nothing is gained either.** Datasets already answer cross-file type disagreement, by a different
and total mechanism: `SchemaReconciliation` widens losslessly where a supertype exists and otherwise
takes `TypeWidening.join`'s top, `KEYWORD`, with a stringification warning. A dataset therefore never
has an unreadable column — it has a *lossy* one. `ExternalCsvUnionByNameNumericWideningIT` pins the
shape: one file's `col` is numeric, another's is text, and the answer is
`("100", "200", "oops", "abc", "def")` — the numbers are strings, so nothing can sum them and nothing
can say which file produced which.

**So datasets are where this type pays next, not a hazard to it.** The union keeps `100` a `long` and
lets `TYPEOF` name the file's type, which is the same proposition as against an index conflict. It is
out of scope here because it is a second producer of legs, not a second consumer: the reconciler would
have to stop collapsing and start declaring legs, and the dataset read path would need a per-file leg
loader answering to the same carried order.

`ExternalCsvUnionTypedPragmaIT` holds the "inert" half, pinned against the pragma-less assertions, with
a control that fails if the pragma stops reaching the queries.

## 6. Two process faults worth keeping

**The docs sync deletes whatever the run did not generate, and one function is permanently exposed.**
`EsqlFunctionPlugin`'s `test.doLast` syncs generated docs into `docs/`, and `Fs.sync` deletes
destination files absent from the source. The `preserve` guard is conditional on the run producing at
most 100 of them, so a filtered run over `expression.function.*` that crosses that count is taken for a
full suite — which is how 55 files for `to_ip`, `inline_cast` and the arithmetic and comparison
operators left the tree under a blanket `git add`, and why the docs CI was red.

An earlier draft of this stopped there and was too narrow. The rule is not about filtering: sync deletes
anything the run did not produce, and a **muted** generator guarantees that for its function on every
run, full or not. `ToIpTests` is muted for `:x-pack:plugin:esql:test` (issue 163299), so a complete unit
run still deletes `to_ip`'s ten files. `unpopulatedKibanaSubdirs` exists for exactly this reason — its
comment names muting — but it preserves whole kibana subdirectories, so it cannot save one function
whose siblings in that subdirectory were generated.

**The consequence for gating: the two suites cannot be run back to back.** `CsvFlattenedKeywordIT` reads
the generated kibana docs to enumerate function parameters, so a `test` run followed by an
`internalClusterTest` run makes the second fail with `TO_IP:0 is unexpected` — a positional index where
a parameter name should be, caused entirely by the preceding task. Restore `docs/` between them.

**A green function test is not evidence its docs render.** `AbstractFunctionTestCase.functionName()`
derives the name from the test class, so `TypeOfTests` asked for `type_of` while the function is
`typeof`; the renderer found nothing and did nothing, and the class passed having checked neither the
docs nor the Kibana definition — nor, it turned out, the declared parameter types, which disagreed with
the test cases. `@FunctionName` is the fix and the only tell is that no generated file exists.

**Correction to §3.10 and to an earlier conclusion of mine.** csv-spec cannot render a union column,
because `CsvTestUtils.Type` is one type per column. It can express any column *derived* from one, such
as `TYPEOF(u)` or `COUNT(*) BY TYPEOF(u)`, both keyword; `pragma:union_typed_fields=true` is accepted by
`CsvSpecReader`. Whether the pragma survives into a `CsvIT` run is unverified — a first attempt failed
with `Unknown column` and was not diagnosed, so the committed `TYPEOF` example uses a single-type column
and the union evidence stays in `UnionTypedFieldIT`.

**Review finding N2 is withdrawn.** `FunctionDefinition.Builder#capabilities` does exist; the finding
grepped for `Builder capabilities(` and the signature is `Builder<T> capabilities(String...)`.

## 7. The two refusals I called defects, re-read against the code

**The `LOOKUP JOIN` refusal is already correctly scoped, and my saying otherwise was wrong.** I told
the user `UNION` in `Join.UNSUPPORTED_TYPES` blocks every column. It does not: that array is consulted
only inside `Join.postAnalysisVerification`'s loop over `config.leftFields()` / `config.rightFields()`,
which are the join keys. A union *payload* column is never looked at there, and the backstop throw in
`QueryList` is on the key-reader path, which analysis has already refused — so the two agree.

Whether a union payload column actually survives a join is a separate question, and it now has an
answer: it does. `UnionTypedFieldIT.testAUnionColumnThatIsNotTheJoinKeyRidesThroughALookupJoin` runs
`FROM union-a,union-b | LOOKUP JOIN union-lookup ON tag | KEEP duration, note` across the two pinned
indices and gets every value back on its own leg, with `original_types` still `[keyword, long]`. The
lookup index deliberately holds only one of the two tags, so three rows join and two miss: a join where
everything matches can keep whole pages, and the path worth exercising is the one that rebuilds them,
which is `Block.filter` plus null-fill in `RightChunkedLeftJoin` over a compound block and its tags.
`AbstractDelegatingCompoundBlock.filter` maps over the sub-blocks, so the union inherits this rather
than implementing it — which is why it wanted a test and not a reading.

**The subquery refusal is not only a sequencing defect of mine — `MergePlan` is a second producer of
union types, and today it is a hard failure.** Two facts:

`MergePlan.output` is a field, not lazy: `outputUnion(subplans)` runs once at construction and is
first-branch-wins per name, so when two branches type a column differently the merge's output simply
takes the first branch's attribute. The disagreement is caught afterwards by
`UnionAll.checkUnionAll`, which fails with `Column [x] has conflicting data types in subqueries: [a]
and [b]`.

That message describes exactly what this type is for, one level up: two *sources* disagreeing about one
column, where the sources are branches rather than indices. So the design-compliant answer is not to
make my refusal more precise — it is for `outputUnion` to declare a union whose legs are the branch
types, and for the merge to tag each branch's values with its leg on the way through. No new block
machinery is needed; the union block already carries tags, and the leg order rule is the same canonical
one. What is new is a second *producer* of legs, and a merge that tags by branch instead of a loader
that tags by a shard's own mapping.

That also dissolves the sequencing problem rather than working around it: a branch whose own column is
a mapping-level union would arrive at `outputUnion` already typed `UNION`, and a merge that can build a
union from disagreeing branches can carry that through instead of flattening it to `UNSUPPORTED`.

Sized honestly, this is the largest remaining increment and it touches analysis, verification and the
merge operator. It is also the one that turns a refusal into the feature.

## 8. `TYPEOF` over a union never resolved, and I reported that it did

`TypeOf.resolveType` admitted `UNION` and the leg types. It did not admit `UNSUPPORTED`, and that is the
type a conflicted field actually has while the Resolution batch runs: the conflict is still an
`InvalidMappedField`, whose own type is `UNSUPPORTED`, and the union is not built until
`UnionTypesCleanup` at the end of analysis. So `EVAL t = TYPEOF(duration)` left the `EVAL` unresolved,
and the user saw `Unknown column [duration]` and `Unknown column [t]` for a field that plainly exists —
the enclosing `KEEP` could not bind against an unresolved child, which is what masked the real cause.

`TYPED` was never affected because it constrains nothing, and its comment says why in the same words.
Two functions over the same column, one of which had the rule written down.

**The fix is to admit the pre-union form**, because the replacement that follows is the designed
mechanism rather than a workaround: `UnionTypesCleanup` ends with a `transformExpressionsOnly` over
`FieldAttribute`, so the child of this node becomes the `UnionEsField` attribute after resolution and
`toEvaluator` reads its legs. Nothing unreadable gets through by being let past: a conflict no union can
carry is erased to an `UnsupportedAttribute` by that same rule, and the verifier refuses it there.

**On the reporting.** I told the user `UnionTypedFieldIT` was three tests green and that
`STATS COUNT(*) BY TYPEOF(duration)` worked end to end on a two-node cluster. `TypeOf.resolveType` is
byte-identical between that claim and the commit that found this, so the claim was false when made.
What the suite did show was `BUILD SUCCESSFUL` for a run whose per-class results I did not open — the
rule that a green gate is evidence only about what it checks, applied to me rather than to the code.
The two tests that now hold it are unit-level and named for the message that recommends the query:
`AnalyzerTests.testTypeOfOverAUnionResolvesToAKeyword` and `testCountByTypeOfOverAUnionResolves`.
The grouping refusal's own test only ever checked the wording of its suggestion, never that the
suggested query ran.

## 9. RENAME, and the lateness defect's real shape

Probed on the two pinned indices of `UnionTypedFieldIT`, one query at a time. Everything works except
one shape:

| query over a union column | result |
|---|---|
| `KEEP duration`, `… \| LIMIT 3` | renders per leg |
| `EVAL t = TYPEOF(duration)` | `long`/`keyword` per value |
| `EVAL c = 1 \| KEEP duration, c` | renders per leg |
| `WHERE duration IS NOT NULL` | all five rows |
| `STATS n = COUNT(*)` | 5 |
| `STATS n = COUNT(*) BY t = TYPEOF(duration)` | `[[2, keyword], [3, long]]` |
| `EVAL s = TYPED(duration, "long")` | longs selected, keywords null |
| `EVAL d = duration::long` | longs converted, keywords null + warning |
| **`RENAME duration AS d`** | **was two internal failures; works as of §11** |

**What `RENAME` did before the refusal, measured twice.** `RENAME` resolves its new name while the
conflict is still an `InvalidMappedField`, so the attribute a later `KEEP` binds to is minted with
`UNSUPPORTED` and keeps it. The plan then says two things about one column, and the optimizer's own
consistency check reports it as
`Output has changed from [d{r}#16[unsupported]] to [d{r}#16[union]]` — a dump of internal ids, not an
answer.

Retyping those references by id inside `UnionTypesCleanup` does fix that, and I wrote it and threw it
away, because it only moves the failure: past the consistency check the renderer throws
`rendering a union needs its leg types`. The legs live on the `UnionEsField`, so they travel while the
column *is* the field; an alias makes a `ReferenceAttribute`, which has no field, so the response says
`union` with nothing saying what a tag indexes.

**So this was a deviation from "a union is always renderable", and the compliant fix is that the legs
must travel with any attribute of this type rather than only with a field.** It was refused in the
interim, with a message naming both ways through (`TYPED` and `::`), both tested to work, because a
refusal recommending a broken escape is worse than no refusal. §11 is that fix; the refusal is gone.

**The general statement, now that there are three instances.** A type decided at the *end* of analysis
cannot be seen by anything minted *during* it. `MergePlan.output` computes eagerly in its constructor
(§7), a `RENAME` alias mints its reference during resolution (here), and `TYPEOF` would read legs off an
attribute that has none. Fixing them one at a time is whack-a-mole; the two candidate roots are
building the union earlier — which fights `ResolveUnionTypes`, since `TYPED` and `::type` need the
conflicted field to still be a conflict — or making the leg list part of what any attribute of type
`UNION` carries. The second is the one that also discharges renderability, and it is the one taken,
in §11.

## 10. Two defects found by measuring instead of reasoning

Both came from asking what actually happens rather than reading what should. Both were reachable with
ordinary queries, and one was the worst shape a defect can take.

**`SORT <other column>` carrying a union: a hang, not an error.**
`FROM union-a,union-b | SORT tag | KEEP duration` hung until the test's timeout, leaked four blocks and
left the request breaker charged. The cause is mine and it had been written down as a *question* in the
first adversarial review and never answered: `TopNExec.encoder` answers `DEFAULT_UNSORTABLE` for
`UNION`, which is a claim that a union rides through a sort on another column, while
`ResultBuilder.resultBuilderFor` has no union arm and throws — and that throw strands the driver rather
than failing the query.

Refused in `TopN.postOptimizationVerification`. The placement is the point: the analyzer sees every
column the sort's child produces, including ones a later `KEEP` drops, so refusing there would refuse
`SORT tag | KEEP tag`; by post-optimization the projections are pruned and the output is what the sort
will really carry. `UnionTypedFieldIT.testAUnionCannotRideThroughASort` pins both halves — the refusal,
and that sorting with the union *out* of the output still works.

**`MV_EXPAND` on a union: five nulls.**
`MV_EXPAND` re-mints its column as a reference, which at the time carried no field and so no legs, and
the column came back entirely null. An answer with nothing to say it is wrong is worse than an error.
Refused in `MvExpand.postAnalysisVerification`, and it stays refused after §11 for two reasons that
survive the legs travelling: `UnionArrayBlock#expand` throws for a block that may hold several values
per position, which is the only input the command exists for, and `Analyzer.resolveMvExpand` mints its
`expanded` attribute with a fresh `NameId` during resolution, so it is not an alias and the retype pass
does not reach it. The refusal's javadoc now says that instead of the superseded reason.

**What was checked and was fine.** The rename refusal only inspects `Project`, so an `EVAL` alias looked
like a hole. It is not: `EVAL x = duration` is already refused with
`EVAL does not support type [union] as the return data type of expression [duration]`, and `COALESCE`
through an `EVAL` likewise — which also means the `Coalesce` backstop comment's "nothing upstream yet
stops one arriving" is true only for the `WHERE` path it names, not for `EVAL`.

**A trap carried into the next increment, and discharged there.** Lifting the rename refusal without
also guarding `TYPED` ships a silent null. `ResolveUnionTypes` rewrites `TYPED` only over a `FieldAttribute` whose field is a
`TypeConflictedField`; after a rename the child is a `ReferenceAttribute` typed `UNION`,
`Typed.resolveType` constrains nothing and admits it, and `toEvaluator` computes
`sameType(UNION, LONG) == false` and returns a constant-null column. So
`RENAME duration AS d | EVAL s = TYPED(d, "long")` would answer five nulls — and `TYPED` is exactly what
the refusal message recommends. The guard belongs in the same increment as the lift, not after it.

## 11. The legs on the attribute, and what that bought

Two commits, in the order that keeps each one reversible.

**Increment 1, `ee22005a65a3` — a union attribute carries its legs, or does not exist.** Behaviour
neutral on purpose: nothing minted a legged reference yet. `UnionTypedExpression` is the interface that
makes the list askable of an expression rather than of a field; `FieldAttribute` derives it from its
`UnionEsField`; `ReferenceAttribute` gained a `legTypes` payload with the invariant
`(dataType == UNION) == (legTypes != null)` enforced in the constructor and carried through `clone`, so
`withName` and `withId` cannot drop it and `withDataType` cannot forge it. `ColumnInfoImpl` and
`TypeOf.legNames` then ask the expression instead of testing for a field. The two
`EsPhysicalOperationProviders` field-extraction sites keep the field test deliberately: a per-shard
loader is something only a field has.

**Increment 2 — the producer, the lift and the one guard that has to come with it.** `Alias.toAttribute`
passes `legTypesOf(child)` across the aliasing boundary, exactly as it already passed the values
analyzer and the data type. `UnionTypesCleanup.refreshUnionReferences` then retypes every reference that
names such an alias, keyed on `NameId`, one round per link of a rename chain. With that in place
`Verifier.checkUnionTypedAttributeRenaming` is deleted.

**`RENAME` now works end to end** across the two pinned data nodes of `UnionTypedFieldIT`: the column
renders per leg, declares `["keyword", "long"]` in `original_types`, survives a chain of renames, and
`TYPEOF` over the renamed name still names each value's own leg, because it reads the tag off the block
and the names off the carried list.

**`TYPED` is refused on a renamed union, in the same commit.** Selection happens by rewriting the
*field* attribute into one conversion per mapped type, so a renamed copy is never rewritten and the
function would compare `union` against the requested type, find them different, and answer null for
every row. The check is `Typed.postAnalysisVerification` rather than `resolveType`, because the type is
not a union yet while resolution runs — a conflict is an `InvalidMappedField` typed `UNSUPPORTED`
throughout, which is the shape the function exists to be applied to. The message is anchored on the
`TYPED` call rather than on the field, whose source for a rename is the `RENAME` clause the user wrote
correctly, and it names the order that does work: select the leg off the column, then rename the result.

**What each test is worth, measured by mutation rather than asserted.** Four mutants, each run against
the five new `AnalyzerTests` cases:

| mutant | red |
|---|---|
| `refreshUnionReferences` not called | the two rename cases, `TYPEOF`, and the `TYPED` refusal |
| the retype loop cut to one round | the chained rename only |
| `Typed.postAnalysisVerification` emptied | the `TYPED` refusal only |
| `Alias.toAttribute` passes `null` legs | the same four as the first |

The one-round mutant going red on exactly the chained rename is what says the loop is load-bearing
rather than defensive.

**And the state this removes was worse than an error.** Running the eleven `UnionTypedFieldIT` cases
against the first mutant: the three rename cases fail with the plan-consistency dump as expected, the
`TYPED` refusal stops firing because its trigger never appears — and six *unrelated* cases then fail
with `Request breaker not reset to 0 on node: node_s0 … was 1836L`. The planning failure leaks the
request breaker and poisons the rest of the suite. The shipped code's run is 11/11 with no breaker
failure, so this is a fact about the state being removed; whether the same leak attends any
`PostOptimizationPhasePlanVerifier` failure on main is not isolated here and is not claimed.

**One asymmetry left standing, deliberately.** `RENAME u AS d` works; `EVAL d = u` is still refused,
with `EVAL does not support type [union] as the return data type of expression [u]`. Both are copies of
a column, so the distinction is not a design choice — the refusal belongs to the `EVAL` command, whose
`postAnalysisVerification` asks `DataType.isRepresentableInExpression`, and that excludes `UNION`.
Widening it so an `EVAL` could copy a union would also admit a union argument to every scalar function
that types itself off a child, which is the blast radius the `Coalesce` finding is about. The gate
stays; the asymmetry is recorded here rather than left for a reader to discover.

**Per-function admission is a separate axis and is not uniform**, which an earlier draft of this
section got wrong by calling `isRepresentableInExpression` "the single gate keeping every scalar
function from accepting a union argument". It is not a gate on functions at all. `TypeOf` admits a
union deliberately — it is the function that exists for one. `Coalesce` and `Case` admit one by
omission, taking their type from a child with no representability test, which is §12's second finding.

## 12. What Fable's review of §11 found, and it was not cosmetic

Two blockers, both server errors, both reachable with an ordinary query under the pragma, and **neither
visible to any gate I had run** — `AnalyzerTests` at 810 green and `internalClusterTest` at 11/11
covered neither, because every pragma-on query in the suite resolves and none of them wraps a union in
a function. The green gate was evidence only about what it checked, again.

**B1 — an unknown column answered with a 500.** `refreshUnionReferences` asked every `Alias` in the
plan for `child().dataType()`. The analyzer deliberately leaves an unresolved child in place for the
verifier to report, and `UnresolvedAttribute.dataType()` throws `UnresolvedException`, which is a
`QlServerException`. So `FROM union_index* | EVAL x = nope` and `FROM union_index* | RENAME nope AS x`
both came back as `Invalid call to dataType on an unresolved object ?nope` instead of
`Unknown column [nope]`. The in-repo precedent for a late retype pass, `collectAliasesNeedingTypeUpdate`
in the same file, guards exactly this and says why; I had not read it.

**B2 — a function that types itself as a union hit my own invariant.** `Coalesce.resolveType` takes its
type from its first child with no representability test, so after the transform swapped the conflict for
a `UnionEsField` the `Coalesce` was typed `UNION` with no legs. `Alias.toAttribute()` then refused to
build the reference, and `FROM union_index* | EVAL x = COALESCE(id, id)` answered
`[x] is a union and must carry its leg types` — an internal invariant in place of the user-facing
refusal that already existed. Same for `CASE`. Measured at `ee22005a65a3`: both queries give the correct
`EVAL does not support type [union]`, so this was introduced by §11 and not pre-existing.

**One fix for both: ask the child for its legs, never for its type.**
`UnionTypedExpression.legTypesOf(alias.child()) != null` is true exactly when the child is a union
attribute. It cannot touch `dataType()` on anything unresolved, and it cannot reach an expression that
is a union without being able to describe itself. Both shapes then fall through to the refusal that
owns them.

**And one message regression, found by the same review.** An attribute carries the source it was minted
with. `handleSpecialFields` mints a reference with `withLocation(u.source())` — the usage site — and the
retype pass was replacing it with `alias.toAttribute()`, whose source is the `RENAME` clause. So
`RENAME id AS d | STATS c = COUNT(*) BY d` reported the grouping refusal at the rename rather than at
the grouping. Each replacement now keeps the source of the reference it replaces, pinned by
`testARefusalOnARenamedUnionPointsAtTheUseNotTheRename` on the exact column. Newly reachable, since the
rename was refused before.

**Checked and standing.** The review traced and cleared the wire path (the legs are written immediately
after the type, inside the attribute-cache body, and `innerEquals` includes them so the cache cannot
conflate a legged and a legless attribute), the `TYPED` guard's reach (dispatched via
`forEachExpression`, so a nested, `WHERE`, `STATS` or `SORT` use is covered, and `::type` on a renamed
union is refused by `isTypeOrUnionType` requiring a `FieldAttribute`), termination (each round grows the
map by an id or returns; a chain of N renames is O(N²), bounded), `FILLNULL`, `SORT`, `STATS BY`, the
`LOOKUP JOIN` key, and `MV_EXPAND`. `FORK` over a renamed union is caught by
`MergePlan.checkNoUnionAcrossBranches` — the merge mints its output with a fresh `NameId` during
resolution, so the retype pass cannot reach it, and that check is the one thing between the lifted
refusal and a column of silent nulls. It now has a test of its own.

**Recorded, not fixed, and measured rather than taken on the review's word.**
`FROM union_index* | RENAME id AS d | ENRICH languages ON d` answers
`line 1:58: Unsupported type [unsupported] for enrich matching field [d]; only [keyword, text, ip,
long, integer, float, double, datetime] allowed for type [match]`. `resolveEnrich`'s `deferToUnionTypes`
branch recognises only a `FieldAttribute`, so a retyped reference misses it and gets the misleading
wording that branch exists to avoid — the attribute is still `UNSUPPORTED` at that point because
`resolveEnrich` runs during resolution. The fix is to widen that branch; it is not in this increment.
The message is wrong, the refusal is right, and no test asserts it, because a test pinning a message
known to be wrong is a test defending the defect. The position is correct, which is the relocation fix
reaching this path too.
