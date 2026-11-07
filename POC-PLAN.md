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

**E. Operations.** Not started. **F. Grouping.** Not started. **G. Datasets.** Not started.

---

## 0a. The minimal POC — the product shape

**One sentence: you can read, inspect, filter and group a column whose indices disagree about its
type; to rank or total it, you commit to a type.**

That is the line. Everything a user needs to make a messy field *usable* is in; everything that needs
an opinion about how two types relate is refused, with the refusal naming the escape.

### What works

| | Query | Needs |
|---|---|---|
| see it | `FROM logs-* \| KEEP duration` | C2 loader, C3 leg list |
| inspect it | `STATS rows = COUNT(*) BY type = TYPEOF(duration)` | `TYPEOF` (new) |
| count it | `STATS COUNT(duration)`, `WHERE duration IS NOT NULL` | tag only |
| filter it | `WHERE duration == 200`, `> 200`, `IN (200, 250)` | per-leg comparison fan-out |
| group it | `STATS COUNT(*) BY duration` | packed-values hash, tag as the type half |
| escape it | `TYPED(duration, "long")`, `duration::long` | union wiring + tests; cast exists |

### What is refused, and what the message says instead

| Refused | Escape named in the message |
|---|---|
| `SORT duration` | `duration::long`, or `TYPED(duration, "long")` |
| `MIN` / `MAX` / `SUM` / `AVG` | `MAX(duration::long)` |
| `duration == 200::any` | the untyped constant is not in this POC |
| a multi-valued union column | blocked on `AbstractDelegatingCompoundBlock` |
| a union as a lookup-join key, late materialization, across `FORK` branches | — |

### Why the line falls there

**Comparison is in because I1 requires it.** A `long` column that acquires a `keyword` leg must keep
`WHERE duration > 200` working — refusing it breaks a query that worked, which is the one thing the
union type exists to prevent. On the numeric legs it is exactly today's semantics; the keyword leg is
null with a warning. Ordering *comparison* and equality are the same fan-out with a different
operator, so they arrive together or not at all.

**`SORT` is out even where it would work.** On `long｜double` every pair is comparable and a sort is
well defined, but it needs TopN to encode a union's sort keys to bytes, and it buys little: a user who
wants ranked output casts. Refusing it everywhere first is the reversible direction. This is the one
accepted break of I1, and the design has always listed it.

**Extrema and sums are out because they are the most expensive increment and the least often
answerable.** `AggregateMapper.toDataType` throws for `UNION`, the aggregators are strongly typed with
generated classes chosen at planning, and a union aggregate must fan out per leg, multiplying
intermediate state across the reduce boundary. Meanwhile on a genuinely mixed field the principled
answer is usually null anyway, and the escape is one cast. `COUNT` needs none of it — it reads the tag.

**Grouping is in because it is cheap and it is the point.** `STATS COUNT(*) BY duration` on
`(leg, value)` is how a user sees what is actually in the field, and the tag sub-block is the type half
of the key read directly, through the packed-values hash rather than a dedicated one.

### Increments, smallest first after the prerequisite

1. **C2, the data-node loader.** The hard prerequisite — nothing else can be demonstrated without it.
   `EsPhysicalOperationProviders.fieldLoader` must load each shard's values with that shard's own
   native loader and append into leg `legOf(shardMappedType)`; the method already reads each shard's
   own mapping, which is exactly what decides the leg. Fold in `RandomBlock` support for union blocks,
   or block coverage stays hand-written only.
2. **C3, the response's leg list.** Ungate the `original_types` producer in
   `TransportEsqlQueryAction` and `TransportEsqlStreamQueryAction`, which is gated on
   `instanceof UnsupportedAttribute` and so never fires for a union attribute.
3. **`TYPEOF`.** Tag only, result always `keyword`. It does not exist, and
   `UNION-TYPES-ARCHITECTURE.md` leans on it in nine places.
4. **`COUNT`, `IS NULL`, `IS NOT NULL`.** Tag only.
5. **The refusals and their messages.** Cheap, and they define the product's edges.
6. **`TYPED`'s union wiring and its tests.** There is no `TypedTests.java` and no csv-spec case;
   `ResolveUnionTypes` matches `TypeConflictedField`, not `UnionEsField`, so `TYPED` is currently
   disconnected from the union. Load-bearing, because every refusal points at it.
7. **Comparison fan-out.** The main engine work: `EsqlBinaryComparison` has no `UNION` arm, and
   neither `commonType` nor `DataTypeConverter` knows the type. The layout is what keeps this from
   being new per-type code — each leg is a conventional typed block, so a fan-out substitutes
   `legBlock(i)` into the page and reuses the existing per-type evaluator, combining by tag.
8. **Grouping.** Increment F, as described above.

---

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

**And filtering under that rule is nearly free**, which earlier cost estimates in this plan got wrong.
Where the legs share a common type, `WHERE duration > 200` on `long｜double` is
`WHERE duration::double > 200` — the existing `ResolveUnionTypes` conversion, already implemented and
already pushdown-capable, so none of §5's sixteen `UnionTypeEsField` guards are even reached. Where
they do not, it is a verifier rule over the leg types. No `EsqlBinaryComparison` union arm, no per-leg
evaluator fan-out, no new per-type code.

Using `commonType` for that rule is safe here despite its gap on `datetime`×`date_nanos`, because
`DateMillisToNanosInEsRelation` resolves an all-dates conflict with `ToDateNanos` before it can become
a union, so that leg pair is unreachable.

---

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
the diff.

### C. Stop erasing the types

`UnionTypesCleanup.cleanTypeConflicts` rewrites a union-typed attribute away today. The data node
should load each leg natively and tag it, and the coordinator should keep the union attribute.

Sites: `UnionTypesCleanup`; `EsPhysicalOperationProviders.fieldLoader`, which takes the per-type
conversion expression out of the `UnionTypeEsField` and wraps it in a `TypeConverter`; and the
`original_types` producer in `TransportEsqlQueryAction` and `TransportEsqlStreamQueryAction`, gated on
`instanceof UnsupportedAttribute` so it will not fire for a union attribute. The leg order fixed here
is the one the block and `originalTypes` share, and it has to be deterministic.

### D. Rendering, and representability

Done; see §0. What it leaves: the Arrow sparse-union write-out, where the tag sub-block is the types
buffer so the mapping is a write-out rather than a translation, and a `CsvTestUtils.Type` member for
csv-spec assertions. The leg types are paired with the block's legs positionally and nothing checks
that they agree, which is a contract increment C has to establish and keep.

### E. Operations

Projection, `COUNT`, `IS NULL`, `TYPEOF` and the per-type selector, everything else refused.
`AggregateMapper.toDataType` throws for `UNION` today, which is where per-type aggregate intermediate
state will announce itself. Scope it so aggregate-metric-double and the all-dates promotion keep
working.

### F. Grouping on the pair of type and value

Through the packed-values hash rather than a dedicated one. The tag sub-block is the type half of the
key, read directly.

### G. Datasets

Its own mechanism on `ColumnMapping`, identity-or-absent per file. `ColumnMapping.mapFilters` and
`inferFileType` are the sites. The `union_by_name` keyword fallback is the behaviour to replace with a
surfaced conflict, numeric promotions left intact.

---

## 4. Known limits this plan accepts

- Multivalued union columns, until `AbstractDelegatingCompoundBlock` supports multivalued `filter`,
  `slice`, `keepMask` and `expand`.
- Late materialization of a union column, refused in `FetchGather`.
- A union as a lookup-join key, refused in `QueryList.createBlockValueReaderForType`.
- No randomized suite generates a union block, since `RandomBlock` cannot build one.
- `FORK` and `UNION ALL` branches go through `ResolveUnionTypesInUnionAll`, untraced here.
- The per-value coercion path for Parquet and ORC, untraced.
