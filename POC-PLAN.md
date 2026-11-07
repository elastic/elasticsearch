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

**C. Stop erasing.** Not started, and now the next increment. **E. Operations.** Not started.
**F. Grouping.** Not started. **G. Datasets.** Not started.

D was taken before C, against the order in §3. C alone would produce union attributes that nothing
could render, so the tree would have been red between the two; D is self-contained, since the renderer
can be built and tested against hand-built union blocks while nothing in the engine produces one.

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
