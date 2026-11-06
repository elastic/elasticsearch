# Union types in ES|QL

## The situation

A field in an ES|QL query result doesn't always have one type. `duration` is a long in January's
indices and a double in June's because someone edited a template. `status` is a long where one team
writes and a keyword where another does. A column is an int32 in half a Parquet corpus and a string in
the half written after a producer changed.

Most of this won't get fixed. Nobody reindexes a year of logs to settle a type, and nobody owns every
producer writing into a bucket.

Each index and each file has exactly one type for the field. No mapping is ambiguous. The
multi-typedness is in the result, and it appears only when a query spans sources that disagree. Three
things follow. There is nothing to store or declare, because the type doesn't exist until someone asks
a question crossing the disagreement. The same field is multi-typed in one query and single-typed in
another. And the response format has to express it, because it's a property of responses.

## What happens today

Across indices the field comes back typed `unsupported`. `KEEP` works and returns nulls. Anything else
fails with a message naming the conflicting types. A cast gets you through and works well: it builds a
conversion table keyed by each index's own mapping, ships it to the data nodes, and each shard reads
its own type and converts during extraction. It's all-or-nothing. One index mapping the field to
something with no conversion rule to your target refuses the whole query, including the indices that
would have worked.

Across dataset files we resolve the disagreement before the query runs. Compatible numerics are
promoted: integer to long, integer or long to double, datetime to date_nanos. Everything else folds to
keyword with a warning. The query succeeds on flattened values. Ordering becomes lexical and arithmetic
is gone. The decision is made once for the whole dataset, so a query reading only the long-typed files
still reads them as keyword. Statistics pruning is lost for every promotion except integer to long, so the
promotions cost us the row-group skip too.

Neither answer was chosen on its merits. The index answer says there's no type for this. The dataset
answer says there's no usable type for this, so widen and warn.

We already do per-type handling where the pain was sharp enough. A conflict whose types are all dates
is rewritten automatically. An aggregate over an aggregate_metric_double mixed with numerics gets
per-type converters, covering every aggregate that declares native support for it. The machinery exists
and runs in production. Users can't reach it.

Three things are missing. You can't see a conflicted field: the values are on disk, typed correctly at
each source, and you get nulls. You can't ask what's in it: we know the set of types and which sources
carry them before reading anything, and it reaches the user only inside an error message. You can't
take it apart: the cast applies to every type at once or not at all.

## Three principles

Each answers a different question, and together they derive the design: where the type comes from, what
an operation means, and what happens to the results. The scenario used throughout is the one in the
next section, where `duration` is long in `logs-01`, double in `logs-02` and keyword in `logs-03`.

### The type belongs to the result, not to the data

Nothing is stored, declared or configured, because there's nothing to attach it to. We compute the type
per query from the sources that query touched. That decides no mapping change, nothing persisted and no
new field type to configure, and it decides that the response carries it, so renderability isn't a
requirement we chose, it's a consequence.

### Composability: an operation means, per type, what it means alone

Take the operator. For each type in the column, ask what it does to a column of only that type. That's
the answer, and we invent nothing because the column happens to be multi-typed.

So `duration + 1` is a type error. On a long-only column addition works; on a keyword-only column it
doesn't, and the error is what the keyword constituent gives you. The tempting alternative, adding the
longs and nulling the keywords, is behaviour neither constituent has.

`TYPED(duration, long) > 260` is fine, and returns the one row that qualifies:

```
duration
--------
300
```

On a long-only column that's a range predicate. Everywhere else `TYPED` gives null, `null > 260` is
null, and `WHERE` drops nulls. Null rather than false matters: comparing against null yields null on any
single-typed column, so nothing has been invented, whereas false would have been.

This decides every refusal of lenient behaviour, and it's why a query must not change meaning when an
index joins or leaves the pattern.

### Closure: a multi-typed result is a legal result

Composability hands back one result per type. Closure decides two things about them: whether they fit in
the answer, and whether a per-type failure can be reported there.

Grouping fits. `STATS n = COUNT(*) BY duration` gives one group per type-and-value pair, and the answer
has a row per group:

```
n | duration
--+-----------
1 | 100.0
1 | "200"
1 | 200
```

Long 200 and keyword "200" are two groups, and the tuple key isn't a design choice, it's what the shape
permits.

Sum grouped by type also fits, and this is the interesting case. `STATS SUM(duration) BY t =
TYPEOF(duration)`:

```
SUM(duration) | t
--------------+---------
750           | long
100.0         | double
null          | keyword     warning: sum has no form for [keyword]
```

Composability produced three per-type results and one of them is a type error. The shape has a row per
type, so that failure is attributable: the user asked for the type as a dimension, and the null says
which type produced nothing. So we return the partial sums rather than failing the query.

This does not generalise, and an earlier draft of this document claimed it did. "Leniency wherever the
answer has a row per type" is false: `EVAL y = duration + 1 | STATS SUM(y) BY t = TYPEOF(duration)` has a
row per type, so that rule would permit the addition the composability section refuses, and
`STATS SUM(duration) BY TYPEOF(host)` has a row per type of a different column entirely. Leniency has to
be decided per operation, not per query shape.

So partial sums are a **third rule with a syntactic guard**, not a consequence of the first two: when an
aggregate's grouping key is `TYPEOF` of the same attribute it is aggregating, the aggregate resolves per
type and returns per-type results, with a null and a warning for each type it has no form for. The guard
is checked at planning time against the query text, not inferred from the shape of the output. Outside
that guard, composability applies unchanged, which is why `duration + 1` stays an error.

Ungrouped sum has nowhere to put them. `STATS SUM(duration)` is one row holding one value, while
composition produced three sums of three types, so closure fails and nothing comes back, even if every
type were summable. The refusal names both collapses: group by type, or cast and merge deliberately.

`SORT` and ungrouped `MIN` and `MAX` fail the same way. An order across types is an order nobody
defined, and one row can't hold a result per type.

One constraint on every refusal: it has to be scoped to what's refused today, or we take away the
aggregate_metric_double and all-dates behaviour that works now.

## What reaches into a union

Two functions, which is the minimum the principles allow.

`TYPEOF(x)` gives each row's type as a name. Its own type is keyword, always. A null value has no type,
so it returns null for that row, which means it is not a constant even on an ordinary column: it is the
column's type where the value is present and null where it isn't. Treating it as a constant on ordinary
columns and null-aware on union ones would be exactly the kind of union-only behaviour composability
forbids. The names it
returns have to be exactly the names `TYPED` accepts and `original_types` reports, which needs settling
once, because ES|QL's internal type names and its response type names already differ in places,
`datetime` against `date` among them.

`TYPED(x, long)` gives a column of type long holding the values already of that type, null elsewhere.
No conversion, no inspection of values. The result is an ordinary single-typed column, so everything
downstream is normal ES|QL.

Narrowing is composition over what those produced, using functions we already have:

```
FROM logs-*
| EVAL ms = COALESCE(TYPED(flow, long), TO_LONG(TYPED(flow, keyword)))
| STATS AVG(ms)
```

A type nobody named contributes null instead of refusing the query. A surprising conversion can't
happen silently: a date-mapped source contributes nothing unless you name it, which closes the case
where dates converted to longs become epoch numbers and get averaged in with durations.

## Syntax, by example

One scenario throughout. `FROM logs-*` spans three indices that disagree about `duration`. `logs-01`
maps it long and holds 200, 250, 300. `logs-02` maps it double and holds 100.0. `logs-03` maps it
keyword and holds "200", "150", "unknown". Seven rows.

It renders, with values instead of nulls, each at its own type.

```
FROM logs-* | KEEP duration | LIMIT 10
```
```
duration            type: union, original_types: [double, keyword, long]
--------
200
250
300
100.0
"200"
"150"
"unknown"
```

In JSON each value carries its own type: 200 as a number, "200" as a string. Text and CSV have no
per-value type to carry, so there the two look the same. That's a property of those formats.

Ask what's in it. This is the triage query.

```
FROM logs-* | STATS rows = COUNT(*) BY type = TYPEOF(duration)
```
```
rows | type
-----+---------
   3 | long
   1 | double
   3 | keyword
```

Take it apart. `TYPED` hands back an ordinary single-typed column.

```
FROM logs-* | EVAL d = TYPED(duration, long) | KEEP duration, d
```
```
duration  | d        d has type: long
----------+------
200       | 200
250       | 250
300       | 300
100.0     | null
"200"     | null
"150"     | null
"unknown" | null
```

Narrow, then aggregate.

```
FROM logs-*
| EVAL ms = COALESCE(TYPED(duration, double),
                     TO_DOUBLE(TYPED(duration, long)),
                     TO_DOUBLE(TYPED(duration, keyword)))
| STATS AVG(ms)
```
```
AVG(ms)
-------
200.0        warning: [unknown] could not be parsed, treated as null
```

Six of seven rows contribute. An ip-mapped index, which breaks a plain cast today, contributes nulls
here because no arm names it.

Filter on one type. This pushes, provided `TYPED` produces an attribute with the real field name and
every shard on the node maps `duration` as long. See What's unsettled.

```
FROM logs-* | WHERE TYPED(duration, long) > 260 | KEEP duration
```
```
duration
--------
300
```

Filter on the type itself. A source whose type doesn't match folds to false and drops at planning. A
matching source folds to `duration IS NOT NULL`, since a null row has no type, so it still reads.

```
FROM logs-* | WHERE TYPEOF(duration) == "keyword" | KEEP duration
```
```
duration     logs-01 and logs-02 dropped at planning
--------
"200"
"150"
"unknown"
```

Group by the column. The key is the tuple of type and value, so long 200 and keyword "200" are two
groups.

```
FROM logs-* | STATS n = COUNT(*) BY duration, t = TYPEOF(duration) | SORT t, n
```
```
n | duration  | t
--+-----------+---------
1 | 100.0     | double
1 | "150"     | keyword
1 | "200"     | keyword
1 | "unknown" | keyword
1 | 200       | long
1 | 250       | long
1 | 300       | long
```

Aggregate per type. Each group holds one type, so nothing needs merging.

```
FROM logs-* | STATS n = COUNT(*), lo = MIN(duration), hi = MAX(duration) BY t = TYPEOF(duration)
```
```
n | lo    | hi        | t          lo, hi have type: union
--+-------+-----------+---------
3 | 200   | 300       | long
1 | 100.0 | 100.0     | double
3 | "150" | "unknown" | keyword
```

Per-type sums come back partial, with the group that has no sum marked. If you want one number per
type on a common scale instead, narrow first and keep the type as the grouping key.

```
FROM logs-*
| EVAL ms = COALESCE(TYPED(duration, double),
                     TO_DOUBLE(TYPED(duration, long)),
                     TO_DOUBLE(TYPED(duration, keyword)))
| STATS SUM(ms) BY t = TYPEOF(duration)
```
```
SUM(ms) | t              SUM(ms) is an ordinary double column
--------+---------
750.0   | long
100.0   | double
350.0   | keyword        "200" + "150"; "unknown" was null
```

Drop `logs-03` and `STATS SUM(duration) BY t = TYPEOF(duration)` works directly, returning 750 as a
long and 100.0 as a double in one multi-typed column.

And what's refused.

```
FROM logs-* | STATS SUM(duration)
  cannot SUM a union-typed field [duration]: per-type results cannot be combined without
  choosing a type. Group by TYPEOF(duration) for per-type sums, or narrow first, e.g.
  SUM(TO_DOUBLE(TYPED(duration, long))).
  [duration] is mapped as [3] incompatible types: [double] in [logs-02], [keyword] in
  [logs-03], [long] in [logs-01]

FROM logs-* | WHERE duration > 100      -- [>] has no form for keyword against an integer literal
FROM logs-* | EVAL x = duration + 1     -- [+] is not defined for [keyword]
FROM logs-* | SORT duration             -- no order across types
```

## How a union type is built today

Field caps reports, per index, the mapping type of each field. `IndexResolver` turns a disagreement
into a `TypeConflictedField` carrying a map from type name to the indices holding it, and gives the
field the data type `UNSUPPORTED`. If the query casts the field, `ResolveUnionTypes` finds the
conversion sitting on that field attribute and builds one conversion expression per mapped type: the
same conversion function with its child replaced by a field attribute pinned to that single type. It
collects them into a `CompactMultiTypeEsField` keyed by type and hangs it on a new synthetic attribute
named `$$<field>$converted_to$<type>`. It does this only when every mapped type has a conversion rule;
otherwise the cast stays unresolved and `UnionTypesCleanup` turns the field into an
`UnsupportedAttribute`, which survives a projection and nothing else.

What crosses to the data node is that per-type conversion table. There, `blockLoaderAndConverter` reads
the shard's own mapping type, looks it up in the table, and applies that entry during field extraction
through `loadAndConvert`. The block coming out is homogeneous, of the single target type, and the
knowledge of which type the shard actually held is consumed and discarded at that point. Two details
matter later. The table must converge on one target type, enforced when it is resolved. And the
attribute's name is what pushdown keys on: `isQueryable` has a branch for union-typed fields that looks
up `attr.name()` in the node's statistics, which is why an auto-promoted field whose attribute keeps the
real field name pushes, and a cast-generated attribute named `$$...$converted_to$...` does not.

## What we will change

**We will add a data type for the union**, so a conflicted column survives analysis and reaches the
response instead of being typed `UNSUPPORTED`.

**We will change `UnionTypesCleanup`** so that a surviving conflict is no longer replaced by an
`UnsupportedAttribute`. It will ship the field with identity entries per type, and we will lift the
requirement that every entry converge on one target type.

**We will add two functions**, `TYPEOF` and the per-type selector, resolved by the same rule that
resolves casts. We will give their attributes the real field name rather than a synthetic one, because
that is what decides whether a predicate on them is pushed.

**We will add a block that carries one typed sub-block per type**, positionally aligned, so the data
node can emit values without converting them. A row's value sits in the sub-block for its type, which we
call a leg, and the other legs are null at that position.

Two things about that block came out differently from the first draft of this section, and both were
decided by the code rather than by preference.

The discriminator is a sub-block of its own, holding the leg index per value, and not something inferred
from which leg happens to be non-null. `AbstractDelegatingCompoundBlock` is the base class the existing
compound blocks use, and it derives the whole of a block's nullity from the first sub-block, in methods
that are final: a value is null exactly where that sub-block is null. Legs cannot satisfy that, because
each leg is null wherever the value has another type. A tag sub-block satisfies it exactly, since the tag
is present precisely when the value is, so putting the tag first buys the filter, slice, keep-mask,
deep-copy, ref-counting and breaker accounting already written in that base class. It also answers in
constant time, it answers for an absent value, and it is what Arrow's union calls the types buffer.

The leg types in the block are physical, not logical. The block lives in the compute module, which does
not depend on `esql-core` outside its tests and so cannot name a `DataType` at all. The logical type of
each leg therefore travels beside the column as the leg order, which the plan and the response agree on,
and `ColumnInfoImpl.originalTypes` is the field that already carries exactly that list to clients for a
conflicted column today. So a date stays a date rather than becoming the number it is stored as, but it
does so because leg two is declared a date and the value is tagged two, not because the block holds the
word "date". Two legs may share an element type while differing logically, which is why a leg is
addressed by index and never by its element type.

We will carry that in one channel, because the user asked for one column. `Layout` forbids two channels
for one attribute id but says nothing against one logical column spread over several attribute ids,
which is what the current union path does; the single channel is a response-shape requirement, not a
`Layout` one. The container itself is not new. `AggregateMetricDoubleArrayBlock.asCompositeBlock()` is
already the wire form of an aggregate-metric-double block toward older nodes, and aggregate_metric_double,
exponential histogram and tdigest are compound blocks of several typed sub-blocks that reach clients
today. What is new is grouping on one and rendering one, since grouping currently throws for the
composite element type and the only data type mapping to it is excluded from output.

**We will change grouping** so the key is the pair of type and value. Group-key hashing dispatches per
element type on a single channel today, so this means either a dedicated hash or decomposing into the
packed-values path, which already handles several channels. The second looks more realistic.

**We will change aggregate resolution so per-type aggregates work**, which is the work the partial-sums
rule needs and which an earlier draft of this document left out. Three pieces. Sum, min and max have to
accept the union type at resolution, where today `Sum.dataType()` reads `field().dataType()` once and
`NumericAggregate.supplier()` switches on it and throws for anything it does not recognise. The
aggregator has to sum per sub-block and emit a union, since the grouping aggregator currently sees every
row of the channel regardless of group. And the planner has to check the guard structurally: that the
grouping key is `TYPEOF` of the attribute being aggregated. A runtime partition cannot feed a
planning-time resolution, so without that check the rule has no mechanism.

**We will add response rendering**, per row by type, and an Arrow union. The block layout above, with
every leg full length and null where the row is another type, is Arrow's *sparse* union: a types buffer
and one child per type, each the length of the whole column. The tag sub-block is that types buffer, so
the mapping is a write-out rather than a translation. A dense union adds an offsets buffer
and compacts each child to only its own values, so it is smaller on the wire but needs a compaction step
the blocks don't do. Sparse maps directly; dense is the optimisation. Arrow's union
is fully typed in the schema rather than opaque bytes, and it is a schema-level commitment, so a
consumer either handles unions or fails before any data arrives.

**We will add transport versioning**, since both the type and the block cross the wire between
coordinator and data nodes and between clusters.

**We will change the dataset path separately, and it needs its own mechanism.** Dataset files never
produce a conflict today, because `SchemaReconciliation` resolves disagreement before planning, so none
of the above reaches them. The route is identity-or-absent per file on the existing per-file column
mapping rather than teaching datasets to produce a conflict field. We will start with the fold to
keyword, the only case that destroys ordering and arithmetic on top of losing pruning, and we will leave
the numeric promotions alone, because turning long plus double into a surfaced conflict would refuse
aggregations and range filters on columns that work today. One case stays out of reach of either
mechanism: a single NDJSON row holding a mixed-type array is folded inside the file by the schema
inferrer, below the per-file granularity both designs work at.

## What's unsettled

Pushdown works, with one condition we control and one we don't. There is a dedicated branch for this:
`if (attr.field() instanceof UnionTypeEsField) { FieldName name = new FieldName(attr.name()); return
stats.isIndexed(name) && stats.hasDocValues(name); }`. A union-typed attribute is pushable when its
attribute name is a real field name that every shard on the node indexes with doc values.
`testToDateNanosPushDown` exercises both sides: the auto-promoted all-dates field pushes into
`EsQueryExec`, because the promotion keeps the real field name, while an explicitly cast field stays in
a `FilterExec`, because a cast names its attribute `$$x$converted_to$type` and no shard maps that.

So the condition we control is naming. `TYPED` has to produce an attribute carrying the real field name
rather than a synthetic one, and then pushdown is the existing path with no new rule. Note that
`aggregatable` being false on the per-type field doesn't block anything: it only skips a shortcut, and
the date field that does push has it false too.

The condition we don't control is that the decision is per node, not per shard. The node's statistics
are built over all its shard contexts and every one has to agree, so a predicate pushes only where every
shard on that node maps the field as the type being asked about. A node holding both a long-mapped and
a keyword-mapped shard pushes nothing for that field, and the predicate falls back to the filter. That
is narrower than per-source and it is not the match-none behaviour an earlier draft of this document
claimed. Getting per-shard emission instead would be new work; whether it's worth it is open.

The tuple's type component may need normalising. A field that is integer in one source and long in
another gives two groups for the same number. Probably not what anyone wants. The fix is to group by a
widened type rather than the mapped one, and the same question decides whether `TYPED(x, long)`
includes an integer-mapped source. Which widening is safe is open: the obvious existing one isn't,
since it promotes datetime to date_nanos and instants outside that range have no representation.

Every refusal has to be scoped to what's refused today, or we take away the aggregate_metric_double and
all-dates behaviour that works now.

Whether `SORT` stays refused. Discover's column sort will meet that refusal on every union column.

Multi-valued union columns. A field can hold several values in one row, and a union field can hold
several of different types, so this is a real shape rather than a corner. The compound block base class
refuses `filter`, `slice`, `keepMask` and `expand` on any multi-valued compound block, which is why
tdigest and exponential histogram are single-valued today as well. Supporting it is a change to that
base class rather than to the union block, so three block types gain it at once, and the size of that
change is not yet known.
