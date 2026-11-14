/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.UnaryAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.IntStream;

/** Deferred per-parent work: item pages and aggregate state cannot escape this reduction boundary. */
public final class NestedReduction extends EsqlScalarFunction {
    public static final TransportVersion VERSION = TransportVersion.fromName("esql_nested_reductions");
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "NestedReduction",
        NestedReduction::new
    );
    // Bound child compute positions; source decoding and individual multivalue fields remain materialized.
    private static final int CHILD_PAGE_SIZE = 1024;
    private final String path;
    private final Set<String> indices;
    private final Expression body;
    /** The {@code BY} keys among {@link #inputs}; empty when the reduction is not grouped. */
    private final List<Attribute> groupings;
    /** Returns the objects as a JSON array instead of reducing them; {@link #body} is unused. */
    private final boolean json;
    /** A predicate must hold for every object, as in {@code NESTED_ALL}, rather than for at least one. */
    private final boolean all;
    private final List<Attribute> inputs;
    private final List<String> paths;
    private final List<Set<String>> mappedIndices;
    /** Why the reduction cannot read the {@code _source} of some of {@link #indices}, by index name. */
    private final Map<String, String> sourceFailures;

    public NestedReduction(
        Source source,
        List<Expression> metadata,
        String path,
        Set<String> indices,
        Expression body,
        List<Attribute> groupings,
        boolean json,
        boolean all,
        List<Attribute> inputs,
        List<String> paths,
        List<Set<String>> mappedIndices,
        Map<String, String> sourceFailures
    ) {
        super(source, metadata);
        this.path = path;
        this.indices = Set.copyOf(indices);
        this.body = body;
        this.groupings = List.copyOf(groupings);
        this.json = json;
        this.all = all;
        this.inputs = List.copyOf(inputs);
        this.paths = List.copyOf(paths);
        this.mappedIndices = mappedIndices.stream().map(Set::copyOf).toList();
        this.sourceFailures = Map.copyOf(sourceFailures);
    }

    private NestedReduction(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readString(),
            in.readCollectionAsImmutableSet(StreamInput::readString),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean(),
            in.readBoolean(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList(),
            in.readCollectionAsList(stream -> stream.readCollectionAsImmutableSet(StreamInput::readString)),
            in.readImmutableMap(StreamInput::readString)
        );
    }

    /** Reject older peers before writing any new expression or operator representation. */
    public static void checkVersion(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(VERSION) == false) {
            throw new IOException("NESTED requires nested reductions transport support");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkVersion(out);
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
        out.writeString(path);
        out.writeStringCollection(indices);
        out.writeNamedWriteable(body);
        out.writeNamedWriteableCollection(groupings);
        out.writeBoolean(json);
        out.writeBoolean(all);
        out.writeNamedWriteableCollection(inputs);
        out.writeStringCollection(paths);
        out.writeCollection(mappedIndices, StreamOutput::writeStringCollection);
        out.writeMap(sourceFailures, StreamOutput::writeString);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return json || groupings.isEmpty() == false ? DataType.KEYWORD : body.dataType().noText();
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    protected TypeResolution resolveType() {
        return childrenResolved() ? TypeResolution.TYPE_RESOLVED : new TypeResolution("unresolved nested metadata");
    }

    @Override
    protected NodeInfo<NestedReduction> info() {
        return NodeInfo.create(
            this,
            NestedReduction::new,
            children(),
            path,
            indices,
            body,
            groupings,
            json,
            all,
            inputs,
            paths,
            mappedIndices,
            sourceFailures
        );
    }

    public String path() {
        return path;
    }

    /** Whether the predicate must hold for every object rather than for at least one. */
    public boolean all() {
        return all;
    }

    /**
     * This existence check over the child fields themselves, for Lucene to match within each child document. {@code null} when
     * it is an aggregate, reads more than child fields, or a child field is not mapped in every index the reduction reads: there
     * a child value is null, which the in-memory check sees but a query on the field does not.
     */
    @Nullable
    public Expression childPredicate() {
        if (body instanceof ToAggregator
            || dataType() != DataType.BOOLEAN
            || mappedIndices.stream().anyMatch(mapped -> mapped.equals(indices) == false)) {
            return null;
        }
        Map<NameId, FieldAttribute> fields = new HashMap<>();
        for (int i = 0; i < inputs.size(); i++) {
            String name = path + "." + paths.get(i);
            EsField field = new EsField(name, inputs.get(i).dataType(), Map.of(), true, EsField.TimeSeriesFieldType.NONE);
            fields.put(inputs.get(i).id(), new FieldAttribute(inputs.get(i).source(), name, field));
        }
        if (body.references().stream().anyMatch(a -> fields.containsKey(a.id()) == false)) {
            return null;
        }
        return body.transformUp(Attribute.class, a -> fields.get(a.id()));
    }

    /** The full names of the child fields this reduction reads. */
    public List<String> fieldNames() {
        return paths.stream().map(p -> path + "." + p).toList();
    }

    /** A copy that does not ask for {@code _source}, for data nodes where every child field has doc values. */
    public NestedReduction withoutSource() {
        Expression source = children().getFirst();
        return replaceChildren(List.of(new Literal(source.source(), null, source.dataType()), children().get(1)));
    }

    @Override
    public NestedReduction replaceChildren(List<Expression> children) {
        return new NestedReduction(
            source(),
            children,
            path,
            indices,
            body,
            groupings,
            json,
            all,
            inputs,
            paths,
            mappedIndices,
            sourceFailures
        );
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && obj instanceof NestedReduction other
            && path.equals(other.path)
            && indices.equals(other.indices)
            && body.equals(other.body)
            && groupings.equals(other.groupings)
            && json == other.json
            && all == other.all
            && inputs.equals(other.inputs)
            && paths.equals(other.paths)
            && mappedIndices.equals(other.mappedIndices)
            && sourceFailures.equals(other.sourceFailures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, indices, body, groupings, json, all, inputs, paths, mappedIndices, sourceFailures);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var reader = NestedChildren.factory(toEvaluator.shardContexts(), path, paths, inputs.stream().map(Attribute::dataType).toList());
        if (json) {
            if (reader == null) {
                throw sourceRead();
            }
            return operatorFactory(children().stream().map(toEvaluator::apply).toList(), List.of(), reader, null, null);
        }
        Expression value = prepareScalar(
            groupings.isEmpty() == false || body instanceof Sum || body instanceof Count ? ((UnaryAggregateFunction) body).field()
                : body instanceof Last last ? last.field()
                : body
        );
        if (body instanceof Last && value.foldable()) {
            // Preserve the declared type and fold once, so scalar evaluation does not repeat fold-time warnings.
            value = new Literal(value.source(), value.fold(toEvaluator.foldCtx()), value.dataType());
        }
        if (dataType() == DataType.NULL || body instanceof Last && value instanceof Literal literal && literal.value() == null) {
            // Ordinary optimization folds these to null. Deferred aggregate bodies need the same behavior without an unsupported NULL
            // supplier.
            return EvalMapper.toEvaluator(toEvaluator.foldCtx(), new Literal(source(), null, dataType()), new Layout.Builder().build());
        }
        Layout.Builder layout = new Layout.Builder();
        inputs.forEach(layout::append);
        List<Expression> scalarBodies = new ArrayList<>(groupings);
        if (body instanceof Last last) {
            scalarBodies.addAll(List.of(value, prepareScalar(last.sort())));
        } else {
            scalarBodies.add(value);
        }
        var scalars = scalarBodies.stream().map(e -> EvalMapper.toEvaluator(toEvaluator.foldCtx(), e, layout.build())).toList();
        var metadata = children().stream().map(toEvaluator::apply).toList();
        if (reader == null) {
            if (groupings.isEmpty() == false) {
                throw sourceRead();
            }
            NestedSource.checkSources(sourceFailures);
        }
        GroupingAggregator.Factory aggregate = groupings.isEmpty() && body instanceof ToAggregator aggregator
            ? aggregator.supplier().groupingAggregatorFactory(AggregatorMode.SINGLE, IntStream.range(0, scalars.size()).boxed().toList())
            : null;
        return operatorFactory(metadata, scalars, reader, groupings.isEmpty() ? null : groupingFactory(), aggregate);
    }

    private ExpressionEvaluator.Factory operatorFactory(
        List<ExpressionEvaluator.Factory> metadata,
        List<ExpressionEvaluator.Factory> scalars,
        @Nullable NestedChildren.Factory reader,
        @Nullable HashAggregationOperator.Factory grouping,
        @Nullable GroupingAggregator.Factory aggregate
    ) {
        return context -> {
            List<ExpressionEvaluator> args = new ArrayList<>();
            List<ExpressionEvaluator> functions = new ArrayList<>();
            NestedChildren children = null;
            try {
                for (var factory : metadata) {
                    args.add(factory.get(context));
                }
                for (var factory : scalars) {
                    functions.add(factory.get(context));
                }
                children = reader == null ? null : reader.get(context);
                return new NestedReductionOperator(context, args, functions, children, grouping, aggregate);
            } catch (RuntimeException | Error e) {
                Releasables.closeWhileHandlingException(args.toArray(ExpressionEvaluator[]::new));
                Releasables.closeWhileHandlingException(functions.toArray(ExpressionEvaluator[]::new));
                Releasables.closeWhileHandlingException(children);
                throw e;
            }
        };
    }

    private IllegalArgumentException sourceRead() {
        return new IllegalArgumentException(
            (json ? "NESTED_JSON" : "NESTED BY") + " reads doc values only; [" + path + "] would have to be read from _source here"
        );
    }

    /**
     * The STATS ... BY operator over the children of a page of parents: each page holds the parent position, then the
     * keys, then the aggregate input. Grouping on the parent position too keeps each parent's groups apart.
     */
    private HashAggregationOperator.Factory groupingFactory() {
        List<BlockHash.GroupSpec> specs = new ArrayList<>(1 + groupings.size());
        specs.add(new BlockHash.GroupSpec(0, ElementType.INT));
        for (int i = 0; i < groupings.size(); i++) {
            specs.add(new BlockHash.GroupSpec(i + 1, PlannerUtils.toElementType(groupings.get(i).dataType())));
        }
        var aggregator = ((ToAggregator) body).supplier().groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(groupings.size() + 1));
        return new HashAggregationOperator.Builder().groups(specs).mode(AggregatorMode.SINGLE).aggregators(List.of(aggregator)).build();
    }

    /** Deferred scalar bodies need ordinary null propagation before constructing conversion evaluators. */
    private static Expression prepareScalar(Expression expression) {
        return expression.transformUp(
            e -> e instanceof EvaluatorMapper
                && (Expressions.isGuaranteedNull(e)
                    || e instanceof AnyNullIsNull && e.children().stream().anyMatch(Expressions::isGuaranteedNull))
                        ? Literal.of(e, null)
                        : e
        );
    }

    /** Pages for one input position share its token; neither their blocks nor their aggregate state can escape the reduction. */
    private record InputRowFrame(long token, Page objects) implements AutoCloseable {
        @Override
        public void close() {
            objects.releaseBlocks();
        }
    }

    /** Physical execution is row preserving: one bounded child frame is fully reduced before attachment. */
    private final class NestedReductionOperator implements ExpressionEvaluator {
        private final DriverContext context;
        private final List<ExpressionEvaluator> metadata;
        private final List<ExpressionEvaluator> scalars;
        /** Reads the children from the index when pages carry {@code _doc}; {@code null} reads {@code _source}. */
        @Nullable
        private final NestedChildren children;
        /** Groups the children by parent and key for {@code BY}; {@code null} when the reduction is not grouped. */
        @Nullable
        private final HashAggregationOperator.Factory grouping;
        /** Aggregates the children read from the index by parent; {@code null} for predicates, {@code BY} and JSON. */
        @Nullable
        private final GroupingAggregator.Factory aggregate;
        /** The child fields of each page {@link #children} gives, after its parent positions. */
        private final int[] fieldChannels;
        private long nextInputRowToken;

        NestedReductionOperator(
            DriverContext context,
            List<ExpressionEvaluator> metadata,
            List<ExpressionEvaluator> scalars,
            @Nullable NestedChildren children,
            @Nullable HashAggregationOperator.Factory grouping,
            @Nullable GroupingAggregator.Factory aggregate
        ) {
            this.context = context;
            this.metadata = metadata;
            this.scalars = scalars;
            this.children = children;
            this.grouping = grouping;
            this.aggregate = aggregate;
            this.fieldChannels = IntStream.range(1, 1 + inputs.size()).toArray();
        }

        @Override
        public Block eval(Page parents) {
            Block[] args = new Block[metadata.size()];
            try (
                var result = BlockUtils.wrapperFor(
                    context.blockFactory(),
                    PlannerUtils.toElementType(dataType()),
                    parents.getPositionCount()
                )
            ) {
                for (int i = 0; i < args.length; i++) {
                    args[i] = metadata.get(i).eval(parents);
                }
                DocVector docs = children == null ? null : NestedChildren.docs(parents);
                if (docs != null) {
                    for (Object value : fromIndex(docs, args[1])) {
                        result.accept(value);
                    }
                    return result.builder().build();
                }
                for (int row = 0; row < parents.getPositionCount(); row++) {
                    context.checkForEarlyTermination();
                    long token = nextInputRowToken++;
                    String index = indexName(args[1], row);
                    if (indices.contains(index) == false) {
                        result.accept(emptyResult());
                        continue;
                    }
                    if (grouping != null || json) {
                        throw sourceRead();
                    }
                    NestedSource.checkSource(sourceFailures, index);
                    if (args[0].isNull(row)) {
                        throw new IllegalArgumentException("NESTED source-backed PoC requires stored, unfiltered _source");
                    }
                    BytesRef source = NestedSource.singleMetadata(args[0], row, "_source");
                    long estimate = 256L + 8L * source.length;
                    context.breaker().addEstimateBytesAndMaybeBreak(estimate, "nested_reduction_frame");
                    // Parser charges outlive parser close: decoded values remain in use until this parent is complete.
                    try (var parsedSource = new ParsedContentReservation(context.breaker(), true)) {
                        result.accept(reduce(NestedSource.objects(path, source, parsedSource, context), index, token));
                    } finally {
                        context.breaker().addWithoutBreaking(-estimate);
                    }
                }
                return result.builder().build();
            } finally {
                Releasables.closeExpectNoException(args);
            }
        }

        private Object emptyResult() {
            if (body instanceof Count && grouping == null) {
                return 0L;
            }
            return json || body instanceof ToAggregator ? null : all;
        }

        /** Reduces the children of a page of parents read from the index, one value per parent. */
        private Object[] fromIndex(DocVector docs, Block indexNames) {
            int positions = docs.getPositionCount();
            boolean[] included = new boolean[positions];
            for (int row = 0; row < positions; row++) {
                included[row] = indices.contains(indexName(indexNames, row));
            }
            Object[] values = new Object[positions];
            boolean[] reduced = new boolean[positions];
            if (json) {
                jsonFromIndex(docs, included, values, reduced);
            } else if (grouping != null) {
                groupsFromIndex(docs, included, values, reduced);
            } else if (aggregate != null) {
                aggregateFromIndex(docs, included, values, reduced);
            } else {
                matchFromIndex(docs, included, values, reduced);
            }
            for (int row = 0; row < positions; row++) {
                if (reduced[row] == false) {
                    values[row] = emptyResult();
                }
            }
            return values;
        }

        /** The scalar bodies over the child fields of a page {@link #children} gives. */
        private Block[] evaluate(Page loaded) {
            Page fields = loaded.projectBlocks(fieldChannels);
            Block[] evaluated = new Block[scalars.size()];
            try {
                for (int i = 0; i < evaluated.length; i++) {
                    evaluated[i] = scalars.get(i).eval(fields);
                }
                return evaluated;
            } catch (RuntimeException | Error e) {
                Releasables.closeExpectNoException(evaluated);
                throw e;
            } finally {
                fields.releaseBlocks();
            }
        }

        private void matchFromIndex(DocVector docs, boolean[] included, Object[] values, boolean[] reduced) {
            children.read(docs, p -> included[p], loaded -> {
                IntVector parents = loaded.<IntBlock>getBlock(0).asVector();
                Block[] evaluated = evaluate(loaded);
                try {
                    Block matches = evaluated[0];
                    for (int c = 0; c < matches.getPositionCount(); c++) {
                        if ((c & 1023) == 0) {
                            context.checkForEarlyTermination();
                        }
                        if (matches.getValueCount(c) > 1) {
                            throw new IllegalArgumentException(
                                (all ? "NESTED_ALL" : "NESTED_ANY") + " requires a scalar predicate result per child"
                            );
                        }
                        // Every child is evaluated even once the answer is known, so later invalid predicates cannot be hidden.
                        boolean childMatched = Boolean.TRUE.equals(BlockUtils.toJavaObject(matches, c));
                        int parent = parents.getInt(c);
                        boolean matched = reduced[parent] ? (Boolean) values[parent] : all;
                        values[parent] = all ? matched & childMatched : matched | childMatched;
                        reduced[parent] = true;
                    }
                } finally {
                    Releasables.closeExpectNoException(evaluated);
                }
            });
        }

        /** One aggregator for the page, grouped by parent position. */
        private void aggregateFromIndex(DocVector docs, boolean[] included, Object[] values, boolean[] reduced) {
            // The parents whose children earlier pages added, which the aggregator takes as having values when a null makes
            // it start tracking which do.
            SeenGroupIds earlier = bigArrays -> {
                BitArray seen = new BitArray(reduced.length, bigArrays);
                for (int p = 0; p < reduced.length; p++) {
                    if (reduced[p]) {
                        seen.set(p);
                    }
                }
                return seen;
            };
            try (GroupingAggregator aggregator = aggregate.apply(context)) {
                children.read(docs, p -> included[p], loaded -> {
                    IntVector parents = loaded.<IntBlock>getBlock(0).asVector();
                    Page input = new Page(loaded.getPositionCount(), evaluate(loaded));
                    try {
                        if (body instanceof Last) {
                            requireScalars(input);
                        }
                        var add = aggregator.prepareProcessPage(earlier, input);
                        if (add != null) {
                            try (add) {
                                add.add(0, parents);
                            }
                        }
                    } finally {
                        input.releaseBlocks();
                    }
                    for (int c = 0; c < parents.getPositionCount(); c++) {
                        reduced[parents.getInt(c)] = true;
                    }
                });
                int[] selected = IntStream.range(0, reduced.length).filter(p -> reduced[p]).toArray();
                if (selected.length == 0) {
                    return;
                }
                aggregator.aggregatorFunction().selectedMayContainUnseenGroups(earlier);
                Block[] results = new Block[1];
                try (
                    IntVector groups = context.blockFactory().newIntArrayVector(selected, selected.length);
                    var prepared = aggregator.prepareForEvaluate(groups, new GroupingAggregatorEvaluationContext(context))
                ) {
                    prepared.evaluate(results, 0, groups);
                    for (int i = 0; i < selected.length; i++) {
                        Object value = BlockUtils.toJavaObject(results[0], i);
                        // The wrapper's scalar contract permits any candidate tied at the maximum key.
                        values[selected[i]] = body instanceof Last && value instanceof List<?> ties
                            ? (ties.isEmpty() ? null : ties.getFirst())
                            : value;
                    }
                } finally {
                    Releasables.closeExpectNoException(results);
                }
            }
        }

        /** Runs the children through STATS ... BY parent, keys and nests each parent's groups, one JSON level per key. */
        private void groupsFromIndex(DocVector docs, boolean[] included, Object[] values, boolean[] reduced) {
            List<Map<String, Object>> groups = new ArrayList<>(Collections.nCopies(included.length, null));
            try (Operator stats = grouping.get(context)) {
                children.read(docs, p -> included[p], loaded -> {
                    while (stats.needsInput() == false) {
                        collectGroups(stats.getOutput(), groups);
                    }
                    Block[] evaluated = evaluate(loaded);
                    Block[] input = new Block[1 + evaluated.length];
                    input[0] = loaded.getBlock(0);
                    input[0].incRef();
                    System.arraycopy(evaluated, 0, input, 1, evaluated.length);
                    stats.addInput(new Page(loaded.getPositionCount(), input));
                });
                stats.finish();
                while (stats.isFinished() == false) {
                    collectGroups(stats.getOutput(), groups);
                }
            }
            for (int p = 0; p < included.length; p++) {
                Map<String, Object> parentGroups = groups.get(p);
                if (parentGroups != null) {
                    values[p] = toJson(builder -> builder.map(parentGroups));
                    reduced[p] = true;
                }
            }
        }

        /** Children with a null key are left out of the object. */
        @SuppressWarnings("unchecked")
        private void collectGroups(@Nullable Page page, List<Map<String, Object>> groups) {
            if (page == null) {
                return;
            }
            try {
                IntBlock parents = page.getBlock(0);
                int keys = groupings.size();
                rows: for (int position = 0; position < page.getPositionCount(); position++) {
                    for (int k = 0; k < keys; k++) {
                        if (page.getBlock(k + 1).isNull(position)) {
                            continue rows;
                        }
                    }
                    int parent = parents.getInt(parents.getFirstValueIndex(position));
                    Map<String, Object> level = groups.get(parent);
                    if (level == null) {
                        level = new TreeMap<>();
                        groups.set(parent, level);
                    }
                    for (int k = 0; k < keys; k++) {
                        Block key = page.getBlock(k + 1);
                        Object value = BlockUtils.toJavaObject(key, position);
                        String name = value instanceof BytesRef bytes ? bytes.utf8ToString() : value.toString();
                        if (k == keys - 1) {
                            level.put(name, BlockUtils.toJavaObject(page.getBlock(keys + 1), position));
                        } else {
                            level = (Map<String, Object>) level.computeIfAbsent(name, n -> new TreeMap<String, Object>());
                        }
                    }
                }
            } finally {
                page.releaseBlocks();
            }
        }

        /** Each parent's children as a JSON array of objects, a field missing where the child has no value for it. */
        private void jsonFromIndex(DocVector docs, boolean[] included, Object[] values, boolean[] reduced) {
            List<List<Map<String, Object>>> objects = new ArrayList<>(Collections.nCopies(included.length, null));
            long[] estimate = new long[1];
            try {
                children.read(docs, p -> included[p], loaded -> {
                    long bytes = 64L * loaded.getPositionCount() * (1 + paths.size());
                    context.breaker().addEstimateBytesAndMaybeBreak(bytes, "nested_reduction_frame");
                    estimate[0] += bytes;
                    IntVector parents = loaded.<IntBlock>getBlock(0).asVector();
                    for (int c = 0; c < loaded.getPositionCount(); c++) {
                        Map<String, Object> object = new LinkedHashMap<>();
                        for (int f = 0; f < paths.size(); f++) {
                            Block field = loaded.getBlock(f + 1);
                            if (field.isNull(c) == false) {
                                put(object, paths.get(f), jsonValue(BlockUtils.toJavaObject(field, c)));
                            }
                        }
                        int parent = parents.getInt(c);
                        if (objects.get(parent) == null) {
                            objects.set(parent, new ArrayList<>());
                        }
                        objects.get(parent).add(object);
                    }
                });
                for (int p = 0; p < included.length; p++) {
                    List<Map<String, Object>> parentObjects = objects.get(p);
                    if (parentObjects != null) {
                        values[p] = toJson(builder -> {
                            builder.startArray();
                            for (Map<String, Object> object : parentObjects) {
                                builder.map(object);
                            }
                            builder.endArray();
                        });
                        reduced[p] = true;
                    }
                }
            } finally {
                context.breaker().addWithoutBreaking(-estimate[0]);
            }
        }

        private Object reduce(List<Map<?, ?>> objects, String index, long token) {
            if (objects.isEmpty()) {
                return emptyResult();
            }
            // One aggregate per incoming parent, retained until all its child pages have contributed.
            try (
                Aggregator aggregator = body instanceof ToAggregator toAggregator
                    ? toAggregator.supplier()
                        .aggregatorFactory(AggregatorMode.SINGLE, IntStream.range(0, scalars.size()).boxed().toList())
                        .apply(context)
                    : null
            ) {
                boolean matched = all;
                for (int offset = 0; offset < objects.size();) {
                    int count = Math.min(CHILD_PAGE_SIZE, objects.size() - offset);
                    try (var frame = loadFrame(objects, index, token, offset, count)) {
                        // Evaluate every page even once the answer is known, so later invalid predicates cannot be hidden.
                        boolean frameMatched = evaluateFrame(frame, aggregator);
                        matched = all ? matched & frameMatched : matched | frameMatched;
                    }
                    offset += count;
                }
                if (aggregator == null) {
                    return matched;
                }
                Block[] reduced = new Block[1];
                try {
                    aggregator.evaluate(reduced, 0, context);
                    Object value = BlockUtils.toJavaObject(reduced[0], 0);
                    // The wrapper's scalar contract permits any candidate tied at the maximum key.
                    return body instanceof Last && value instanceof List<?> ties ? (ties.isEmpty() ? null : ties.getFirst()) : value;
                } finally {
                    Releasables.closeExpectNoException(reduced);
                }
            }
        }

        private static BytesRef toJson(CheckedConsumer<XContentBuilder, IOException> content) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                content.accept(builder);
                return BytesReference.bytes(builder).toBytesRef();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /** A child value as JSON writes it: keywords as strings. */
        private static Object jsonValue(Object value) {
            if (value instanceof BytesRef bytes) {
                return bytes.utf8ToString();
            }
            if (value instanceof List<?> list) {
                return list.stream().map(NestedReductionOperator::jsonValue).toList();
            }
            return value;
        }

        /** Puts {@code value} at the dotted {@code path}, creating the objects above it. */
        @SuppressWarnings("unchecked")
        private static void put(Map<String, Object> object, String path, Object value) {
            int dot;
            while ((dot = path.indexOf('.')) >= 0) {
                object = (Map<String, Object>) object.computeIfAbsent(path.substring(0, dot), k -> new LinkedHashMap<String, Object>());
                path = path.substring(dot + 1);
            }
            object.put(path, value);
        }

        private String indexName(Block indexNames, int row) {
            String index = NestedSource.singleMetadata(indexNames, row, "_index").utf8ToString();
            if (index.isEmpty()) {
                throw new IllegalArgumentException("NESTED requires non-empty _index metadata");
            }
            return index;
        }

        private static void requireScalars(Page page) {
            for (int b = 0; b < page.getBlockCount(); b++) {
                Block block = page.getBlock(b);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    if (block.getValueCount(i) > 1) {
                        throw new IllegalArgumentException("NESTED selection requires scalar child values and ordering keys");
                    }
                }
            }
        }

        private InputRowFrame loadFrame(List<Map<?, ?>> objects, String index, long token, int offset, int count) {
            Block[] channels = new Block[inputs.size()];
            try {
                for (int c = 0; c < channels.length; c++) {
                    try (
                        var wrapper = BlockUtils.wrapperFor(
                            context.blockFactory(),
                            PlannerUtils.toElementType(inputs.get(c).dataType()),
                            count
                        )
                    ) {
                        for (int position = 0; position < count; position++) {
                            if ((position & 1023) == 0) {
                                context.checkForEarlyTermination();
                            }
                            wrapper.accept(
                                mappedIndices.get(c).contains(index)
                                    ? NestedSource.childValue(
                                        objects.get(offset + position),
                                        paths.get(c),
                                        inputs.get(c).dataType(),
                                        context
                                    )
                                    : null
                            );
                        }
                        channels[c] = wrapper.builder().build();
                    }
                }
                var frame = new InputRowFrame(token, new Page(count, channels));
                channels = null;
                return frame;
            } finally {
                if (channels != null) {
                    Releasables.closeExpectNoException(channels);
                }
            }
        }

        private boolean evaluateFrame(InputRowFrame frame, Aggregator aggregator) {
            Block[] evaluated = new Block[scalars.size()];
            try {
                for (int i = 0; i < evaluated.length; i++) {
                    evaluated[i] = scalars.get(i).eval(frame.objects());
                }
                if (aggregator != null) {
                    if (body instanceof Last) {
                        for (Block block : evaluated) {
                            for (int i = 0; i < block.getPositionCount(); i++) {
                                if (block.getValueCount(i) > 1) {
                                    throw new IllegalArgumentException("NESTED selection requires scalar child values and ordering keys");
                                }
                            }
                        }
                    }
                    try (var mask = context.blockFactory().newConstantBooleanVector(true, frame.objects().getPositionCount())) {
                        aggregator.processPage(new Page(frame.objects().getPositionCount(), evaluated), mask);
                    }
                    return false;
                }
                boolean matched = all;
                for (int i = 0; i < evaluated[0].getPositionCount(); i++) {
                    if ((i & 1023) == 0) {
                        context.checkForEarlyTermination();
                    }
                    if (evaluated[0].getValueCount(i) > 1) {
                        throw new IllegalArgumentException(
                            (all ? "NESTED_ALL" : "NESTED_ANY") + " requires a scalar predicate result per child"
                        );
                    }
                    // A decided answer cannot hide an invalid multivalue result later in the same scope.
                    boolean childMatched = Boolean.TRUE.equals(BlockUtils.toJavaObject(evaluated[0], i));
                    matched = all ? matched & childMatched : matched | childMatched;
                }
                return matched;
            } finally {
                Releasables.closeExpectNoException(evaluated);
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return 128 + metadata.stream().mapToLong(ExpressionEvaluator::baseRamBytesUsed).sum() + scalars.stream()
                .mapToLong(ExpressionEvaluator::baseRamBytesUsed)
                .sum();
        }

        @Override
        public void close() {
            Releasables.close(Releasables.wrap(metadata), Releasables.wrap(scalars), children);
        }
    }

}
