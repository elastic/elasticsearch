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
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
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
import java.util.HashMap;
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
        return groupings.isEmpty() ? body.dataType().noText() : DataType.KEYWORD;
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
            inputs,
            paths,
            mappedIndices,
            sourceFailures
        );
    }

    public String path() {
        return path;
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
        return new NestedReduction(source(), children, path, indices, body, groupings, inputs, paths, mappedIndices, sourceFailures);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && obj instanceof NestedReduction other
            && path.equals(other.path)
            && indices.equals(other.indices)
            && body.equals(other.body)
            && groupings.equals(other.groupings)
            && inputs.equals(other.inputs)
            && paths.equals(other.paths)
            && mappedIndices.equals(other.mappedIndices)
            && sourceFailures.equals(other.sourceFailures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, indices, body, groupings, inputs, paths, mappedIndices, sourceFailures);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        Expression value = prepareScalar(
            groupings.isEmpty() == false ? ((UnaryAggregateFunction) body).field()
                : body instanceof Sum sum ? sum.field()
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
        var shards = NestedIndexObjects.shards(toEvaluator.shardContexts(), path, paths);
        if (shards == null) {
            if (groupings.isEmpty() == false) {
                throw groupedSourceRead();
            }
            NestedSource.checkSources(sourceFailures);
        }
        HashAggregationOperator.Factory grouping = groupings.isEmpty() ? null : groupingFactory();
        return context -> {
            List<ExpressionEvaluator> args = new ArrayList<>();
            List<ExpressionEvaluator> functions = new ArrayList<>();
            try {
                for (var factory : metadata) {
                    args.add(factory.get(context));
                }
                for (var factory : scalars) {
                    functions.add(factory.get(context));
                }
                return new NestedReductionOperator(
                    context,
                    args,
                    functions,
                    shards == null ? null : new NestedIndexObjects(shards, paths),
                    grouping
                );
            } catch (RuntimeException | Error e) {
                Releasables.closeWhileHandlingException(args.toArray(ExpressionEvaluator[]::new));
                Releasables.closeWhileHandlingException(functions.toArray(ExpressionEvaluator[]::new));
                throw e;
            }
        };
    }

    private IllegalArgumentException groupedSourceRead() {
        return new IllegalArgumentException("NESTED BY reads doc values only; [" + path + "] would have to be read from _source here");
    }

    /** The STATS ... BY operator over one parent's children: the keys come first in each page, the aggregate input last. */
    private HashAggregationOperator.Factory groupingFactory() {
        List<BlockHash.GroupSpec> specs = new ArrayList<>(groupings.size());
        for (int i = 0; i < groupings.size(); i++) {
            specs.add(new BlockHash.GroupSpec(i, PlannerUtils.toElementType(groupings.get(i).dataType())));
        }
        var aggregator = ((ToAggregator) body).supplier().groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(groupings.size()));
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
        /** Reads child objects from the index when pages carry {@code _doc}; {@code null} reads {@code _source}. */
        @Nullable
        private final NestedIndexObjects indexed;
        /** Groups each parent's children for {@code BY}; {@code null} when the reduction is not grouped. */
        @Nullable
        private final HashAggregationOperator.Factory grouping;
        private long nextInputRowToken;

        NestedReductionOperator(
            DriverContext context,
            List<ExpressionEvaluator> metadata,
            List<ExpressionEvaluator> scalars,
            @Nullable NestedIndexObjects indexed,
            @Nullable HashAggregationOperator.Factory grouping
        ) {
            this.context = context;
            this.metadata = metadata;
            this.scalars = scalars;
            this.indexed = indexed;
            this.grouping = grouping;
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
                DocVector docs = indexed == null ? null : NestedIndexObjects.docs(parents);
                for (int row = 0; row < parents.getPositionCount(); row++) {
                    context.checkForEarlyTermination();
                    long token = nextInputRowToken++;
                    String index = NestedSource.singleMetadata(args[1], row, "_index").utf8ToString();
                    if (index.isEmpty()) {
                        throw new IllegalArgumentException("NESTED requires non-empty _index metadata");
                    }
                    if (indices.contains(index) == false) {
                        result.accept(emptyResult());
                        continue;
                    }
                    if (docs != null) {
                        List<Map<?, ?>> objects = indexed.objects(docs, row, context);
                        long estimate = 64L * objects.size() * (1 + paths.size());
                        context.breaker().addEstimateBytesAndMaybeBreak(estimate, "nested_reduction_frame");
                        try {
                            result.accept(reduce(objects, index, token));
                        } finally {
                            context.breaker().addWithoutBreaking(-estimate);
                        }
                        continue;
                    }
                    if (grouping != null) {
                        throw groupedSourceRead();
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
            return body instanceof ToAggregator ? null : false;
        }

        private Object reduce(List<Map<?, ?>> objects, String index, long token) {
            if (objects.isEmpty()) {
                return emptyResult();
            }
            if (grouping != null) {
                return reduceGroups(objects, index, token);
            }
            // One aggregate per incoming parent, retained until all its child pages have contributed.
            try (
                Aggregator aggregator = body instanceof ToAggregator aggregate
                    ? aggregate.supplier()
                        .aggregatorFactory(AggregatorMode.SINGLE, IntStream.range(0, scalars.size()).boxed().toList())
                        .apply(context)
                    : null
            ) {
                boolean matched = false;
                for (int offset = 0; offset < objects.size();) {
                    int count = Math.min(CHILD_PAGE_SIZE, objects.size() - offset);
                    try (var frame = loadFrame(objects, index, token, offset, count)) {
                        // Evaluate every page even after a match, so later invalid predicates cannot be hidden.
                        matched |= evaluateFrame(frame, aggregator);
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

        /** Runs one parent's children through STATS ... BY and nests the groups, one JSON level per key. */
        private BytesRef reduceGroups(List<Map<?, ?>> objects, String index, long token) {
            Map<String, Object> groups = new TreeMap<>();
            try (Operator stats = grouping.get(context)) {
                for (int offset = 0; offset < objects.size();) {
                    int count = Math.min(CHILD_PAGE_SIZE, objects.size() - offset);
                    try (var frame = loadFrame(objects, index, token, offset, count)) {
                        Block[] evaluated = new Block[scalars.size()];
                        try {
                            for (int i = 0; i < evaluated.length; i++) {
                                evaluated[i] = scalars.get(i).eval(frame.objects());
                            }
                        } catch (RuntimeException | Error e) {
                            Releasables.closeExpectNoException(evaluated);
                            throw e;
                        }
                        while (stats.needsInput() == false) {
                            collectGroups(stats.getOutput(), groups);
                        }
                        stats.addInput(new Page(count, evaluated));
                    }
                    offset += count;
                }
                stats.finish();
                while (stats.isFinished() == false) {
                    collectGroups(stats.getOutput(), groups);
                }
            }
            if (groups.isEmpty()) {
                return null;
            }
            try (XContentBuilder json = XContentFactory.jsonBuilder()) {
                json.map(groups);
                return BytesReference.bytes(json).toBytesRef();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /** Children with a null key are left out of the object. */
        @SuppressWarnings("unchecked")
        private void collectGroups(@Nullable Page page, Map<String, Object> groups) {
            if (page == null) {
                return;
            }
            try {
                int keys = groupings.size();
                rows: for (int position = 0; position < page.getPositionCount(); position++) {
                    Map<String, Object> level = groups;
                    for (int k = 0; k < keys; k++) {
                        Block key = page.getBlock(k);
                        if (key.isNull(position)) {
                            continue rows;
                        }
                        Object value = BlockUtils.toJavaObject(key, position);
                        String name = value instanceof BytesRef bytes ? bytes.utf8ToString() : value.toString();
                        if (k == keys - 1) {
                            level.put(name, BlockUtils.toJavaObject(page.getBlock(keys), position));
                        } else {
                            level = (Map<String, Object>) level.computeIfAbsent(name, n -> new TreeMap<String, Object>());
                        }
                    }
                }
            } finally {
                page.releaseBlocks();
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
                boolean matched = false;
                for (int i = 0; i < evaluated[0].getPositionCount(); i++) {
                    if ((i & 1023) == 0) {
                        context.checkForEarlyTermination();
                    }
                    if (evaluated[0].getValueCount(i) > 1) {
                        throw new IllegalArgumentException("NESTED existence requires a scalar predicate result per child");
                    }
                    // A true child cannot hide an invalid multivalue result later in the same scope.
                    matched |= Boolean.TRUE.equals(BlockUtils.toJavaObject(evaluated[0], i));
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
            Releasables.close(Releasables.wrap(metadata), Releasables.wrap(scalars));
        }
    }

}
