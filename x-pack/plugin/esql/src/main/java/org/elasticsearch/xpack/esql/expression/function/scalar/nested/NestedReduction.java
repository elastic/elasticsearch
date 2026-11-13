/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
    private final List<Attribute> inputs;
    private final List<String> paths;
    private final List<Set<String>> mappedIndices;

    public NestedReduction(
        Source source,
        List<Expression> metadata,
        String path,
        Set<String> indices,
        Expression body,
        List<Attribute> inputs,
        List<String> paths,
        List<Set<String>> mappedIndices
    ) {
        super(source, metadata);
        this.path = path;
        this.indices = Set.copyOf(indices);
        this.body = body;
        this.inputs = List.copyOf(inputs);
        this.paths = List.copyOf(paths);
        this.mappedIndices = mappedIndices.stream().map(Set::copyOf).toList();
    }

    private NestedReduction(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readString(),
            in.readCollectionAsImmutableSet(StreamInput::readString),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList(),
            in.readCollectionAsList(stream -> stream.readCollectionAsImmutableSet(StreamInput::readString))
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
        out.writeNamedWriteableCollection(inputs);
        out.writeStringCollection(paths);
        out.writeCollection(mappedIndices, StreamOutput::writeStringCollection);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return body.dataType().noText();
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
        return NodeInfo.create(this, NestedReduction::new, children(), path, indices, body, inputs, paths, mappedIndices);
    }

    @Override
    public NestedReduction replaceChildren(List<Expression> children) {
        return new NestedReduction(source(), children, path, indices, body, inputs, paths, mappedIndices);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && obj instanceof NestedReduction other
            && path.equals(other.path)
            && indices.equals(other.indices)
            && body.equals(other.body)
            && inputs.equals(other.inputs)
            && paths.equals(other.paths)
            && mappedIndices.equals(other.mappedIndices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, indices, body, inputs, paths, mappedIndices);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        Expression value = prepareScalar(body instanceof Sum sum ? sum.field() : body instanceof Last last ? last.field() : body);
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
        List<? extends Expression> scalarBodies = body instanceof Sum ? List.of(value)
            : body instanceof Last last ? List.of(value, prepareScalar(last.sort()))
            : List.of(value);
        var scalars = scalarBodies.stream().map(e -> EvalMapper.toEvaluator(toEvaluator.foldCtx(), e, layout.build())).toList();
        var metadata = children().stream().map(toEvaluator::apply).toList();
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
                return new NestedReductionOperator(context, args, functions);
            } catch (RuntimeException | Error e) {
                Releasables.closeWhileHandlingException(args.toArray(ExpressionEvaluator[]::new));
                Releasables.closeWhileHandlingException(functions.toArray(ExpressionEvaluator[]::new));
                throw e;
            }
        };
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
        private long nextInputRowToken;

        NestedReductionOperator(DriverContext context, List<ExpressionEvaluator> metadata, List<ExpressionEvaluator> scalars) {
            this.context = context;
            this.metadata = metadata;
            this.scalars = scalars;
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
                    if (args[0].isNull(row)) {
                        throw new IllegalArgumentException("NESTED source-backed PoC requires stored, unfiltered _source");
                    }
                    BytesRef source = NestedSource.singleMetadata(args[0], row, "_source");
                    long estimate = 256L + 8L * source.length;
                    context.breaker().addEstimateBytesAndMaybeBreak(estimate, "nested_reduction_frame");
                    // Parser charges outlive parser close: decoded values remain in use until this parent is complete.
                    try (var parsedSource = new ParsedContentReservation(context.breaker(), true)) {
                        result.accept(reduce(source, index, token, parsedSource));
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

        private Object reduce(BytesRef source, String index, long token, ParsedContentReservation parsedSource) {
            List<Map<?, ?>> objects = NestedSource.objects(path, source, parsedSource, context);
            if (objects.isEmpty()) {
                return emptyResult();
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
