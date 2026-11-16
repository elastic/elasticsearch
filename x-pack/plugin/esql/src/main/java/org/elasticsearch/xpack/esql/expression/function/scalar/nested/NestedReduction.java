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
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
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
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
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
    private final String path;
    private final Expression body;
    /** The {@code BY} keys among {@link #inputs}; empty when the reduction is not grouped. */
    private final List<Attribute> groupings;
    /** Returns the objects as a JSON array instead of reducing them; {@link #body} is unused. */
    private final boolean json;
    /** A predicate must hold for every object, as in {@code NESTED_ALL}, rather than for at least one. */
    private final boolean all;
    private final List<Attribute> inputs;
    private final List<String> paths;

    public NestedReduction(
        Source source,
        String path,
        Expression body,
        List<Attribute> groupings,
        boolean json,
        boolean all,
        List<Attribute> inputs,
        List<String> paths
    ) {
        super(source, List.of());
        this.path = path;
        this.body = body;
        this.groupings = List.copyOf(groupings);
        this.json = json;
        this.all = all;
        this.inputs = List.copyOf(inputs);
        this.paths = List.copyOf(paths);
    }

    private NestedReduction(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readString(),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean(),
            in.readBoolean(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList()
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
        out.writeString(path);
        out.writeNamedWriteable(body);
        out.writeNamedWriteableCollection(groupings);
        out.writeBoolean(json);
        out.writeBoolean(all);
        out.writeNamedWriteableCollection(inputs);
        out.writeStringCollection(paths);
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
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected NodeInfo<NestedReduction> info() {
        return NodeInfo.create(this, NestedReduction::new, path, body, groupings, json, all, inputs, paths);
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
     * it is an aggregate or reads more than child fields.
     */
    @Nullable
    public Expression childPredicate() {
        if (body instanceof ToAggregator || dataType() != DataType.BOOLEAN) {
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

    /** The child fields this reduction reads, relative to {@link #path}. */
    public List<String> paths() {
        return paths;
    }

    /** The types of the child fields in {@link #paths}. */
    public List<DataType> types() {
        return inputs.stream().map(Attribute::dataType).toList();
    }

    @Override
    public NestedReduction replaceChildren(List<Expression> children) {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && obj instanceof NestedReduction other
            && path.equals(other.path)
            && body.equals(other.body)
            && groupings.equals(other.groupings)
            && json == other.json
            && all == other.all
            && inputs.equals(other.inputs)
            && paths.equals(other.paths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, body, groupings, json, all, inputs, paths);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ReducerFactory reducer = reducer(toEvaluator.foldCtx());
        if (reducer == null) {
            return EvalMapper.toEvaluator(toEvaluator.foldCtx(), new Literal(source(), null, dataType()), new Layout.Builder().build());
        }
        var children = NestedChildren.factory(toEvaluator.shardContexts(), path, paths, types());
        if (children == null) {
            throw new IllegalStateException("NESTED reads the child documents of the shards on this node, and none are local");
        }
        var group = new NestedReduceOperator.Group(children, List.of(reducer), List.of(IntStream.rangeClosed(0, paths.size()).toArray()));
        return context -> {
            var reader = group.get(context);
            return new ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    return reader.reduce(page)[0];
                }

                @Override
                public long baseRamBytesUsed() {
                    return reader.baseRamBytesUsed();
                }

                @Override
                public void close() {
                    reader.close();
                }
            };
        };
    }

    /**
     * What reduces the children of each page of parents, fed by a read of the children it may share with other reductions over
     * {@link #path}. {@code null} when the result is null whatever the children, as ordinary folding would make it.
     */
    @Nullable
    public ReducerFactory reducer(FoldContext foldCtx) {
        if (json) {
            return new ReducerFactory(List.of(), null, null);
        }
        Expression value = prepareScalar(
            groupings.isEmpty() == false || body instanceof Sum || body instanceof Count || body instanceof Min || body instanceof Max
                ? ((UnaryAggregateFunction) body).field()
                : body instanceof Last last ? last.field()
                : body
        );
        if (body instanceof Last && value.foldable()) {
            // Preserve the declared type and fold once, so scalar evaluation does not repeat fold-time warnings.
            value = new Literal(value.source(), value.fold(foldCtx), value.dataType());
        }
        if (dataType() == DataType.NULL || body instanceof Last && value instanceof Literal literal && literal.value() == null) {
            // Ordinary optimization folds these to null. Deferred aggregate bodies need the same behavior without an unsupported NULL
            // supplier.
            return null;
        }
        Layout.Builder layout = new Layout.Builder();
        inputs.forEach(layout::append);
        List<Expression> scalarBodies = new ArrayList<>(groupings);
        if (body instanceof Last last) {
            scalarBodies.addAll(List.of(value, prepareScalar(last.sort())));
        } else {
            scalarBodies.add(value);
        }
        var scalars = scalarBodies.stream().map(e -> EvalMapper.toEvaluator(foldCtx, e, layout.build())).toList();
        GroupingAggregator.Factory aggregate = groupings.isEmpty() && body instanceof ToAggregator aggregator
            ? aggregator.supplier().groupingAggregatorFactory(AggregatorMode.SINGLE, IntStream.range(0, scalars.size()).boxed().toList())
            : null;
        return new ReducerFactory(scalars, groupings.isEmpty() ? null : groupingFactory(), aggregate);
    }

    /** Builds each driver's {@link Reducer}. */
    public final class ReducerFactory {
        private final List<ExpressionEvaluator.Factory> scalars;
        @Nullable
        private final HashAggregationOperator.Factory grouping;
        @Nullable
        private final GroupingAggregator.Factory aggregate;

        private ReducerFactory(
            List<ExpressionEvaluator.Factory> scalars,
            @Nullable HashAggregationOperator.Factory grouping,
            @Nullable GroupingAggregator.Factory aggregate
        ) {
            this.scalars = scalars;
            this.grouping = grouping;
            this.aggregate = aggregate;
        }

        public Reducer get(DriverContext context) {
            List<ExpressionEvaluator> functions = new ArrayList<>();
            try {
                for (var factory : scalars) {
                    functions.add(factory.get(context));
                }
                return new Reducer(context, functions, grouping, aggregate);
            } catch (RuntimeException | Error e) {
                Releasables.closeWhileHandlingException(functions.toArray(ExpressionEvaluator[]::new));
                throw e;
            }
        }

        @Override
        public String toString() {
            return NestedReduction.this.sourceText();
        }
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

    /**
     * One driver's reduction of the children of each page of parents, one value per parent. The children come from a read that
     * other reductions over the same nested field may share: each {@link Pass} takes the pages of one page of parents.
     */
    public final class Reducer implements Releasable {
        private final DriverContext context;
        private final List<ExpressionEvaluator> scalars;
        /** Groups the children by parent and key for {@code BY}; {@code null} when the reduction is not grouped. */
        @Nullable
        private final HashAggregationOperator.Factory grouping;
        /** Aggregates the children read from the index by parent; {@code null} for predicates, {@code BY} and JSON. */
        @Nullable
        private final GroupingAggregator.Factory aggregate;
        /** The child fields of each page a {@link Pass} takes, after its parent positions. */
        private final int[] fieldChannels;

        private Reducer(
            DriverContext context,
            List<ExpressionEvaluator> scalars,
            @Nullable HashAggregationOperator.Factory grouping,
            @Nullable GroupingAggregator.Factory aggregate
        ) {
            this.context = context;
            this.scalars = scalars;
            this.grouping = grouping;
            this.aggregate = aggregate;
            this.fieldChannels = IntStream.range(1, 1 + inputs.size()).toArray();
        }

        /** Starts reducing the children of a page of {@code positions} parents. */
        Pass start(int positions) {
            if (json) {
                return new JsonPass(positions);
            }
            if (grouping != null) {
                return new GroupPass(positions);
            }
            if (aggregate != null) {
                return new AggregatePass(positions);
            }
            return new MatchPass(positions);
        }

        /**
         * The children of one page of parents. {@link #accept} takes them in parent order, in pages whose block 0 holds each
         * child's parent position and whose other blocks are this reduction's child fields, then {@link #finish} gives one value
         * per parent.
         */
        abstract class Pass implements Releasable {
            final Object[] values;
            final boolean[] reduced;

            Pass(int positions) {
                values = new Object[positions];
                reduced = new boolean[positions];
            }

            abstract void accept(Page loaded);

            /** Sets the values of the parents whose children {@link #accept} took. */
            void complete() {}

            final Block finish() {
                complete();
                try (var result = BlockUtils.wrapperFor(context.blockFactory(), PlannerUtils.toElementType(dataType()), values.length)) {
                    for (int row = 0; row < values.length; row++) {
                        result.accept(reduced[row] ? values[row] : emptyResult());
                    }
                    return result.builder().build();
                }
            }

            @Override
            public void close() {}
        }

        private Object emptyResult() {
            if (body instanceof Count && grouping == null) {
                return 0L;
            }
            return json || body instanceof ToAggregator ? null : all;
        }

        /** The scalar bodies over the child fields of a page a {@link Pass} takes. */
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

        private final class MatchPass extends Pass {
            MatchPass(int positions) {
                super(positions);
            }

            @Override
            void accept(Page loaded) {
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
            }
        }

        /** One aggregator for the page of parents, grouped by parent position. */
        private final class AggregatePass extends Pass {
            private final GroupingAggregator aggregator;
            /**
             * The parents whose children earlier pages added, which the aggregator takes as having values when a null makes it
             * start tracking which do.
             */
            private final SeenGroupIds earlier = bigArrays -> {
                BitArray seen = new BitArray(reduced.length, bigArrays);
                for (int p = 0; p < reduced.length; p++) {
                    if (reduced[p]) {
                        seen.set(p);
                    }
                }
                return seen;
            };

            AggregatePass(int positions) {
                super(positions);
                aggregator = aggregate.apply(context);
            }

            @Override
            void accept(Page loaded) {
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
            }

            @Override
            void complete() {
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

            @Override
            public void close() {
                aggregator.close();
            }
        }

        /** Runs the children through STATS ... BY parent, keys and nests each parent's groups, one JSON level per key. */
        private final class GroupPass extends Pass {
            private final List<Map<String, Object>> groups;
            private final Operator stats;

            GroupPass(int positions) {
                super(positions);
                groups = new ArrayList<>(Collections.nCopies(positions, null));
                stats = grouping.get(context);
            }

            @Override
            void accept(Page loaded) {
                while (stats.needsInput() == false) {
                    collectGroups(stats.getOutput());
                }
                Block[] evaluated = evaluate(loaded);
                Block[] input = new Block[1 + evaluated.length];
                input[0] = loaded.getBlock(0);
                input[0].incRef();
                System.arraycopy(evaluated, 0, input, 1, evaluated.length);
                stats.addInput(new Page(loaded.getPositionCount(), input));
            }

            @Override
            void complete() {
                stats.finish();
                while (stats.isFinished() == false) {
                    collectGroups(stats.getOutput());
                }
                for (int p = 0; p < values.length; p++) {
                    Map<String, Object> parentGroups = groups.get(p);
                    if (parentGroups != null) {
                        values[p] = toJson(builder -> builder.map(parentGroups));
                        reduced[p] = true;
                    }
                }
            }

            /** Children with a null key are left out of the object. */
            @SuppressWarnings("unchecked")
            private void collectGroups(@Nullable Page page) {
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

            @Override
            public void close() {
                stats.close();
            }
        }

        /** Each parent's children as a JSON array of objects, a field missing where the child has no value for it. */
        private final class JsonPass extends Pass {
            private final List<List<Map<String, Object>>> objects;
            private long estimate;

            JsonPass(int positions) {
                super(positions);
                objects = new ArrayList<>(Collections.nCopies(positions, null));
            }

            @Override
            void accept(Page loaded) {
                long bytes = 64L * loaded.getPositionCount() * (1 + paths.size());
                context.breaker().addEstimateBytesAndMaybeBreak(bytes, "nested_reduction_frame");
                estimate += bytes;
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
            }

            @Override
            void complete() {
                for (int p = 0; p < values.length; p++) {
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
            }

            @Override
            public void close() {
                context.breaker().addWithoutBreaking(-estimate);
            }
        }

        long baseRamBytesUsed() {
            return 128 + scalars.stream().mapToLong(ExpressionEvaluator::baseRamBytesUsed).sum();
        }

        @Override
        public void close() {
            Releasables.close(scalars);
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
            return list.stream().map(NestedReduction::jsonValue).toList();
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
}
