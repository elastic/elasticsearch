/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.UnaryAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * A {@link NestedStats} lowered to one value per parent. The aggregate's fields are not columns of the parent page;
 * they are loaded from the child documents. The parent {@code _doc} stays on that page through
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LoadNestedFromIndex}.
 */
public final class NestedReduce extends EsqlScalarFunction {
    public static final TransportVersion VERSION = TransportVersion.fromName("esql_nested");
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "NestedReduce",
        NestedReduce::new
    );

    private final String path;
    private final Expression body;
    /** Child fields of {@link #body}, in the order {@link NestedChildren} loads them. */
    private final List<Attribute> inputs;
    private final List<String> relativePaths;

    public NestedReduce(Source source, String path, Expression body, List<Attribute> inputs, List<String> relativePaths) {
        super(source, List.of());
        this.path = path;
        this.body = body;
        this.inputs = List.copyOf(inputs);
        this.relativePaths = List.copyOf(relativePaths);
    }

    private NestedReduce(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readString(),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList()
        );
    }

    public static void checkVersion(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(VERSION) == false) {
            throw new IOException("NESTED requires nested reduction transport support");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkVersion(out);
        source().writeTo(out);
        out.writeString(path);
        out.writeNamedWriteable(body);
        out.writeNamedWriteableCollection(inputs);
        out.writeStringCollection(relativePaths);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return body.dataType();
    }

    @Override
    public Nullability nullable() {
        return body instanceof Count ? Nullability.FALSE : Nullability.TRUE;
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
    protected NodeInfo<NestedReduce> info() {
        return NodeInfo.create(this, NestedReduce::new, path, body, inputs, relativePaths);
    }

    @Override
    public NestedReduce replaceChildren(List<Expression> newChildren) {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && obj instanceof NestedReduce other
            && path.equals(other.path)
            && body.equals(other.body)
            && inputs.equals(other.inputs)
            && relativePaths.equals(other.relativePaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), path, body, inputs, relativePaths);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        Layout layout = new Layout.Builder().append(inputs).build();
        FoldContext foldCtx = toEvaluator.foldCtx();
        List<Expression> scalars = scalarBodies();
        List<ExpressionEvaluator.Factory> scalarFactories = new ArrayList<>(scalars.size());
        for (Expression scalar : scalars) {
            scalarFactories.add(EvalMapper.toEvaluator(foldCtx, scalar, layout, toEvaluator.shardContexts(), null));
        }
        List<ToAggregator> aggregators = aggregators();
        List<GroupingAggregator.Factory> factories = new ArrayList<>(aggregators.size());
        for (ToAggregator aggregator : aggregators) {
            factories.add(aggregator.supplier().groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(0)));
        }
        List<DataType> types = inputs.stream().map(Attribute::dataType).toList();
        NestedChildren.Factory children = NestedChildren.factory(
            toEvaluator.shardContexts(),
            path,
            relativePaths,
            types,
            Collections.nCopies(types.size(), (ValuesSourceReaderOperator.ConverterFactory) null),
            PlannerSettings.DEFAULTS
        );
        int[] fieldChannels = IntStream.range(0, inputs.size()).map(i -> i + 1).toArray();
        boolean count = body instanceof Count;
        DataType resultType = dataType();
        return context -> new Evaluator(context, children.get(context), scalarFactories, factories, fieldChannels, count, resultType);
    }

    /** The value each aggregator reads. AVG is SUM of the value, as a double when it is a long, divided by COUNT. */
    private List<Expression> scalarBodies() {
        if (body instanceof Avg avg) {
            Expression field = avg.field();
            Expression sumField = field.dataType() == DataType.LONG ? new ToDouble(field.source(), field) : field;
            return List.of(sumField, field);
        }
        return List.of(((UnaryAggregateFunction) body).field());
    }

    private List<ToAggregator> aggregators() {
        if (body instanceof Avg avg) {
            Expression field = avg.field();
            Expression sumField = field.dataType() == DataType.LONG ? new ToDouble(field.source(), field) : field;
            return List.of(new Sum(avg.source(), sumField), new Count(avg.source(), field));
        }
        return List.of((ToAggregator) body);
    }

    private static final class Evaluator implements ExpressionEvaluator {
        private final DriverContext context;
        private final NestedChildren children;
        private final List<ExpressionEvaluator> scalars;
        private final List<GroupingAggregator.Factory> factories;
        private final int[] fieldChannels;
        private final boolean count;
        private final DataType resultType;

        private Evaluator(
            DriverContext context,
            NestedChildren children,
            List<ExpressionEvaluator.Factory> scalarFactories,
            List<GroupingAggregator.Factory> factories,
            int[] fieldChannels,
            boolean count,
            DataType resultType
        ) {
            this.context = context;
            this.children = children;
            this.factories = factories;
            this.fieldChannels = fieldChannels;
            this.count = count;
            this.resultType = resultType;
            List<ExpressionEvaluator> built = new ArrayList<>(scalarFactories.size());
            try {
                for (ExpressionEvaluator.Factory factory : scalarFactories) {
                    built.add(factory.get(context));
                }
                this.scalars = built;
            } catch (RuntimeException | Error e) {
                Releasables.closeWhileHandlingException(Releasables.wrap(built));
                throw e;
            }
        }

        @Override
        public Block eval(Page page) {
            var docs = docs(page);
            int positions = docs.getPositionCount();
            boolean[] reduced = new boolean[positions];
            List<GroupingAggregator> aggregators = new ArrayList<>(factories.size());
            try {
                for (GroupingAggregator.Factory factory : factories) {
                    aggregators.add(factory.apply(context));
                }
                SeenGroupIds earlier = bigArrays -> seen(reduced, bigArrays);
                children.read(docs, position -> true, loaded -> accept(loaded, aggregators, earlier, reduced));
                return finish(aggregators, earlier, reduced);
            } finally {
                Releasables.close(aggregators);
            }
        }

        private void accept(Page loaded, List<GroupingAggregator> aggregators, SeenGroupIds earlier, boolean[] reduced) {
            IntVector parents = loaded.<IntBlock>getBlock(0).asVector();
            if (parents == null) {
                throw new IllegalStateException("NESTED expected one parent position per child");
            }
            Page fields = loaded.projectBlocks(fieldChannels);
            Block[] evaluated = new Block[scalars.size()];
            try {
                for (int i = 0; i < evaluated.length; i++) {
                    evaluated[i] = scalars.get(i).eval(fields);
                }
                for (int i = 0; i < aggregators.size(); i++) {
                    Block values = evaluated[i];
                    evaluated[i] = null;
                    Page input = new Page(values.getPositionCount(), values);
                    try {
                        var add = aggregators.get(i).prepareProcessPage(earlier, input);
                        if (add != null) {
                            try (add) {
                                add.add(0, parents);
                            }
                        }
                    } finally {
                        input.releaseBlocks();
                    }
                }
                for (int c = 0; c < parents.getPositionCount(); c++) {
                    reduced[parents.getInt(c)] = true;
                }
            } finally {
                Releasables.closeExpectNoException(evaluated);
                fields.releaseBlocks();
            }
        }

        private Block finish(List<GroupingAggregator> aggregators, SeenGroupIds earlier, boolean[] reduced) {
            int[] selected = IntStream.range(0, reduced.length).filter(p -> reduced[p]).toArray();
            Object[] values = new Object[reduced.length];
            if (selected.length > 0) {
                List<Block> results = new ArrayList<>();
                try (IntVector groups = context.blockFactory().newIntArrayVector(selected, selected.length)) {
                    for (GroupingAggregator aggregator : aggregators) {
                        aggregator.aggregatorFunction().selectedMayContainUnseenGroups(earlier);
                        Block[] out = new Block[1];
                        try (var prepared = aggregator.prepareForEvaluate(groups, new GroupingAggregatorEvaluationContext(context))) {
                            prepared.evaluate(out, 0, groups);
                            results.add(out[0]);
                        }
                    }
                    for (int i = 0; i < selected.length; i++) {
                        values[selected[i]] = combine(results, i);
                    }
                } finally {
                    Releasables.close(results);
                }
            }
            try (var wrapper = BlockUtils.wrapperFor(context.blockFactory(), PlannerUtils.toElementType(resultType), reduced.length)) {
                for (int row = 0; row < reduced.length; row++) {
                    if (reduced[row]) {
                        wrapper.accept(values[row]);
                    } else {
                        wrapper.accept(count ? 0L : null);
                    }
                }
                return wrapper.builder().build();
            }
        }

        /** AVG is the sum divided by the count. A single aggregate is that aggregate's value. */
        private Object combine(List<Block> results, int position) {
            if (results.size() == 1) {
                return BlockUtils.toJavaObject(results.get(0), position);
            }
            Object sum = BlockUtils.toJavaObject(results.get(0), position);
            Object seen = BlockUtils.toJavaObject(results.get(1), position);
            if (sum instanceof Number total && seen instanceof Number counted && counted.doubleValue() != 0.0d) {
                return total.doubleValue() / counted.doubleValue();
            }
            return null;
        }

        private static BitArray seen(boolean[] reduced, BigArrays bigArrays) {
            BitArray bits = new BitArray(reduced.length, bigArrays);
            for (int p = 0; p < reduced.length; p++) {
                if (reduced[p]) {
                    bits.set(p);
                }
            }
            return bits;
        }

        private static org.elasticsearch.compute.data.DocVector docs(Page page) {
            for (int i = 0; i < page.getBlockCount(); i++) {
                if (page.getBlock(i) instanceof org.elasticsearch.compute.data.DocBlock doc) {
                    return doc.asVector();
                }
            }
            throw new IllegalStateException("NESTED needs the parent documents, which this page does not carry");
        }

        @Override
        public long baseRamBytesUsed() {
            long bytes = 0;
            for (ExpressionEvaluator scalar : scalars) {
                bytes += scalar.baseRamBytesUsed();
            }
            return bytes;
        }

        @Override
        public void close() {
            Releasables.close(scalars);
            Releasables.close(children);
        }
    }
}
