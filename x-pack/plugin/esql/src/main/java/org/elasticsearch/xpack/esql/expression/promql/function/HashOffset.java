/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.MixHash64;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.GroupKeyEncoder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper.ToEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Internal, non-user-callable scalar computing the stable sampling offset of a series identity for PromQL
 * {@code limit_ratio}. For each row it encodes the key columns exactly like the grouping hash (multivalues with list
 * semantics, nulls as empty) and scales the stable 64-bit hash to an offset in {@code [0, 1)}.
 * <p>
 * The caller compares the offset against the requested ratio ({@code offset < r} for a non-negative ratio,
 * {@code offset >= 1 + r} for a negative one); edge ratios never reach this function because the translator emits
 * a constant filter or no filter for them instead.
 */
public final class HashOffset extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "HashOffset",
        HashOffset::new
    );

    private final List<Expression> keys;

    public HashOffset(Source source, List<Expression> keys) {
        super(source, keys);
        this.keys = keys;
    }

    private HashOffset(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(keys);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    /**
     * Never null: even absent (null) keys and the empty key set of a constant vector encode to a shared
     * identity with a valid offset, so every row is decided instead of filtered as null.
     */
    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    /**
     * Never folds so every row hashes in the engine: drivers deciding the same identity must observe
     * the same offset however rows are partitioned, and the empty key set must not constant-fold away
     * the per-step sampling of constant range queries.
     */
    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        for (Expression key : keys) {
            ElementType elementType;
            try {
                elementType = PlannerUtils.toElementType(key.dataType());
            } catch (RuntimeException e) {
                return new TypeResolution("hash_offset does not support key type [" + key.dataType() + "]");
            }
            switch (elementType) {
                case INT, LONG, DOUBLE, DOUBLE_RANGE, FLOAT, BOOLEAN, BYTES_REF, NULL -> {
                }
                default -> {
                    return new TypeResolution("hash_offset does not support key type [" + key.dataType() + "]");
                }
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new HashOffset(source(), newChildren);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, HashOffset::new, keys);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<ExpressionEvaluator.Factory> argFactories = keys.stream().map(toEvaluator::apply).toList();
        List<ElementType> elementTypes = keys.stream().map(key -> PlannerUtils.toElementType(key.dataType())).toList();
        return new HashOffsetEvaluatorFactory(argFactories, elementTypes);
    }

    /**
     * Scales a 64-bit hash to a sampling offset in {@code [0, 1)}. Multiplying the unsigned value by
     * 2^-64 is exact, so every hash maps to a distinct offset with no rounding skew beyond the
     * unavoidable double rounding of values above 2^53 (which stays monotonic).
     */
    static double toOffset(long hash) {
        double unsigned = hash >= 0 ? (double) hash : (double) (hash & Long.MAX_VALUE) + 0x1p63;
        return unsigned * 0x1p-64;
    }

    static final class HashOffsetEvaluator implements ExpressionEvaluator {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HashOffsetEvaluator.class);

        private final DriverContext driverContext;
        private final List<ExpressionEvaluator> args;
        private final GroupKeyEncoder encoder;
        private final double emptyOffset;

        HashOffsetEvaluator(DriverContext driverContext, List<ExpressionEvaluator> args, List<ElementType> elementTypes) {
            this.driverContext = driverContext;
            this.args = args;
            BlockFactory blockFactory = driverContext.blockFactory();
            this.encoder = new GroupKeyEncoder(
                IntStream.range(0, args.size()).toArray(),
                elementTypes,
                new PagedBytesBuilder(blockFactory.bigArrays().recycler(), blockFactory.breaker(), "hash-offset", 64)
            );
            // The empty key set encodes to zero bytes; precompute its shared identity offset once.
            this.emptyOffset = toOffset(MixHash64.hash64(new byte[0], 0, 0));
        }

        @Override
        public Block eval(Page page) {
            int positions = page.getPositionCount();
            BlockFactory blockFactory = driverContext.blockFactory();
            if (args.isEmpty()) {
                try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(positions)) {
                    for (int p = 0; p < positions; p++) {
                        builder.appendDouble(emptyOffset);
                    }
                    return builder.build();
                }
            }
            Block[] argBlocks = new Block[args.size()];
            for (int i = 0; i < args.size(); i++) {
                argBlocks[i] = args.get(i).eval(page);
            }
            Page keys = new Page(argBlocks);
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(positions)) {
                for (int p = 0; p < positions; p++) {
                    PagedBytesCursor key = encoder.encode(keys, p);
                    builder.appendDouble(toOffset(key.mixHash64()));
                }
                return builder.build();
            } finally {
                keys.releaseBlocks();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            long size = SHALLOW_SIZE + encoder.ramBytesUsed();
            for (ExpressionEvaluator arg : args) {
                size += arg.baseRamBytesUsed();
            }
            return size;
        }

        @Override
        public String toString() {
            return "HashOffset" + args;
        }

        @Override
        public void close() {
            for (ExpressionEvaluator arg : args) {
                arg.close();
            }
            encoder.close();
        }
    }

    record HashOffsetEvaluatorFactory(List<ExpressionEvaluator.Factory> args, List<ElementType> elementTypes)
        implements
            ExpressionEvaluator.Factory {

        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new HashOffsetEvaluator(context, args.stream().map(factory -> factory.get(context)).toList(), elementTypes);
        }

        @Override
        public String toString() {
            return "HashOffset" + args;
        }
    }
}
