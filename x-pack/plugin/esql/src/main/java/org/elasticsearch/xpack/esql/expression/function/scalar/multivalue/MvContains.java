/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCounters;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Function that takes two multivalued expressions and checks if values of one expression are all present(equals) in the other.
 * <p>
 * Given Set A = {"a","b","c"} and Set B = {"b","c"}, the relationship between first (row) and second (column) arguments is:
 * <ul>
 *     <li>A, B &rArr; true  (A &sube; B)</li>
 *     <li>B, A &rArr; false (A &#8840; B)</li>
 *     <li>A, A &rArr; true (A &equiv; A)</li>
 *     <li>B, B &rArr; true (B &equiv; B)</li>
 *     <li>A, null &rArr; true (B &sube; &empty;)</li>
 *     <li>null, A &rArr; false (&empty; &#8840; B)</li>
 *     <li>B, null &rArr; true (B &sube; &empty;)</li>
 *     <li>null, B &rArr; false (&empty; &#8840; B)</li>
 *     <li>null, null &rArr; true (&empty; &equiv; &empty;)</li>
 * </ul>
 */
public class MvContains extends BinaryScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvContains",
        MvContains::new
    );

    @FunctionInfo(
        returnType = "boolean",
        description = "Checks if all values yielded by the second multivalue expression are present in the values yielded by "
            + "the first multivalue expression. Returns a boolean. Null values are treated as an empty set.",
        examples = {
            @Example(file = "string", tag = "mv_contains"),
            @Example(file = "string", tag = "mv_contains_bothsides"),
            @Example(file = "string", tag = "mv_contains_where"), },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") }
    )
    public MvContains(
        Source source,
        @Param(
            name = "superset",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "geohash",
                "geotile",
                "geohex",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "Multivalue expression."
        ) Expression superset,
        @Param(
            name = "subset",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "geohash",
                "geotile",
                "geohex",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "Multivalue expression."
        ) Expression subset
    ) {
        super(source, superset, subset);
    }

    private MvContains(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isRepresentableExceptCounters(left(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (left().dataType() == DataType.NULL) {
            return isRepresentableExceptCounters(right(), sourceText(), SECOND);
        }
        return isType(right(), t -> t.noText() == left().dataType().noText(), sourceText(), SECOND, left().dataType().noText().typeName());
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected MvContains replaceChildren(Expression newLeft, Expression newRight) {
        return new MvContains(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvContains::new, left(), right());
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var supersetType = PlannerUtils.toElementType(left().dataType());
        var subsetType = PlannerUtils.toElementType(right().dataType());

        if (subsetType == ElementType.NULL) {
            return EvalOperator.CONSTANT_TRUE_FACTORY;
        }

        if (supersetType != ElementType.NULL && supersetType != subsetType) {
            throw new EsqlIllegalArgumentException(
                "Incompatible data types for MvContains, superset type({}) value({}) and subset type({}) value({}) don't match.",
                supersetType,
                left(),
                subsetType,
                right()
            );
        }

        return switch (supersetType) {
            case BOOLEAN -> new MvContainsBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvContainsBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvContainsDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvContainsIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvContainsLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case NULL -> new IsNull.IsNullEvaluatorFactory(toEvaluator.apply(right()));
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType());
        };
    }

    // @Evaluator(extraName = "Int") see end of file.
    static void process(BooleanBlock.Builder builder, int position, IntBlock field1, IntBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, IntBlock::getInt));
    }

    // @Evaluator(extraName = "Boolean") see end of file.
    static void process(BooleanBlock.Builder builder, int position, BooleanBlock field1, BooleanBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, BooleanBlock::getBoolean));
    }

    // @Evaluator(extraName = "Long") see end of file.
    static void process(BooleanBlock.Builder builder, int position, LongBlock field1, LongBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, LongBlock::getLong));
    }

    // @Evaluator(extraName = "Double") see end of file.
    static void process(BooleanBlock.Builder builder, int position, DoubleBlock field1, DoubleBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, DoubleBlock::getDouble));
    }

    // @Evaluator(extraName = "BytesRef") see end of file.
    static void process(BooleanBlock.Builder builder, int position, BytesRefBlock field1, BytesRefBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, (block, index) -> {
            var ref = new BytesRef();
            // we pass in a reference, but sometimes we only get a return value, see ConstantBytesRefVector.getBytesRef
            ref = block.getBytesRef(index, ref);
            // pass empty ref as null
            if (ref.length == 0) {
                return null;
            }
            return ref;
        }));
    }

    static void appendTo(BooleanBlock.Builder builder, Boolean bool) {
        if (bool == null) {
            builder.appendNull();
        } else {
            builder.beginPositionEntry().appendBoolean(bool).endPositionEntry();
        }
    }

    /**
     * A block is considered a subset if the superset contains values that test equal for all the values in the subset, independent of
     * order. Duplicates are ignored in the sense that for each duplicate in the subset, we will search/match against the first/any value
     * in the superset.
     *
     * @param superset block to check against
     * @param subset   block containing values that should be present in the other block.
     * @return {@code true} if the given blocks are a superset and subset to each other, {@code false} if not.
     */
    static <BlockType extends Block, Type> Boolean containsAll(
        BlockType superset,
        BlockType subset,
        final int position,
        ValueExtractor<BlockType, Type> valueExtractor
    ) {
        if (superset == subset) {
            return true;
        }
        if (subset.areAllValuesNull()) {
            return true;
        }

        final var valueCount = subset.getValueCount(position);
        final var startIndex = subset.getFirstValueIndex(position);
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            var value = valueExtractor.extractValue(subset, valueIndex);
            if (value == null) { // null entries are considered to always be an element in the superset.
                continue;
            }
            if (hasValue(superset, position, value, valueExtractor) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if the block has the value at any of it's positions
     * @param superset Block to search
     * @param value to search for
     * @return true if the supplied long value is in the supplied Block
     */
    static <BlockType extends Block, Type> boolean hasValue(
        BlockType superset,
        final int position,
        Type value,
        ValueExtractor<BlockType, Type> valueExtractor
    ) {
        final var supersetCount = superset.getValueCount(position);
        final var startIndex = superset.getFirstValueIndex(position);
        for (int supersetIndex = startIndex; supersetIndex < startIndex + supersetCount; supersetIndex++) {
            var element = valueExtractor.extractValue(superset, supersetIndex);
            if (element != null && element.equals(value)) {
                return true;
            }
        }
        return false;
    }

    interface ValueExtractor<BlockType extends Block, Type> {
        Type extractValue(BlockType block, int position);
    }

    /**
     * Evaluator that always returns true for all values in the block (~column)
     */
    public static final class ConstantBooleanTrueEvaluator implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext driverContext) {
            return new ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    return driverContext.blockFactory().newConstantBooleanBlockWith(true, page.getPositionCount());
                }

                @Override
                public void close() {}

                @Override
                public String toString() {
                    return "ConstantBooleanTrueEvaluator";
                }

                @Override
                public long baseRamBytesUsed() {
                    return 0;
                }
            };
        }

        @Override
        public String toString() {
            return "ConstantBooleanTrueEvaluator";
        }
    }

    /**
     *  Currently {@code EvaluatorImplementer} generates:
     *          if (allBlocksAreNulls) {
     *           result.appendNull();
     *           continue position;
     *         }
     *  when all params are null, this violates our contract of always returning a boolean.
     *  It should probably also generate the warnings method conditionally - omitted here.
     *  TODO extend code generation to handle this case
     */
    public static class MvContainsBooleanEvaluator implements EvalOperator.ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvContainsBooleanEvaluator.class);
        private final EvalOperator.ExpressionEvaluator field1;
        private final EvalOperator.ExpressionEvaluator field2;
        private final DriverContext driverContext;

        public MvContainsBooleanEvaluator(
            EvalOperator.ExpressionEvaluator field1,
            EvalOperator.ExpressionEvaluator field2,
            DriverContext driverContext
        ) {
            this.field1 = field1;
            this.field2 = field2;
            this.driverContext = driverContext;
        }

        @Override
        public Block eval(Page page) {
            try (BooleanBlock field1Block = (BooleanBlock) field1.eval(page)) {
                try (BooleanBlock field2Block = (BooleanBlock) field2.eval(page)) {
                    return eval(page.getPositionCount(), field1Block, field2Block);
                }
            }
        }

        public BooleanBlock eval(int positionCount, BooleanBlock field1Block, BooleanBlock field2Block) {
            try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    MvContains.process(result, p, field1Block, field2Block);
                }
                return result.build();
            }
        }

        @Override
        public String toString() {
            return "MvContainsBooleanEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field1, field2);
        }

        @Override
        public long baseRamBytesUsed() {
            long baseRamBytesUsed = BASE_RAM_BYTES_USED;
            baseRamBytesUsed += field1.baseRamBytesUsed();
            baseRamBytesUsed += field2.baseRamBytesUsed();
            return baseRamBytesUsed;
        }

        public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
            private final Source source;
            private final EvalOperator.ExpressionEvaluator.Factory field1;
            private final EvalOperator.ExpressionEvaluator.Factory field2;

            public Factory(
                Source source,
                EvalOperator.ExpressionEvaluator.Factory field1,
                EvalOperator.ExpressionEvaluator.Factory field2
            ) {
                this.source = source;
                this.field1 = field1;
                this.field2 = field2;
            }

            @Override
            public MvContainsBooleanEvaluator get(DriverContext context) {
                return new MvContainsBooleanEvaluator(field1.get(context), field2.get(context), context);
            }

            @Override
            public String toString() {
                return "MvContainsBooleanEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
            }
        }
    }

    /**
     *  Currently {@code EvaluatorImplementer} generates:
     *          if (allBlocksAreNulls) {
     *           result.appendNull();
     *           continue position;
     *         }
     *  when all params are null, this violates our contract of always returning a boolean.
     *  It should probably also generate the warnings method conditionally - omitted here.
     *  TODO extend code generation to handle this case
     */
    public static class MvContainsBytesRefEvaluator implements EvalOperator.ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvContainsBytesRefEvaluator.class);
        private final EvalOperator.ExpressionEvaluator field1;
        private final EvalOperator.ExpressionEvaluator field2;
        private final DriverContext driverContext;

        public MvContainsBytesRefEvaluator(
            EvalOperator.ExpressionEvaluator field1,
            EvalOperator.ExpressionEvaluator field2,
            DriverContext driverContext
        ) {
            this.field1 = field1;
            this.field2 = field2;
            this.driverContext = driverContext;
        }

        @Override
        public Block eval(Page page) {
            try (BytesRefBlock field1Block = (BytesRefBlock) field1.eval(page)) {
                try (BytesRefBlock field2Block = (BytesRefBlock) field2.eval(page)) {
                    return eval(page.getPositionCount(), field1Block, field2Block);
                }
            }
        }

        public BooleanBlock eval(int positionCount, BytesRefBlock field1Block, BytesRefBlock field2Block) {
            try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    MvContains.process(result, p, field1Block, field2Block);
                }
                return result.build();
            }
        }

        @Override
        public String toString() {
            return "MvContainsBytesRefEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field1, field2);
        }

        @Override
        public long baseRamBytesUsed() {
            long baseRamBytesUsed = BASE_RAM_BYTES_USED;
            baseRamBytesUsed += field1.baseRamBytesUsed();
            baseRamBytesUsed += field2.baseRamBytesUsed();
            return baseRamBytesUsed;
        }

        public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
            private final Source source;
            private final EvalOperator.ExpressionEvaluator.Factory field1;
            private final EvalOperator.ExpressionEvaluator.Factory field2;

            public Factory(
                Source source,
                EvalOperator.ExpressionEvaluator.Factory field1,
                EvalOperator.ExpressionEvaluator.Factory field2
            ) {
                this.source = source;
                this.field1 = field1;
                this.field2 = field2;
            }

            @Override
            public MvContainsBytesRefEvaluator get(DriverContext context) {
                return new MvContainsBytesRefEvaluator(field1.get(context), field2.get(context), context);
            }

            @Override
            public String toString() {
                return "MvContainsBytesRefEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
            }
        }
    }

    /**
     *  Currently {@code EvaluatorImplementer} generates:
     *          if (allBlocksAreNulls) {
     *           result.appendNull();
     *           continue position;
     *         }
     *  when all params are null, this violates our contract of always returning a boolean.
     *  It should probably also generate the warnings method conditionally - omitted here.
     *  TODO extend code generation to handle this case
     */
    public static class MvContainsDoubleEvaluator implements EvalOperator.ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvContainsDoubleEvaluator.class);
        private final EvalOperator.ExpressionEvaluator field1;
        private final EvalOperator.ExpressionEvaluator field2;
        private final DriverContext driverContext;

        public MvContainsDoubleEvaluator(
            EvalOperator.ExpressionEvaluator field1,
            EvalOperator.ExpressionEvaluator field2,
            DriverContext driverContext
        ) {
            this.field1 = field1;
            this.field2 = field2;
            this.driverContext = driverContext;
        }

        @Override
        public Block eval(Page page) {
            try (DoubleBlock field1Block = (DoubleBlock) field1.eval(page)) {
                try (DoubleBlock field2Block = (DoubleBlock) field2.eval(page)) {
                    return eval(page.getPositionCount(), field1Block, field2Block);
                }
            }
        }

        public BooleanBlock eval(int positionCount, DoubleBlock field1Block, DoubleBlock field2Block) {
            try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    MvContains.process(result, p, field1Block, field2Block);
                }
                return result.build();
            }
        }

        @Override
        public String toString() {
            return "MvContainsDoubleEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field1, field2);
        }

        @Override
        public long baseRamBytesUsed() {
            long baseRamBytesUsed = BASE_RAM_BYTES_USED;
            baseRamBytesUsed += field1.baseRamBytesUsed();
            baseRamBytesUsed += field2.baseRamBytesUsed();
            return baseRamBytesUsed;
        }

        public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
            private final Source source;
            private final EvalOperator.ExpressionEvaluator.Factory field1;
            private final EvalOperator.ExpressionEvaluator.Factory field2;

            public Factory(
                Source source,
                EvalOperator.ExpressionEvaluator.Factory field1,
                EvalOperator.ExpressionEvaluator.Factory field2
            ) {
                this.source = source;
                this.field1 = field1;
                this.field2 = field2;
            }

            @Override
            public MvContainsDoubleEvaluator get(DriverContext context) {
                return new MvContainsDoubleEvaluator(field1.get(context), field2.get(context), context);
            }

            @Override
            public String toString() {
                return "MvContainsDoubleEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
            }
        }
    }

    /**
     *  Currently {@code EvaluatorImplementer} generates:
     *          if (allBlocksAreNulls) {
     *           result.appendNull();
     *           continue position;
     *         }
     *  when all params are null, this violates our contract of always returning a boolean.
     *  It should probably also generate the warnings method conditionally - omitted here.
     *  TODO extend code generation to handle this case
     */
    public static class MvContainsIntEvaluator implements EvalOperator.ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvContainsIntEvaluator.class);
        private final EvalOperator.ExpressionEvaluator field1;
        private final EvalOperator.ExpressionEvaluator field2;
        private final DriverContext driverContext;

        public MvContainsIntEvaluator(
            EvalOperator.ExpressionEvaluator field1,
            EvalOperator.ExpressionEvaluator field2,
            DriverContext driverContext
        ) {
            this.field1 = field1;
            this.field2 = field2;
            this.driverContext = driverContext;
        }

        @Override
        public Block eval(Page page) {
            try (IntBlock field1Block = (IntBlock) field1.eval(page)) {
                try (IntBlock field2Block = (IntBlock) field2.eval(page)) {
                    return eval(page.getPositionCount(), field1Block, field2Block);
                }
            }
        }

        public BooleanBlock eval(int positionCount, IntBlock field1Block, IntBlock field2Block) {
            try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    MvContains.process(result, p, field1Block, field2Block);
                }
                return result.build();
            }
        }

        @Override
        public String toString() {
            return "MvContainsIntEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field1, field2);
        }

        @Override
        public long baseRamBytesUsed() {
            long baseRamBytesUsed = BASE_RAM_BYTES_USED;
            baseRamBytesUsed += field1.baseRamBytesUsed();
            baseRamBytesUsed += field2.baseRamBytesUsed();
            return baseRamBytesUsed;
        }

        public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
            private final Source source;
            private final EvalOperator.ExpressionEvaluator.Factory field1;
            private final EvalOperator.ExpressionEvaluator.Factory field2;

            public Factory(
                Source source,
                EvalOperator.ExpressionEvaluator.Factory field1,
                EvalOperator.ExpressionEvaluator.Factory field2
            ) {
                this.source = source;
                this.field1 = field1;
                this.field2 = field2;
            }

            @Override
            public MvContainsIntEvaluator get(DriverContext context) {
                return new MvContainsIntEvaluator(field1.get(context), field2.get(context), context);
            }

            @Override
            public String toString() {
                return "MvContainsIntEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
            }
        }
    }

    /**
     *  Currently {@code EvaluatorImplementer} generates:
     *          if (allBlocksAreNulls) {
     *           result.appendNull();
     *           continue position;
     *         }
     *  when all params are null, this violates our contract of always returning a boolean.
     *  It should probably also generate the warnings method conditionally - omitted here.
     *  TODO extend code generation to handle this case
     */
    public static class MvContainsLongEvaluator implements EvalOperator.ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvContainsLongEvaluator.class);
        private final EvalOperator.ExpressionEvaluator field1;
        private final EvalOperator.ExpressionEvaluator field2;
        private final DriverContext driverContext;

        public MvContainsLongEvaluator(
            EvalOperator.ExpressionEvaluator field1,
            EvalOperator.ExpressionEvaluator field2,
            DriverContext driverContext
        ) {
            this.field1 = field1;
            this.field2 = field2;
            this.driverContext = driverContext;
        }

        @Override
        public Block eval(Page page) {
            try (LongBlock field1Block = (LongBlock) field1.eval(page)) {
                try (LongBlock field2Block = (LongBlock) field2.eval(page)) {
                    return eval(page.getPositionCount(), field1Block, field2Block);
                }
            }
        }

        public BooleanBlock eval(int positionCount, LongBlock field1Block, LongBlock field2Block) {
            try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    MvContains.process(result, p, field1Block, field2Block);
                }
                return result.build();
            }
        }

        @Override
        public String toString() {
            return "MvContainsLongEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field1, field2);
        }

        @Override
        public long baseRamBytesUsed() {
            long baseRamBytesUsed = BASE_RAM_BYTES_USED;
            baseRamBytesUsed += field1.baseRamBytesUsed();
            baseRamBytesUsed += field2.baseRamBytesUsed();
            return baseRamBytesUsed;
        }

        public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
            private final Source source;
            private final EvalOperator.ExpressionEvaluator.Factory field1;
            private final EvalOperator.ExpressionEvaluator.Factory field2;

            public Factory(
                Source source,
                EvalOperator.ExpressionEvaluator.Factory field1,
                EvalOperator.ExpressionEvaluator.Factory field2
            ) {
                this.source = source;
                this.field1 = field1;
                this.field2 = field2;
            }

            @Override
            public MvContainsLongEvaluator get(DriverContext context) {
                return new MvContainsLongEvaluator(field1.get(context), field2.get(context), context);
            }

            @Override
            public String toString() {
                return "MvContainsLongEvaluator[" + "field1=" + field1 + ", field2=" + field2 + "]";
            }
        }
    }
}
