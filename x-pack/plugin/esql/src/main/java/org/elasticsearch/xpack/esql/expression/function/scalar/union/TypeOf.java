/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.union;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.UnionBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionEsField;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * The type each value actually has, as a string. For a union column that is the leg the value's own index mapped it
 * into, which is the only way to tell two values of one column apart without converting either; for every other column
 * it is that column's one type, so the function is total and needs no refusal of its own.
 * <p>
 * Answering per value rather than per row is deliberate: a union position can hold values from more than one leg, so a
 * per-row answer would have to pick one of them. The names are the mapping ones, matching the column's own
 * {@code original_types}, so grouping on this and reading that list speak the same vocabulary.
 * </p>
 */
public class TypeOf extends UnaryScalarFunction implements AnyNullIsNull {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "TypeOf", TypeOf::new);

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TypeOf.class).unary(TypeOf::new).name("typeof");

    @FunctionInfo(
        returnType = "keyword",
        briefSummary = "Returns the type of each value, as a string.",
        description = "Returns the name of the type each value has, as its index mapped it. For a column whose indices "
            + "agree on one type every row answers the same; for one they disagree about, each row answers with its own "
            + "index's type. The names are the ones the mapping uses, so they match the column's `original_types`.",
        examples = @Example(file = "union_types", tag = "typeof")
    )
    public TypeOf(
        Source source,
        @Param(
            name = "field",
            // Every type UnionEsField#isLegType admits, which is the domain resolveType enforces. The two lists are
            // checked against each other by the function test harness, so a leg type missing here is a failure.
            type = {
                "boolean",
                "date",
                "date_nanos",
                "double",
                "flattened",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "Column to report the type of."
        ) Expression field
    ) {
        super(source, field);
    }

    private TypeOf(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    /**
     * Exactly the types a union leg can hold, which is the question this function exists to answer - asking
     * {@link UnionEsField#isLegType} rather than repeating its list, so widening what a union may carry widens this
     * with it and the two cannot drift. A union itself is in, being what a column of several of those types is.
     */
    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(
            field(),
            dt -> dt == DataType.UNION || UnionEsField.isLegType(dt),
            sourceText(),
            DEFAULT,
            "any type a column of more than one type can hold"
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TypeOf(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TypeOf::new, field());
    }

    /**
     * The leg names, read off the field the attribute carries. They are available here and not at resolution because
     * a type conflict only becomes a {@link UnionEsField} at the end of analysis, and the analyzer rewrites the
     * attribute inside every expression when it does - so by the time an evaluator is built the names are on the child.
     */
    private List<String> legNames() {
        if (field() instanceof FieldAttribute attribute && attribute.field() instanceof UnionEsField union) {
            return union.legTypes().stream().map(DataType::esType).toList();
        }
        return null;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(field());
        List<String> legNames = legNames();
        String oneType = legNames == null ? field().dataType().esType() : null;
        return new ExpressionEvaluator.Factory() {
            @Override
            public String toString() {
                return describe(fieldEvaluator, legNames, oneType);
            }

            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new Evaluator(fieldEvaluator.get(context), legNames, oneType, context.blockFactory());
            }
        };
    }

    /** One spelling for the factory and the evaluator, which a test pins to a single string. */
    private static String describe(Object field, List<String> legNames, String oneType) {
        return "TypeOfEvaluator[field=" + field + ",legs=" + legNames + ",type=" + oneType + "]";
    }

    private static final class Evaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final ExpressionEvaluator eval;
        private final BytesRef[] legNames;
        private final BytesRef oneType;
        private final BlockFactory blockFactory;
        /** The names as text, because a BytesRef prints as hex and this string reaches a user through a profile. */
        private final String description;

        Evaluator(ExpressionEvaluator eval, List<String> legNames, String oneType, BlockFactory blockFactory) {
            this.eval = eval;
            this.legNames = legNames == null ? null : legNames.stream().map(BytesRef::new).toArray(BytesRef[]::new);
            this.oneType = oneType == null ? null : new BytesRef(oneType);
            this.blockFactory = blockFactory;
            this.description = describe(eval, legNames, oneType);
        }

        @Override
        public Block eval(Page page) {
            try (Block block = eval.eval(page)) {
                int positionCount = block.getPositionCount();
                // A union column is a ConstantNullBlock on a page no index supplied the field for, and a column of any
                // type can be one; either way every value is absent rather than of some type.
                if (block.areAllValuesNull()) {
                    return blockFactory.newConstantNullBlock(positionCount);
                }
                boolean union = legNames != null && block instanceof UnionBlock;
                try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        int valueCount = block.getValueCount(p);
                        if (valueCount == 0) {
                            builder.appendNull();
                            continue;
                        }
                        if (valueCount > 1) {
                            builder.beginPositionEntry();
                        }
                        int first = block.getFirstValueIndex(p);
                        for (int v = 0; v < valueCount; v++) {
                            builder.appendBytesRef(union ? nameOf((UnionBlock) block, first + v) : oneType);
                        }
                        if (valueCount > 1) {
                            builder.endPositionEntry();
                        }
                    }
                    return builder.build();
                }
            }
        }

        /**
         * A tag outside the declared legs is not reachable from a block this cluster built, since the loader refuses a
         * leg it has no room for; it would mean the block and the plan disagreed about the leg list, which is worth a
         * clear failure rather than a name invented here.
         */
        private BytesRef nameOf(UnionBlock block, int valueIndex) {
            int leg = block.tag(valueIndex);
            if (leg < 0 || leg >= legNames.length) {
                throw new IllegalStateException("value is tagged leg [" + leg + "] but only [" + legNames.length + "] are declared");
            }
            return legNames[leg];
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + eval.baseRamBytesUsed();
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(eval);
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
