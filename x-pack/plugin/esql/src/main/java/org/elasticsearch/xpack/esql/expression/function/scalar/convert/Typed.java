/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Selects the values of a field that its source mapped as one given type, null elsewhere. Performs no
 * conversion and never inspects a value: the type comes from the mapping, which {@code ResolveUnionTypes}
 * pins per leg.
 */
public class Typed extends EsqlScalarFunction implements ConvertFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Typed", Typed::new);

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Typed.class).binary(Typed::new).name("typed");

    private final Expression field;
    /** Non-child: {@code UnionTypeEsField.rewrapWithCast} calls {@code replaceChildren} with the field alone. */
    private final Expression typeLiteral;

    @FunctionInfo(
        returnType = { "boolean", "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "unsigned_long", "version" },
        briefSummary = "Selects the values of a field that already have the given type.",
        description = "Returns the values of `field` whose source mapped them as `type`, and null for every "
            + "other row. Performs no conversion: a value is included because the index that supplied it "
            + "mapped the field as that type, not because the value looks like one."
    )
    public Typed(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "unsigned_long", "version" },
            description = "Field to select values from."
        ) Expression field,
        @Param(name = "type", type = "keyword", description = "Name of the type to select, as a string literal.") Expression typeLiteral
    ) {
        // Only the field is a child; see typeLiteral.
        super(source, List.of(field));
        this.field = field;
        this.typeLiteral = typeLiteral;
    }

    private Typed(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(typeLiteral);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Expression field() {
        return field;
    }

    /** Every type: {@code ResolveUnionTypes} refuses the whole query unless an entry is built per mapped type. */
    @Override
    public Set<DataType> supportedTypes() {
        return Set.copyOf(DataType.types());
    }

    @Override
    public DataType dataType() {
        DataType target = targetType();
        return target == null ? DataType.UNSUPPORTED : target;
    }

    /** Never, even when the types match: {@code rewrapWithCast} would otherwise hand back every row. */
    @Override
    public boolean isNoop() {
        return false;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Typed(source(), newChildren.get(0), typeLiteral);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Typed::new, field, typeLiteral);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        if (typeLiteral.foldable() == false) {
            return new TypeResolution(
                "second argument of [" + sourceText() + "] must be a constant, received [" + typeLiteral.sourceText() + "]"
            );
        }
        if (targetType() == null) {
            return new TypeResolution("second argument of [" + sourceText() + "] must be a known type name");
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(field);
        DataType target = targetType();
        boolean matches = sameType(field.dataType(), target);
        return new ExpressionEvaluator.Factory() {
            @Override
            public String toString() {
                return "TypedEvaluator[field=" + fieldEvaluator + ",type=" + target + ",matches=" + matches + "]";
            }

            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new Evaluator(fieldEvaluator.get(context), matches, context.blockFactory());
            }
        };
    }

    private static final class Evaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final ExpressionEvaluator eval;
        private final boolean matches;
        private final BlockFactory blockFactory;

        Evaluator(ExpressionEvaluator eval, boolean matches, BlockFactory blockFactory) {
            this.eval = eval;
            this.matches = matches;
            this.blockFactory = blockFactory;
        }

        @Override
        public Block eval(Page page) {
            if (matches) {
                // Ownership of the child's block passes to our caller.
                return eval.eval(page);
            }
            try (Block ignored = eval.eval(page)) {
                return blockFactory.newConstantNullBlock(page.getPositionCount());
            }
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
            return "TypedEvaluator[matches=" + matches + "]";
        }
    }

    /**
     * The type is a non-child, and {@code Function} equality compares only the class and the children, so without this
     * {@code TYPED(x, "integer")} equals {@code TYPED(x, "long")}. Optimizer rules key maps on expression identity, so
     * two such calls collapse into one and the second binds to the wrong leg.
     */
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(typeLiteral, ((Typed) obj).typeLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), typeLiteral);
    }

    /** Widened: a leg is pinned to its raw mapped type, so a {@code short} leg loads as an int block. */
    static boolean sameType(DataType a, DataType b) {
        if (a == null || b == null) {
            return false;
        }
        return a.widenSmallNumeric() == b.widenSmallNumeric();
    }

    private DataType targetType() {
        if (typeLiteral.foldable() == false) {
            return null;
        }
        // A string literal folds to a BytesRef, so toString() on it is not the type name.
        String name = BytesRefs.toString(typeLiteral.fold(FoldContext.small()));
        return name == null ? null : DataType.fromTypeName(name);
    }
}
