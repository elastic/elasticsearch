/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongMultiplyExact;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.MUL;

public class Mul extends DenseVectorArithmeticOperation implements BinaryComparisonInversible, NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Mul", Mul::new);
    public static final String OP_NAME = "Mul";

    /**
     * When {@code true}, a non-finite scalar product ({@code NaN}/{@code ±Inf}) is returned as-is instead of being
     * rejected to {@code null}. Set only by the PromQL translation so that e.g. {@code metric * Inf} surfaces
     * {@code Inf} rather than dropping the series; the default is {@code false}, preserving ES|QL's finite-only
     * arithmetic contract. Only the scalar double path honors this flag; the dense_vector path is never reached
     * by PromQL and keeps rejecting non-finite results.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(operator = "*", returnType = { "double", "integer", "long", "unsigned_long", "dense_vector" }, description = """
        Multiply two values together. For numeric fields, if either field is <<esql-multivalued-fields,multivalued>>
        then the result is `null`. For dense_vector operations, both arguments should be dense_vectors. Inequal vector dimensions generate
        null result.
        """)
    public Mul(
        Source source,
        @Param(
            name = "lhs",
            description = "A numeric value or dense_vector",
            type = { "double", "integer", "long", "unsigned_long", "dense_vector" }
        ) Expression left,
        @Param(
            name = "rhs",
            description = "A numeric value or dense_vector",
            type = { "double", "integer", "long", "unsigned_long", "dense_vector" }
        ) Expression right
    ) {
        this(source, left, right, false);
    }

    public Mul(Source source, Expression left, Expression right, boolean allowNonFinite) {
        super(
            source,
            left,
            right,
            MUL,
            MulIntsEvaluator.Factory::new,
            MulLongsEvaluator.Factory::new,
            MulUnsignedLongsEvaluator.Factory::new,
            (s, lhs, rhs) -> new MulDoublesEvaluator.Factory(s, lhs, rhs, allowNonFinite),
            MUL_DENSE_VECTOR_EVALUATOR
        );
        this.allowNonFinite = allowNonFinite;
    }

    private Mul(StreamInput in) throws IOException {
        // Children are serialized by BinaryScalarFunction#writeTo (source, left, right); the non-finite flag, when
        // present, follows them, so read it last to match. Bypassing the base StreamInput constructor lets the flag
        // be supplied to the double-evaluator factory at construction time.
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            NonFiniteSupport.readNonFinite(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeNonFinite(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Div::new;
    }

    @Override
    protected boolean isCommutative() {
        return true;
    }

    @Override
    public Mul swapLeftAndRight() {
        return new Mul(source(), right(), left(), allowNonFinite);
    }

    @Override
    protected NodeInfo<Mul> info() {
        return NodeInfo.create(this, Mul::new, left(), right(), allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new Mul(source(), left(), right(), false);
    }

    @Override
    protected Mul replaceChildren(Expression left, Expression right) {
        return new Mul(source(), left, right, allowNonFinite);
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        return Math.multiplyExact(lhs, rhs);
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        return Math.multiplyExact(lhs, rhs);
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLongs(long lhs, long rhs) {
        return unsignedLongMultiplyExact(lhs, rhs);
    }

    @Evaluator(extraName = "Doubles", warnExceptions = { ArithmeticException.class })
    static double processDoubles(double lhs, double rhs, @Fixed(includeInToString = false) boolean allowNonFinite) {
        double result = lhs * rhs;
        return allowNonFinite ? result : NumericUtils.asFiniteNumber(result);
    }

    private static float mulDenseVectorElements(float lhs, float rhs) {
        return NumericUtils.asFiniteNumber(lhs * rhs);
    }

    private static final DenseVectorBinaryEvaluator MUL_DENSE_VECTOR_EVALUATOR = new DenseVectorBinaryEvaluator() {
        @Override
        public ExpressionEvaluator.Factory vectorsOperation(
            Source source,
            ExpressionEvaluator.Factory lhs,
            ExpressionEvaluator.Factory rhs
        ) {
            return new DenseVectorsEvaluator.Factory(source, lhs, rhs, Mul::mulDenseVectorElements, OP_NAME);
        }

        @Override
        public ExpressionEvaluator.Factory scalarVectorOperation(Source source, float lhs, ExpressionEvaluator.Factory rhs) {
            return new DenseVectorScalarEvaluator.Factory(source, lhs, rhs, Mul::mulDenseVectorElements, OP_NAME);
        }

        @Override
        public ExpressionEvaluator.Factory vectorScalarOperation(Source source, ExpressionEvaluator.Factory lhs, float rhs) {
            return new DenseVectorScalarEvaluator.Factory(source, lhs, rhs, Mul::mulDenseVectorElements, OP_NAME);
        }
    };
}
