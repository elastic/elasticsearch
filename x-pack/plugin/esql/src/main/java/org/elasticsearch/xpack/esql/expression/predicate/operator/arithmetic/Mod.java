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
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.MOD;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToUnsignedLong;

public class Mod extends EsqlArithmeticOperation implements NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Mod", Mod::new);

    /**
     * When {@code true}, the scalar double remainder follows IEEE-754: {@code x % 0} yields {@code NaN} (and other
     * non-finite results are returned as-is) instead of being rejected to {@code null}. Set only by the PromQL
     * translation, where Prometheus defines {@code %} via {@code math.Mod} ({@code x % 0 == NaN}, series kept). The
     * default is {@code false}, preserving ES|QL's divide-by-zero error. Only the scalar double path honors this flag.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(
        operator = "%",
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Divide one number by another and return the remainder. "
            + "If either field is <<esql-multivalued-fields,multivalued>> then the result is `null`."
    )
    public Mod(
        Source source,
        @Param(name = "lhs", description = "A numeric value.", type = { "double", "integer", "long", "unsigned_long" }) Expression left,
        @Param(name = "rhs", description = "A numeric value.", type = { "double", "integer", "long", "unsigned_long" }) Expression right
    ) {
        this(source, left, right, false);
    }

    public Mod(Source source, Expression left, Expression right, boolean allowNonFinite) {
        super(
            source,
            left,
            right,
            MOD,
            ModIntsEvaluator.Factory::new,
            ModLongsEvaluator.Factory::new,
            ModUnsignedLongsEvaluator.Factory::new,
            (s, lhs, rhs) -> new ModDoublesEvaluator.Factory(s, lhs, rhs, allowNonFinite),
            // The lenient (PromQL) path disables the constant-RHS fast path so all double remainder flows through the
            // binary evaluator above, which surfaces NaN (including x%0). The strict path keeps the fast path.
            allowNonFinite ? null : ModIntsByConstantEvaluator.Factory::new,
            allowNonFinite ? null : ModLongsByConstantEvaluator.Factory::new,
            allowNonFinite ? null : ModDoublesByConstantEvaluator.Factory::new,
            /* excludeZeroRhs */ true
        );
        this.allowNonFinite = allowNonFinite;
    }

    private Mod(StreamInput in) throws IOException {
        // Children are serialized by BinaryScalarFunction#writeTo (source, left, right); the non-finite flag, when
        // present, follows them, so read it last to match. Bypassing the base StreamInput constructor lets the flag
        // reach the double-evaluator factories.
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
    protected NodeInfo<Mod> info() {
        return NodeInfo.create(this, Mod::new, left(), right(), allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new Mod(source(), left(), right(), false);
    }

    @Override
    protected Mod replaceChildren(Expression left, Expression right) {
        return new Mod(source(), left, right, allowNonFinite);
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        if (rhs == 0) {
            throw new ArithmeticException("/ by zero");
        }
        return lhs % rhs;
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        if (rhs == 0L) {
            throw new ArithmeticException("/ by zero");
        }
        return lhs % rhs;
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLongs(long lhs, long rhs) {
        if (rhs == NumericUtils.ZERO_AS_UNSIGNED_LONG) {
            throw new ArithmeticException("/ by zero");
        }
        return longToUnsignedLong(Long.remainderUnsigned(longToUnsignedLong(lhs, true), longToUnsignedLong(rhs, true)), true);
    }

    @Evaluator(extraName = "Doubles", warnExceptions = { ArithmeticException.class })
    static double processDoubles(double lhs, double rhs, @Fixed(includeInToString = false) boolean allowNonFinite) {
        double value = lhs % rhs;
        if (allowNonFinite == false && (Double.isNaN(value) || Double.isInfinite(value))) {
            // Prometheus defines x % 0 as NaN (via math.Mod) with the series kept; the strict path rejects it instead.
            throw new ArithmeticException("/ by zero");
        }
        return value;
    }

    @Evaluator(extraName = "IntsByConstant")
    static int processIntsByConstant(int lhs, @Fixed(jitConstant = true) int rhs) {
        return lhs % rhs;
    }

    @Evaluator(extraName = "LongsByConstant")
    static long processLongsByConstant(long lhs, @Fixed(jitConstant = true) long rhs) {
        return lhs % rhs;
    }

    @Evaluator(extraName = "DoublesByConstant", warnExceptions = { ArithmeticException.class })
    static double processDoublesByConstant(double lhs, @Fixed(jitConstant = true) double rhs) {
        double value = lhs % rhs;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new ArithmeticException("/ by zero");
        }
        return value;
    }
}
