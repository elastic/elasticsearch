/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Internal, non-user-callable scalar rendering a sample value as the label text of PromQL {@code count_values}. Meant to
 * be applied once per output group, after grouping by the double value, rather than once per input row.
 */
public final class FloatToLabelValue extends EsqlScalarFunction implements AnyNullIsNull, VersionedNamedWriteable {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FloatToLabelValue",
        FloatToLabelValue::new
    );

    /**
     * Transport version gating the PromQL {@code count_values} internal scalar {@link FloatToLabelValue}.
     */
    public static final TransportVersion PROMQL_COUNT_VALUES = TransportVersion.fromName("esql_promql_count_values");

    private final Expression value;

    public FloatToLabelValue(Source source, Expression value) {
        super(source, List.of(value));
        this.value = value;
    }

    private FloatToLabelValue(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(value);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return PROMQL_COUNT_VALUES;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public boolean foldable() {
        return value.foldable();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(value, t -> t == DataType.DOUBLE, sourceText(), DEFAULT, "double");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FloatToLabelValue(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FloatToLabelValue::new, value);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new FloatToLabelValueEvaluator.Factory(source(), toEvaluator.apply(value));
    }

    @Evaluator
    static BytesRef process(double value) {
        return new BytesRef(format(value));
    }

    /**
     * Renders {@code d} exactly like Go's {@code strconv.FormatFloat(d, 'f', -1, 64)}, the label text Prometheus'
     * {@code count_values} produces: the shortest digits that round-trip, in plain notation (never an exponent),
     * {@code NaN}, {@code +Inf}, {@code -Inf}, and {@code -0} distinct from {@code 0}.
     * <p>
     * The digits come from {@link Double#toString(double)}, which also selects the shortest round-tripping decimal,
     * with one difference: when a single digit would suffice it may still pick a closer two-digit decimal. That can
     * only happen for subnormals, whose rounding interval is wide enough to contain several one-digit decimals, so for
     * them a two-digit result is replaced by the closest one-digit decimal when that round-trips, since Go always emits
     * the shortest digits.
     */
    static String format(double d) {
        if (Double.isNaN(d)) {
            return "NaN";
        }
        if (Double.isInfinite(d)) {
            return d > 0 ? "+Inf" : "-Inf";
        }
        if (d == 0) {
            return Double.doubleToRawLongBits(d) < 0 ? "-0" : "0";
        }
        double abs = Math.abs(d);
        String shortest = Double.toString(abs);
        String digits;
        int pointPos; // number of digits before the decimal point; may be negative or exceed digits.length()
        int e = shortest.indexOf('E');
        if (e >= 0) {
            // d.dddE[-]n
            digits = shortest.charAt(0) + shortest.substring(2, e);
            pointPos = 1 + Integer.parseInt(shortest.substring(e + 1));
        } else {
            // iii.fff
            int dot = shortest.indexOf('.');
            digits = shortest.substring(0, dot) + shortest.substring(dot + 1);
            pointPos = dot;
        }
        int lead = 0;
        while (digits.charAt(lead) == '0') {
            lead++;
        }
        int end = digits.length();
        while (digits.charAt(end - 1) == '0') {
            end--;
        }
        digits = digits.substring(lead, end);
        pointPos -= lead;

        if (digits.length() == 2 && abs < Double.MIN_NORMAL) {
            BigDecimal oneDigit = new BigDecimal(abs).round(new MathContext(1, RoundingMode.HALF_EVEN));
            if (oneDigit.doubleValue() == abs) {
                digits = oneDigit.unscaledValue().toString();
                pointPos = 1 - oneDigit.scale();
            }
        }

        StringBuilder sb = new StringBuilder(digits.length() + Math.absExact(pointPos) + 3);
        if (d < 0) {
            sb.append('-');
        }
        if (pointPos <= 0) {
            sb.append("0.").repeat('0', -pointPos).append(digits);
        } else if (pointPos >= digits.length()) {
            sb.append(digits).repeat('0', pointPos - digits.length());
        } else {
            sb.append(digits, 0, pointPos).append('.').append(digits, pointPos, digits.length());
        }
        return sb.toString();
    }
}
