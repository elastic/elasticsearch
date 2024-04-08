/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.Params;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogicPipe;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogicProcessor.BinaryLogicOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonPipe;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.DateUtils;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

// BETWEEN or range - is a mix of gt(e) AND lt(e)
public class Range extends ScalarFunction {

    private final Expression value, lower, upper;
    private final boolean includeLower, includeUpper;
    private final ZoneId zoneId;

    public Range(Source src, Expression value, Expression lower, boolean inclLower, Expression upper, boolean inclUpper, ZoneId zoneId) {
        super(src, asList(value, lower, upper));

        this.value = value;
        this.lower = lower;
        this.upper = upper;
        this.includeLower = inclLower;
        this.includeUpper = inclUpper;
        this.zoneId = zoneId;
    }

    @Override
    protected NodeInfo<Range> info() {
        return NodeInfo.create(this, Range::new, value, lower, includeLower, upper, includeUpper, zoneId);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Range(source(), newChildren.get(0), newChildren.get(1), includeLower, newChildren.get(2), includeUpper, zoneId);
    }

    public Expression value() {
        return value;
    }

    public Expression lower() {
        return lower;
    }

    public Expression upper() {
        return upper;
    }

    public boolean includeLower() {
        return includeLower;
    }

    public boolean includeUpper() {
        return includeUpper;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public boolean foldable() {
        if (lower.foldable() && upper.foldable()) {
            return areBoundariesInvalid() || value.foldable();
        }

        return false;
    }

    @Override
    public Object fold() {
        if (areBoundariesInvalid()) {
            return Boolean.FALSE;
        }

        Object val = value.fold();
        Integer lowerCompare = BinaryComparison.compare(lower.fold(), val);
        Integer upperCompare = BinaryComparison.compare(val, upper().fold());
        boolean lowerComparsion = lowerCompare == null ? false : (includeLower ? lowerCompare <= 0 : lowerCompare < 0);
        boolean upperComparsion = upperCompare == null ? false : (includeUpper ? upperCompare <= 0 : upperCompare < 0);
        return lowerComparsion && upperComparsion;
    }

    /**
     * Check whether the boundaries are invalid ( upper &lt; lower) or not.
     * If they are, the value does not have to be evaluated.
     */
    protected boolean areBoundariesInvalid() {
        Object lowerValue = lower.fold();
        Object upperValue = upper.fold();
        if (DataTypes.isDateTime(value.dataType()) || DataTypes.isDateTime(lower.dataType()) || DataTypes.isDateTime(upper.dataType())) {
            try {
                if (upperValue instanceof String upperString) {
                    upperValue = DateUtils.asDateTime(upperString);
                }
                if (lowerValue instanceof String lowerString) {
                    lowerValue = DateUtils.asDateTime(lowerString);
                }
            } catch (DateTimeException e) {
                // one of the patterns is not a normal date, it could be a date math expression
                // that has to be evaluated at lower level.
                return false;
            }
            // for all the other cases, normal BinaryComparison logic is sufficient
        }

        Integer compare = BinaryComparison.compare(lowerValue, upperValue);
        // upper < lower OR upper == lower and the range doesn't contain any equals
        return compare != null && (compare > 0 || (compare == 0 && (includeLower == false || includeUpper == false)));
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate valueScript = asScript(value);
        ScriptTemplate lowerScript = asScript(lower);
        ScriptTemplate upperScript = asScript(upper);

        String template = formatTemplate(
            format(
                Locale.ROOT,
                "{ql}.and({ql}.%s(%s, %s), {ql}.%s(%s, %s))",
                includeLower() ? "gte" : "gt",
                valueScript.template(),
                lowerScript.template(),
                includeUpper() ? "lte" : "lt",
                valueScript.template(),
                upperScript.template()
            )
        );

        Params params = paramsBuilder().script(valueScript.params())
            .script(lowerScript.params())
            .script(valueScript.params())
            .script(upperScript.params())
            .build();

        return new ScriptTemplate(template, params, DataTypes.BOOLEAN);
    }

    @Override
    protected Pipe makePipe() {
        BinaryComparisonPipe lowerPipe = new BinaryComparisonPipe(
            source(),
            this,
            Expressions.pipe(value()),
            Expressions.pipe(lower()),
            includeLower() ? BinaryComparisonOperation.GTE : BinaryComparisonOperation.GT
        );
        BinaryComparisonPipe upperPipe = new BinaryComparisonPipe(
            source(),
            this,
            Expressions.pipe(value()),
            Expressions.pipe(upper()),
            includeUpper() ? BinaryComparisonOperation.LTE : BinaryComparisonOperation.LT
        );
        BinaryLogicPipe and = new BinaryLogicPipe(source(), this, lowerPipe, upperPipe, BinaryLogicOperation.AND);
        return and;
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeLower, includeUpper, value, lower, upper, zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Range other = (Range) obj;
        return Objects.equals(includeLower, other.includeLower)
            && Objects.equals(includeUpper, other.includeUpper)
            && Objects.equals(value, other.value)
            && Objects.equals(lower, other.lower)
            && Objects.equals(upper, other.upper)
            && Objects.equals(zoneId, other.zoneId);
    }
}
