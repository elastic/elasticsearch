/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.function.scalar;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.Params;
import org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.DateUtils;

import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.ql.expression.gen.script.Scripts.PARAM;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

/**
 * A {@code ScalarFunction} is a {@code Function} that takes values from some
 * operation and converts each to another value. An example would be
 * {@code ABS()}, which takes one value at a time, applies a function to the
 * value (abs) and returns a new value.
 */
public abstract class ScalarFunction extends Function {

    protected ScalarFunction(Source source) {
        super(source, emptyList());
    }

    protected ScalarFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }

    //
    // Script generation
    //
    public ScriptTemplate asScript(Expression exp) {
        if (exp.foldable()) {
            return scriptWithFoldable(exp);
        }

        if (exp instanceof FieldAttribute) {
            return scriptWithField((FieldAttribute) exp);
        }

        if (exp instanceof ScalarFunction) {
            return scriptWithScalar((ScalarFunction) exp);
        }

        if (exp instanceof AggregateFunction) {
            return scriptWithAggregate((AggregateFunction) exp);
        }

        if (exp instanceof GroupingFunction) {
            return scriptWithGrouping((GroupingFunction) exp);
        }
        throw new QlIllegalArgumentException("Cannot evaluate script for expression {}", exp);
    }

    protected ScriptTemplate scriptWithFoldable(Expression foldable) {
        Object fold = foldable.fold();

        // FIXME: this needs to be refactored
        //
        // Custom type handling
        //

        // wrap intervals with dedicated methods for serialization
        if (fold instanceof ZonedDateTime zdt) {
            return new ScriptTemplate(
                processScript("{sql}.asDateTime({})"),
                paramsBuilder().variable(DateUtils.toString(zdt)).build(),
                dataType()
            );
        }

        if (fold instanceof IntervalScripting is) {
            return new ScriptTemplate(
                processScript(is.script()),
                paramsBuilder().variable(is.value()).variable(is.typeName()).build(),
                dataType()
            );
        }

        if (fold instanceof OffsetTime ot) {
            return new ScriptTemplate(processScript("{sql}.asTime({})"), paramsBuilder().variable(ot.toString()).build(), dataType());
        }

        if (fold != null && fold.getClass().getSimpleName().equals("GeoShape")) {
            return new ScriptTemplate(processScript("{sql}.stWktToSql({})"), paramsBuilder().variable(fold.toString()).build(), dataType());
        }

        return new ScriptTemplate(processScript("{}"), paramsBuilder().variable(fold).build(), dataType());
    }

    protected ScriptTemplate scriptWithScalar(ScalarFunction scalar) {
        ScriptTemplate nested = scalar.asScript();
        return new ScriptTemplate(processScript(nested.template()), paramsBuilder().script(nested.params()).build(), dataType());
    }

    protected ScriptTemplate scriptWithAggregate(AggregateFunction aggregate) {
        String template = PARAM;
        ParamsBuilder paramsBuilder = paramsBuilder().agg(aggregate);

        DataType nullSafeCastDataType = null;
        DataType dataType = aggregate.dataType();
        if (dataType.name().equals("DATE") || dataType == DATETIME ||
        // Aggregations on date_nanos are returned as string
            aggregate.field().dataType() == DATETIME) {

            template = "{sql}.asDateTime({})";
        } else if (dataType.isInteger()) {
            // MAX, MIN need to retain field's data type, so that possible operations on integral types (like division) work
            // correctly -> perform a cast in the aggs filtering script, the bucket selector for HAVING.
            // SQL function classes not available in QL: filter by name
            String fn = aggregate.functionName();
            if ("MAX".equals(fn) || "MIN".equals(fn)) {
                nullSafeCastDataType = dataType;
            } else if ("SUM".equals(fn)) {
                // SUM(integral_type) requires returning a LONG value
                nullSafeCastDataType = LONG;
            }
        }
        if (nullSafeCastDataType != null) {
            template = "{ql}.nullSafeCastNumeric({},{})";
            paramsBuilder.variable(nullSafeCastDataType.name());
        }
        return new ScriptTemplate(processScript(template), paramsBuilder.build(), dataType());
    }

    // This method isn't actually used at the moment, since there is no grouping function (ie HISTOGRAM)
    // that currently results in a script being generated
    protected ScriptTemplate scriptWithGrouping(GroupingFunction grouping) {
        String template = PARAM;
        return new ScriptTemplate(processScript(template), paramsBuilder().grouping(grouping).build(), dataType());
    }

    protected ScriptTemplate scriptWithField(FieldAttribute field) {
        Params params = paramsBuilder().variable(field.exactAttribute().name()).build();
        // unsigned_long fields get returned in scripts as plain longs, so a conversion is required
        return field.dataType() != UNSIGNED_LONG
            ? new ScriptTemplate(processScript(Scripts.DOC_VALUE), params, dataType())
            : new ScriptTemplate(
                processScript(format("{ql}.", "nullSafeCastToUnsignedLong({})", Scripts.DOC_VALUE)),
                params,
                UNSIGNED_LONG
            );
    }

    protected String processScript(String script) {
        return formatTemplate(script);
    }

    protected String formatTemplate(String template) {
        return Scripts.formatTemplate(template);
    }
}
