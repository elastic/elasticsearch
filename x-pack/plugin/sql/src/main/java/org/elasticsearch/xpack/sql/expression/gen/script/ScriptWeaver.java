/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.gen.script;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.grouping.GroupingFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoShape;
import org.elasticsearch.xpack.sql.expression.literal.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.IntervalYearMonth;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.OffsetTime;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Mixin-like interface for customizing the default script generation.
 */
public interface ScriptWeaver {

    default ScriptTemplate asScript(Expression exp) {
        if (exp.foldable()) {
            return scriptWithFoldable(exp);
        }

        Attribute attr = Expressions.attribute(exp);
        if (attr != null) {
            if (attr instanceof ScalarFunctionAttribute) {
                return scriptWithScalar((ScalarFunctionAttribute) attr);
            }
            if (attr instanceof AggregateFunctionAttribute) {
                return scriptWithAggregate((AggregateFunctionAttribute) attr);
            }
            if (attr instanceof GroupingFunctionAttribute) {
                return scriptWithGrouping((GroupingFunctionAttribute) attr);
            }
            if (attr instanceof FieldAttribute) {
                return scriptWithField((FieldAttribute) attr);
            }
        }
        throw new SqlIllegalArgumentException("Cannot evaluate script for expression {}", exp);
    }

    /*
     * To be used when the function has an optional parameter.
     */
    default ScriptTemplate asOptionalScript(Expression exp) {
        return exp == null ? asScript(Literal.NULL) : asScript(exp);
    }

    DataType dataType();

    default ScriptTemplate scriptWithFoldable(Expression foldable) {
        Object fold = foldable.fold();

        //
        // Custom type handling
        //

        // wrap intervals with dedicated methods for serialization
        if (fold instanceof ZonedDateTime) {
            ZonedDateTime zdt = (ZonedDateTime) fold;
            return new ScriptTemplate(processScript("{sql}.asDateTime({})"),
                    paramsBuilder().variable(DateUtils.toString(zdt)).build(), dataType());
        }

        if (fold instanceof IntervalYearMonth) {
            IntervalYearMonth iym = (IntervalYearMonth) fold;
            return new ScriptTemplate(processScript("{sql}.intervalYearMonth({},{})"),
                    paramsBuilder().variable(iym.interval().toString()).variable(iym.dataType().name()).build(),
                    dataType());
        }
        if (fold instanceof IntervalDayTime) {
            IntervalDayTime idt = (IntervalDayTime) fold;
            return new ScriptTemplate(processScript("{sql}.intervalDayTime({},{})"),
                    paramsBuilder().variable(idt.interval().toString()).variable(idt.dataType().name()).build(),
                    dataType());
        }
        if (fold instanceof OffsetTime) {
            OffsetTime ot = (OffsetTime) fold;
            return new ScriptTemplate(processScript("{sql}.asTime({})"),
                    paramsBuilder().variable(ot.toString()).build(),
                    dataType());
        }

        if (fold instanceof GeoShape) {
            GeoShape geoShape = (GeoShape) fold;
            return new ScriptTemplate(processScript("{sql}.stWktToSql({})"),
                paramsBuilder().variable(geoShape.toString()).build(),
                dataType());
        }

        return new ScriptTemplate(processScript("{}"),
                paramsBuilder().variable(fold).build(),
                dataType());
    }

    default ScriptTemplate scriptWithScalar(ScalarFunctionAttribute scalar) {
        ScriptTemplate nested = scalar.script();
        return new ScriptTemplate(processScript(nested.template()),
                paramsBuilder().script(nested.params()).build(),
                dataType());
    }

    default ScriptTemplate scriptWithAggregate(AggregateFunctionAttribute aggregate) {
        String template = "{}";
        if (aggregate.dataType().isDateBased()) {
            template = "{sql}.asDateTime({})";
        }
        return new ScriptTemplate(processScript(template),
                paramsBuilder().agg(aggregate).build(),
                dataType());
    }

    default ScriptTemplate scriptWithGrouping(GroupingFunctionAttribute grouping) {
        String template = "{}";
        if (grouping.dataType().isDateBased()) {
            template = "{sql}.asDateTime({})";
        }
        return new ScriptTemplate(processScript(template),
                paramsBuilder().grouping(grouping).build(),
                dataType());
    }
    
    default ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(processScript("doc[{}].value"),
                paramsBuilder().variable(field.name()).build(),
                dataType());
    }

    default String processScript(String script) {
        return formatTemplate(script);
    }

    default String formatTemplate(String template) {
        return Scripts.formatTemplate(template);
    }
}
