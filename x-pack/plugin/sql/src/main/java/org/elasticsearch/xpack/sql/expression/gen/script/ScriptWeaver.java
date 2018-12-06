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
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.literal.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.IntervalYearMonth;
import org.elasticsearch.xpack.sql.type.DataType;

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
            if (attr instanceof FieldAttribute) {
                return scriptWithField((FieldAttribute) attr);
            }
        }
        throw new SqlIllegalArgumentException("Cannot evaluate script for expression {}", exp);
    }

    DataType dataType();

    default ScriptTemplate scriptWithFoldable(Expression foldable) {
        Object fold = foldable.fold();
        // wrap intervals with dedicated methods for serialization
        if (fold instanceof IntervalYearMonth) {
            IntervalYearMonth iym = (IntervalYearMonth) fold;
            return new ScriptTemplate(processScript("{sql}.intervalYearMonth({},{})"),
                    paramsBuilder().variable(iym.interval().toString()).variable(iym.dataType().name()).build(),
                    dataType());
        } else if (fold instanceof IntervalDayTime) {
            IntervalDayTime idt = (IntervalDayTime) fold;
            return new ScriptTemplate(processScript("{sql}.intervalDayTime({},{})"),
                    paramsBuilder().variable(idt.interval().toString()).variable(idt.dataType().name()).build(),
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
        if (aggregate.dataType() == DataType.DATE) {
            template = "{sql}.asDateTime({})";
        }
        return new ScriptTemplate(processScript(template),
                paramsBuilder().agg(aggregate).build(),
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