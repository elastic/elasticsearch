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
import org.elasticsearch.xpack.sql.expression.function.scalar.whitelist.InternalSqlScriptUtils;
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
        return new ScriptTemplate(processScript("{}"),
                paramsBuilder().variable(foldable.fold()).build(),
                dataType());
    }

    default ScriptTemplate scriptWithScalar(ScalarFunctionAttribute scalar) {
        ScriptTemplate nested = scalar.script();
        return new ScriptTemplate(processScript(nested.template()),
                paramsBuilder().script(nested.params()).build(),
                dataType());
    }

    default ScriptTemplate scriptWithAggregate(AggregateFunctionAttribute aggregate) {
        return new ScriptTemplate(processScript("{}"),
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
        return template.replace("{sql}", InternalSqlScriptUtils.class.getSimpleName()).replace("{}", "params.%s");
    }
}