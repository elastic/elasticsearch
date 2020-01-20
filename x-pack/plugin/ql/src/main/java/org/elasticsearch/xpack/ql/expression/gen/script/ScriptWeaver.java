/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.gen.script;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Mixin-like interface for customizing the default script generation.
 */
public interface ScriptWeaver {

    DataType dataType();

    default ScriptTemplate scriptWithFoldable(Expression foldable) {
        Object fold = foldable.fold();

        return new ScriptTemplate(processScript("{}"),
                paramsBuilder().variable(fold).build(),
                dataType());
    }

    default ScriptTemplate scriptWithScalar(ScalarFunction scalar) {
        ScriptTemplate nested = scalar.asScript();
        return new ScriptTemplate(processScript(nested.template()),
                paramsBuilder().script(nested.params()).build(),
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