/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import java.util.Locale;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import static java.lang.String.format;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public abstract class MathFunction extends ScalarFunction {

    protected MathFunction(Location location) {
        super(location);
    }

    protected MathFunction(Location location, Expression argument) {
        super(location, argument);
    }

    public boolean foldable() {
        return argument().foldable();
    }


    @Override
    protected String chainScalarTemplate(String template) {
        return createTemplate(template);
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScalarFunctionAttribute scalar) {
        ScriptTemplate nested = scalar.script();
        return new ScriptTemplate(createTemplate(nested.template()),
                paramsBuilder().script(nested.params()).build(),
                dataType());
    }

    @Override
    protected ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate) {
        return new ScriptTemplate(createTemplate(formatTemplate("{}")),
                paramsBuilder().agg(aggregate.functionId(), aggregate.propertyPath()).build(),
                dataType());
    }

    @Override
    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        return new ScriptTemplate(createTemplate(formatTemplate("doc[{}].value")),
                paramsBuilder().variable(field.name()).build(),
                dataType());
    }

    private String createTemplate(String template) {
        return format(Locale.ROOT, "Math.%s(%s)", mathFunction(), template);
    }

    protected String mathFunction() {
        return getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public ColumnProcessor asProcessor() {
        return l -> {
            double d = ((Number) l).doubleValue();
            return math(d);
        };
    }

    protected abstract Object math(double d);
}