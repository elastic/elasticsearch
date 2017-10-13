/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Arrays;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public abstract class BinaryScalarFunction extends ScalarFunction {

    private final Expression left, right;

    protected BinaryScalarFunction(Location location, Expression left, Expression right) {
        super(location, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    public boolean foldable() {
        return left.foldable() && right.foldable();
    }

    @Override
    public ScalarFunctionAttribute toAttribute() {
        return new ScalarFunctionAttribute(location(), name(), dataType(), id(), asScript(), orderBy(), asProcessorDefinition());
    }

    protected ScriptTemplate asScript() {
        ScriptTemplate leftScript = asScript(left());
        ScriptTemplate rightScript = asScript(right());

        return asScriptFrom(leftScript, rightScript);
    }

    protected abstract ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript);

    protected ScriptTemplate asScript(Expression exp) {
        if (exp.foldable()) {
            return asScriptFromFoldable(exp);
        }

        Attribute attr = Expressions.attribute(exp);
        if (attr != null) {
            if (attr instanceof ScalarFunctionAttribute) {
                return asScriptFrom((ScalarFunctionAttribute) attr);
            }
            if (attr instanceof AggregateFunctionAttribute) {
                return asScriptFrom((AggregateFunctionAttribute) attr);
            }
            // fall-back to
            return asScriptFrom((FieldAttribute) attr);
        }
        throw new SqlIllegalArgumentException("Cannot evaluate script for field %s", exp);
    }

    protected ScriptTemplate asScriptFrom(ScalarFunctionAttribute scalar) {
        return scalar.script();
    }

    protected ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate) {
        return new ScriptTemplate(formatTemplate("{}"),
                paramsBuilder().agg(aggregate.functionId(), aggregate.propertyPath()).build(), 
                aggregate.dataType());
    }

    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        return new ScriptTemplate(formatTemplate("doc[{}].value"), 
                paramsBuilder().variable(field.name()).build(),
                field.dataType());
    }

    protected ScriptTemplate asScriptFromFoldable(Expression foldable) {
        return new ScriptTemplate(formatTemplate("{}"),
                paramsBuilder().variable(foldable.fold()).build(), 
                foldable.dataType());
    }
}