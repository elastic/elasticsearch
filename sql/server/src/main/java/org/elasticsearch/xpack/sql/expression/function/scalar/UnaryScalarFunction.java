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
import org.elasticsearch.xpack.sql.expression.function.scalar.script.Params;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public abstract class UnaryScalarFunction extends ScalarFunction {

    private final Expression field;

    protected UnaryScalarFunction(Location location) {
        super(location);
        this.field = null;
    }

    protected UnaryScalarFunction(Location location, Expression field) {
        super(location, singletonList(field));
        this.field = field;
    }

    public Expression field() {
        return field;
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Override
    public ScalarFunctionAttribute toAttribute() {
        String functionId = null;
        Attribute attr = Expressions.attribute(field());

        if (attr instanceof AggregateFunctionAttribute) {
            AggregateFunctionAttribute afa = (AggregateFunctionAttribute) attr;
            functionId = afa.functionId();
        }

        return new ScalarFunctionAttribute(location(), name(), dataType(), id(), asScript(), orderBy(), asProcessor());
    }

    protected ScriptTemplate asScript() {
        if (field.foldable()) {
            return asScriptFromFoldable(field);
        }

        Attribute attr = Expressions.attribute(field());
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
        throw new SqlIllegalArgumentException("Cannot evaluate script for field %s", field());
    }

    protected ScriptTemplate asScriptFromFoldable(Expression foldable) {
        return new ScriptTemplate(formatTemplate("{}"),
                paramsBuilder().variable(foldable.fold()).build(), 
                foldable.dataType());
    }
    
    protected ScriptTemplate asScriptFrom(ScalarFunctionAttribute scalar) {
        ScriptTemplate nested = scalar.script();
        Params p = paramsBuilder().script(nested.params()).build();
        return new ScriptTemplate(chainScalarTemplate(nested.template()), p, dataType());
    }

    protected abstract ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate);

    protected abstract ScriptTemplate asScriptFrom(FieldAttribute field);

    protected abstract String chainScalarTemplate(String template);
}