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
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.Params;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;

public abstract class ScalarFunction extends Function {

    private final Expression argument;

    protected ScalarFunction(Location location) {
        super(location, emptyList());
        this.argument = null;
    }

    protected ScalarFunction(Location location, Expression child) {
        super(location, singletonList(child));
        this.argument = child;
    }

    public Expression argument() {
        return argument;
    }

    @Override
    public ScalarFunctionAttribute toAttribute() {
        String functionId = null;
        Attribute attr = Expressions.attribute(argument());

        if (attr instanceof AggregateFunctionAttribute) {
            AggregateFunctionAttribute afa = (AggregateFunctionAttribute) attr;
            functionId = afa.functionId();
        }

        return new ScalarFunctionAttribute(location(), name(), dataType(), id(), asScript(), orderBy(), functionId);
    }

    protected ScriptTemplate asScript() {
        Attribute attr = Expressions.attribute(argument());
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
        throw new SqlIllegalArgumentException("Cannot evaluate script for field %s", argument());
    }

    protected ScriptTemplate asScriptFrom(ScalarFunctionAttribute scalar) {
        ScriptTemplate nested = scalar.script();
        Params p = paramsBuilder().script(nested.params()).build();
        return new ScriptTemplate(chainScalarTemplate(nested.template()), p, dataType());
    }

    protected abstract ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate);

    protected abstract ScriptTemplate asScriptFrom(FieldAttribute field);

    protected abstract String chainScalarTemplate(String template);


    public abstract ColumnsProcessor asProcessor();

    // used if the function is monotonic and thus does not have to be computed for ordering purposes 
    public Expression orderBy() {
        return null;
    }
}