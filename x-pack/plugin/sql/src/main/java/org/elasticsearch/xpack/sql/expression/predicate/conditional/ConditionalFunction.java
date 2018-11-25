/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Base class for conditional predicates.
 */
public abstract class ConditionalFunction extends ScalarFunction {

    private DataType dataType = DataType.NULL;

    protected ConditionalFunction(Location location, List<Expression> fields) {
        super(location, fields);
    }

    protected abstract String scriptMethodName();

    @Override
    protected TypeResolution resolveType() {
        for (Expression e : children()) {
            dataType = DataTypeConversion.commonType(dataType, e.dataType());
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public ScriptTemplate asScript() {
        List<ScriptTemplate> templates = new ArrayList<>();
        for (Expression ex : children()) {
            templates.add(asScript(ex));
        }

        StringJoiner template = new StringJoiner(",", "{sql}." + scriptMethodName() +"([", "])");
        ParamsBuilder params = paramsBuilder();

        for (ScriptTemplate scriptTemplate : templates) {
            template.add(scriptTemplate.template());
            params.script(scriptTemplate.params());
        }

        return new ScriptTemplate(template.toString(), params.build(), dataType());
    }
}
