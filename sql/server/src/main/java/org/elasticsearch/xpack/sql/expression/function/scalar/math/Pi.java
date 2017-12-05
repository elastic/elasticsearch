/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;


import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.Params;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.elasticsearch.xpack.sql.util.StringUtils;

public class Pi extends MathFunction {

    private static final ScriptTemplate TEMPLATE = new ScriptTemplate("Math.PI", Params.EMPTY, DataTypes.DOUBLE);

    public Pi(Location location) {
        super(location, new Literal(location, Math.PI, DataTypes.DOUBLE));
    }

    @Override
    public Object fold() {
        return Math.PI;
    }

    @Override
    protected String functionArgs() {
        return StringUtils.EMPTY;
    }

    @Override
    public ScriptTemplate asScript() {
        return TEMPLATE;
    }

    @Override
    protected MathOperation operation() {
        return MathOperation.PI;
    }
}
