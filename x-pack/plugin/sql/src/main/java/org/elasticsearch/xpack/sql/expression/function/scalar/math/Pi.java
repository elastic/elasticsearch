/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;


import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.expression.gen.script.Params;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

public class Pi extends MathFunction {

    private static final ScriptTemplate TEMPLATE = new ScriptTemplate("Math.PI", Params.EMPTY, DataType.DOUBLE);

    public Pi(Source source) {
        super(source, new Literal(source, "PI", Math.PI, DataType.DOUBLE));
    }

    @Override
    protected NodeInfo<Pi> info() {
        return NodeInfo.create(this);
    }

    @Override
    protected Pi replaceChild(Expression field) {
        throw new UnsupportedOperationException("this node doesn't have any children");
    }

    @Override
    public Object fold() {
        return Math.PI;
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
