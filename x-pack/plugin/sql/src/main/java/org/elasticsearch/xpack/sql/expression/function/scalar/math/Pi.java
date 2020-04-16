/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;


import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.gen.script.Params;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

public class Pi extends MathFunction {

    private static final ScriptTemplate TEMPLATE = new ScriptTemplate("Math.PI", Params.EMPTY, DataTypes.DOUBLE);

    public Pi(Source source) {
        super(source, new Literal(source, Math.PI, DataTypes.DOUBLE));
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
