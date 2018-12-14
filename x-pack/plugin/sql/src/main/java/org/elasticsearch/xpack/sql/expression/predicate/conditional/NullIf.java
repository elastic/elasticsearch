/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Accepts 2 arguments of any data type and returns null if they are equal,
 * and the 1st argument otherwise.
 */
public class NullIf extends ConditionalFunction {

    public NullIf(Location location, Expression left, Expression right) {
        super(location, Arrays.asList(left, right));
    }

    @Override
    protected NodeInfo<? extends NullIf> info() {
        return NodeInfo.create(this, NullIf::new, children().get(0), children().get(1));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new NullIf(location(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected TypeResolution resolveType() {
        dataType = children().get(0).dataType();
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
        return true;
    }

    @Override
    public Object fold() {
        return NullIfProcessor.apply(children().get(0).fold(), children().get(1).fold());
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate left = asScript(children().get(0));
        ScriptTemplate right = asScript(children().get(1));
        String template = "{sql}.nullif(" + left.template() + "," + right.template() + ")";
        ParamsBuilder params = paramsBuilder();
        params.script(left.params());
        params.script(right.params());

        return new ScriptTemplate(template, params.build(), dataType);
    }

    @Override
    protected Pipe makePipe() {
        return new NullIfPipe(location(), this,
            Expressions.pipe(children().get(0)), Expressions.pipe(children().get(1)));
    }
}
