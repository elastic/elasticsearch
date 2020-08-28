/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Accepts 2 arguments of any data type and returns null if they are equal,
 * and the 1st argument otherwise.
 */
public class NullIf extends ConditionalFunction {

    public NullIf(Source source, Expression left, Expression right) {
        super(source, Arrays.asList(left, right));
    }

    @Override
    protected NodeInfo<? extends NullIf> info() {
        return NodeInfo.create(this, NullIf::new, children().get(0), children().get(1));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new NullIf(source(), newChildren.get(0), newChildren.get(1));
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

        return new ScriptTemplate(formatTemplate(template), params.build(), dataType);
    }

    @Override
    protected Pipe makePipe() {
        return new NullIfPipe(source(), this,
            Expressions.pipe(children().get(0)), Expressions.pipe(children().get(1)));
    }
}
