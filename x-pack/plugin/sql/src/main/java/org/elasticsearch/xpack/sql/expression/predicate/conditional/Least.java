/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.xpack.sql.expression.predicate.conditional.ConditionalProcessor.ConditionalOperation.LEAST;

public class Least extends ConditionalFunction {

    public Least(Location location, List<Expression> fields) {
        super(location, new ArrayList<>(new LinkedHashSet<>(fields)));
    }

    @Override
    protected NodeInfo<? extends Least> info() {
        return NodeInfo.create(this, Least::new, children());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Least(location(), newChildren);
    }

    @Override
    public Object fold() {
        LinkedHashSet<Object> values = new LinkedHashSet<>(children().size());
        for (Expression e : children()) {
            values.add(e.fold());
        }
        return LEAST.apply(values);
    }

    @Override
    protected String scriptMethodName() {
        return LEAST.scriptMethodName();
    }

    @Override
    protected Pipe makePipe() {
        return new ConditionalPipe(location(), this, Expressions.pipe(children()), LEAST);
    }
}
