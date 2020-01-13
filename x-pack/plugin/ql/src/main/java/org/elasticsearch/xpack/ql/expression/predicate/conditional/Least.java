/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.xpack.ql.expression.predicate.conditional.ConditionalProcessor.ConditionalOperation.LEAST;

public class Least extends ArbitraryConditionalFunction {

    public Least(Source source, List<Expression> fields) {
        super(source, new ArrayList<>(new LinkedHashSet<>(fields)), LEAST);
    }

    @Override
    protected NodeInfo<? extends Least> info() {
        return NodeInfo.create(this, Least::new, children());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Least(source(), newChildren);
    }

    @Override
    public Object fold() {
        return LEAST.apply(Foldables.valuesOfNoDuplicates(children(), dataType));
    }
}
