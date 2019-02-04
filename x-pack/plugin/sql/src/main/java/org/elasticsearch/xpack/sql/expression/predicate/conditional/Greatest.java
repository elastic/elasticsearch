/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Foldables;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.xpack.sql.expression.predicate.conditional.ConditionalProcessor.ConditionalOperation.GREATEST;

public class Greatest extends ArbitraryConditionalFunction {

    public Greatest(Source source, List<Expression> fields) {
        super(source, new ArrayList<>(new LinkedHashSet<>(fields)), GREATEST);
    }

    @Override
    protected NodeInfo<? extends Greatest> info() {
        return NodeInfo.create(this, Greatest::new, children());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Greatest(source(), newChildren);
    }

    @Override
    public Object fold() {
        return GREATEST.apply(Foldables.valuesOfNoDuplicates(children(), dataType));
    }
}
