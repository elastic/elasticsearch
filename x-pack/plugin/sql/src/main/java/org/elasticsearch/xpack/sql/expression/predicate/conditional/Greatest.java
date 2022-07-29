/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
        Set<Object> values = Sets.newLinkedHashSetWithExpectedSize(children().size());
        for (Expression e : children()) {
            values.add(SqlDataTypeConverter.convert(Foldables.valueOf(e), dataType));
        }
        return GREATEST.apply(values);
    }
}
