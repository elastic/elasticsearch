/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

@Experimental
public class MedianAbsoluteDeviation extends NumericAggregate {

    // TODO: Add parameter
    public MedianAbsoluteDeviation(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<MedianAbsoluteDeviation> info() {
        return NodeInfo.create(this, MedianAbsoluteDeviation::new, field());
    }

    @Override
    public MedianAbsoluteDeviation replaceChildren(List<Expression> newChildren) {
        return new MedianAbsoluteDeviation(source(), newChildren.get(0));
    }
}
