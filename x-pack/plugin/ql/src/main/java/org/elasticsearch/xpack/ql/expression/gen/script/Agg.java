/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.script;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.expression.function.aggregate.InnerAggregate;

class Agg extends Param<AggregateFunction> {

    private static final String COUNT_PATH = "_count";

    Agg(AggregateFunction aggRef) {
        super(aggRef);
    }

    String aggName() {
        return Expressions.id(value());
    }

    public String aggProperty() {
        AggregateFunction agg = value();

        if (agg instanceof InnerAggregate) {
            InnerAggregate inner = (InnerAggregate) agg;
            return Expressions.id((Expression) inner.outer()) + "." + inner.innerName();
        }
        // Count needs special handling since in most cases it is not a dedicated aggregation
        else if (agg instanceof Count) {
            Count c = (Count) agg;
            // for literals get the last count
            if (c.field().foldable()) {
                return COUNT_PATH;
            }
            // when dealing with fields, check whether there's a single-metric (distinct -> cardinality)
            // or a bucket (non-distinct - filter agg)
            else {
                if (c.distinct()) {
                    return Expressions.id(c);
                } else {
                    return Expressions.id(c) + "." + COUNT_PATH;
                }
            }
        }
        return null;
    }

    @Override
    public String prefix() {
        return "a";
    }
}
