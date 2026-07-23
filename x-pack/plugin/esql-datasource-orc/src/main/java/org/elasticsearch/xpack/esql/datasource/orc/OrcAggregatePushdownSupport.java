/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;

import java.util.List;

/**
 * ORC supports COUNT(*), MIN(column), MAX(column) from stripe statistics. Virtual columns
 * (engine-synthesized {@code _file.*}, ES metadata) cannot be answered from file stats and are
 * always rejected via {@link PushdownPredicates#isVirtualColumn}.
 */
public class OrcAggregatePushdownSupport implements AggregatePushdownSupport {

    @Override
    public Pushability canPushAggregates(List<Expression> aggregates, List<Expression> groupings) {
        if (groupings.isEmpty() == false) {
            return Pushability.NO;
        }
        for (int i = 0; i < aggregates.size(); i++) {
            Expression agg = aggregates.get(i);
            if (agg instanceof Count count) {
                // COUNT(*) / COUNT(<literal>) come from the stripe row count; COUNT(<attribute>)
                // is computed as rowCount - nullCount(column), which only exists for real ORC
                // columns. Virtual fields (engine-synthesized) have no stripe stats and are
                // rejected here.
                if (count.field().foldable() == false && PushdownPredicates.isVirtualColumn(count.field())) {
                    return Pushability.NO;
                }
            } else if (agg instanceof Min min) {
                if (min.field() instanceof Attribute == false || PushdownPredicates.isVirtualColumn(min.field())) {
                    return Pushability.NO;
                }
            } else if (agg instanceof Max max) {
                if (max.field() instanceof Attribute == false || PushdownPredicates.isVirtualColumn(max.field())) {
                    return Pushability.NO;
                }
            } else {
                return Pushability.NO;
            }
        }
        return Pushability.YES;
    }
}
