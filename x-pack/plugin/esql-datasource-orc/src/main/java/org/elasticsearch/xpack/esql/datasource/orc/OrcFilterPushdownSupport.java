/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;

import java.util.ArrayList;
import java.util.List;

/**
 * Filter pushdown support for ORC files using the SearchArgument API.
 * <p>
 * ORC's SearchArgument performs block-level skipping (stripes and 10K-row groups)
 * based on column statistics and bloom filters. It does NOT filter individual rows.
 * Therefore all pushed expressions use RECHECK semantics — they remain in the ESQL
 * FilterExec for row-level correctness while the SearchArgument optimizes I/O.
 */
public class OrcFilterPushdownSupport implements FilterPushdownSupport {

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        // All filters stay in remainder for RECHECK — FilterExec must always evaluate them
        List<Expression> remainder = new ArrayList<>(filters);

        List<Expression> pushed = new ArrayList<>();
        for (Expression filter : filters) {
            if (OrcPushdownFilters.canConvert(filter)) {
                pushed.add(filter);
            }
        }

        if (pushed.isEmpty()) {
            return PushdownResult.none(filters);
        }

        SearchArgument sarg = OrcPushdownFilters.buildSearchArgument(pushed);
        if (sarg == null) {
            return PushdownResult.none(filters);
        }

        // remainder contains ALL original filters — this guarantees FilterExec stays in the plan
        return new PushdownResult(sarg, remainder);
    }

    @Override
    public Pushability canPush(Expression expr) {
        // RECHECK: ORC SearchArgument only skips blocks (stripes/row-groups),
        // it does NOT filter individual rows. The filter must remain in FilterExec.
        return OrcPushdownFilters.canConvert(expr) ? Pushability.RECHECK : Pushability.NO;
    }
}
