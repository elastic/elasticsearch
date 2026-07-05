/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.List;

/**
 * SPI for declaring that a format can compute aggregates from file-level statistics.
 * Implementations are queried by the optimizer to determine if aggregates can be
 * pushed down. The actual computation happens via file footer reads at execution time.
 */
public interface AggregatePushdownSupport {

    enum Pushability {
        YES,
        NO
    }

    Pushability canPushAggregates(List<Expression> aggregates, List<Expression> groupings);

    AggregatePushdownSupport UNSUPPORTED = (aggregates, groupings) -> Pushability.NO;
}
