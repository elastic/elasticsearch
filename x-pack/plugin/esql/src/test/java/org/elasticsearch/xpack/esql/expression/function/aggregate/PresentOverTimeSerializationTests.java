/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class PresentOverTimeSerializationTests extends AbstractTimeSeriesAggregationSerializationTests<PresentOverTime> {
    @Override
    protected PresentOverTime create(Source source, Expression field, Expression filter, Expression window) {
        return new PresentOverTime(source, field, filter, window);
    }
}
