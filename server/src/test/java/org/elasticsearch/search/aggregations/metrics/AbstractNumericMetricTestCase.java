/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

public abstract class AbstractNumericMetricTestCase<AF extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Numeric, AF>> extends
    BaseAggregationTestCase<AF> {

    @Override
    protected final AF createTestAggregatorBuilder() {
        AF factory = doCreateTestAggregatorFactory();
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        return factory;
    }

    protected abstract AF doCreateTestAggregatorFactory();
}
