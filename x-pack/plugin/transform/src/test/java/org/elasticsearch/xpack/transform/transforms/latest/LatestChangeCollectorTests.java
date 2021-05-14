/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LatestChangeCollectorTests extends ESTestCase {

    public void testBuildFilterQuery() {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp");

        assertThat(
            changeCollector.buildFilterQuery(0, 123456789),
            is(equalTo(QueryBuilders.rangeQuery("timestamp").gte(0L).lt(123456789L).format("epoch_millis"))));

        assertThat(
            changeCollector.buildFilterQuery(123456789, 234567890),
            is(equalTo(QueryBuilders.rangeQuery("timestamp").gte(123456789L).lt(234567890L).format("epoch_millis"))));
    }
}
