/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SearchLoadMetricsCollectorTests extends ESTestCase {

    public void testEmptyCollector() {
        final PlainActionFuture<SearchLoadMetrics> searchLoadMetricsFuture = new PlainActionFuture<>();
        SearchLoadMetricsCollector.EMPTY.collectSearchLoadMetrics(ClusterState.EMPTY_STATE, searchLoadMetricsFuture);
        final SearchLoadMetrics searchLoadMetrics = safeGet(searchLoadMetricsFuture);
        assertThat(searchLoadMetrics.searchCopyDemands(), equalTo(Map.of()));
        assertThat(searchLoadMetrics.nodeSearchCapacities(), equalTo(Map.of()));
    }
}
