/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EnumSerializationTestUtils;

public class NodesStatsRequestParametersTests extends ESTestCase {

    // future-proof of accidental enum ordering change or extension
    public void testEnsureMetricOrdinalsOrder() {
        EnumSerializationTestUtils.assertEnumSerialization(
            Metric.class,
            Metric.OS,
            Metric.PROCESS,
            Metric.JVM,
            Metric.THREAD_POOL,
            Metric.FS,
            Metric.TRANSPORT,
            Metric.HTTP,
            Metric.BREAKER,
            Metric.SCRIPT,
            Metric.DISCOVERY,
            Metric.INGEST,
            Metric.ADAPTIVE_SELECTION,
            Metric.SCRIPT_CACHE,
            Metric.INDEXING_PRESSURE,
            Metric.REPOSITORIES,
            Metric.ALLOCATIONS
        );
    }

}
