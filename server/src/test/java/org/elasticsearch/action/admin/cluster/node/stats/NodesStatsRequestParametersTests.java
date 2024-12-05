/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesRefStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EnumSerializationTestUtils;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

public class NodesStatsRequestParametersTests extends ESTestCase {

    public void testReadWriteMetricSet() {
        for (var version : List.of(TransportVersions.V_8_15_0, TransportVersions.V_8_16_0)) {
            var randSet = randomSubsetOf(Metric.ALL);
            var metricsOut = randSet.isEmpty() ? EnumSet.noneOf(Metric.class) : EnumSet.copyOf(randSet);
            try {
                var out = new BytesRefStreamOutput();
                out.setTransportVersion(version);
                Metric.writeSetTo(out, metricsOut);
                var in = new ByteArrayStreamInput(out.get().bytes);
                in.setTransportVersion(version);
                var metricsIn = Metric.readSetFrom(in);
                assertEquals(metricsOut, metricsIn);
            } catch (IOException e) {
                var errMsg = "metrics=" + metricsOut.toString();
                throw new AssertionError(errMsg, e);
            }
        }
    }

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
