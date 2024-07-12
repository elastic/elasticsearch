/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesRefStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Set;

public class NodesStatsRequestParametersTests extends ESTestCase {

    public void testMetricsReadWrite() throws IOException {
        var out = new BytesRefStreamOutput();
        var outMetics = Set.of(Metric.OS, Metric.JVM, Metric.ALLOCATIONS);
        out.writeCollection(outMetics);
        var in = new ByteArrayStreamInput(out.get().bytes);
        var inMetrics = Metric.readSetFrom(in);
        assertEquals(outMetics, inMetrics);
    }

    public void testUnknownMetricsReadWrite() throws IOException {
        var out = new BytesRefStreamOutput();
        var outMetics = Set.of(Metric.OS.metricName(), Metric.JVM.metricName(), "unknown");
        out.writeStringCollection(outMetics);
        var in = new ByteArrayStreamInput(out.get().bytes);
        var inMetrics = Metric.readSetFrom(in);
        assertEquals("should ignore unknown metrics", Set.of(Metric.OS, Metric.JVM), inMetrics);
    }

}
