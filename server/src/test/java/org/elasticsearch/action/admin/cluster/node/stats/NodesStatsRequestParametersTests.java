/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesRefStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class NodesStatsRequestParametersTests extends ESTestCase {

    private static NodesStatsRequestParameters randomRequest() {
        var req = new NodesStatsRequestParameters();
        req.setIndices(CommonStatsFlags.ALL);
        req.setIncludeShardsStats(randomBoolean());
        req.requestedMetrics().addAll(randomSubsetOf(Metric.ALL));
        return req;
    }

    public void testReadWriteMetricSet() {
        var reqOut = randomRequest();
        try {
            var out = new BytesRefStreamOutput();
            reqOut.writeTo(out);
            var in = new ByteArrayStreamInput(out.get().bytes);
            var reqIn = new NodesStatsRequestParameters(in);
            assertEquals(reqOut.requestedMetrics(), reqIn.requestedMetrics());
        } catch (IOException e) {
            var errMsg = "metrics=" + reqOut.requestedMetrics().toString();
            throw new AssertionError(errMsg, e);
        }
    }

    public void testReadWriteMetricThrowsOnUnknown() throws IOException {
        var metricsOut = List.of("os", "wrong");
        var out = new BytesRefStreamOutput();
        out.writeStringCollection(metricsOut);
        var in = new ByteArrayStreamInput(out.get().bytes);
        assertThrows(AssertionError.class, () -> Metric.readSetFrom(in));
    }
}
