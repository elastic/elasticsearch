/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MetricStatsTests extends AbstractWireSerializingTestCase<ClusterBalanceStats.MetricStats> {

    @Override
    protected Writeable.Reader<ClusterBalanceStats.MetricStats> instanceReader() {
        return ClusterBalanceStats.MetricStats::readFrom;
    }

    @Override
    protected ClusterBalanceStats.MetricStats createTestInstance() {
        return createRandomMetricStats();
    }

    public static ClusterBalanceStats.MetricStats createRandomMetricStats() {
        return new ClusterBalanceStats.MetricStats(randomDouble(), randomDouble(), randomDouble(), randomDouble(), randomDouble());
    }

    @Override
    protected ClusterBalanceStats.MetricStats mutateInstance(ClusterBalanceStats.MetricStats instance) throws IOException {
        return createTestInstance();
    }

    public void testToXContent() throws IOException {
        ClusterBalanceStats.MetricStats stats = createRandomMetricStats();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        // Convert to map for easy assertions
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        assertThat(map.get("total"), equalTo(stats.total()));
        assertThat(map.get("min"), equalTo(stats.min()));
        assertThat(map.get("max"), equalTo(stats.max()));
        assertThat(map.get("average"), equalTo(stats.average()));
        assertThat(map.get("std_dev"), equalTo(stats.stdDev()));
    }
}
