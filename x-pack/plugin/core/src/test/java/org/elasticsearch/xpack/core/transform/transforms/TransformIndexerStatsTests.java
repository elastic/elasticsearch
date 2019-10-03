/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class TransformIndexerStatsTests extends AbstractSerializingTestCase<TransformIndexerStats> {

    @Override
    protected TransformIndexerStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected Writeable.Reader<TransformIndexerStats> instanceReader() {
        return TransformIndexerStats::new;
    }

    @Override
    protected TransformIndexerStats doParseInstance(XContentParser parser) {
        return TransformIndexerStats.fromXContent(parser);
    }

    public static TransformIndexerStats randomStats() {
        return new TransformIndexerStats(randomLongBetween(10L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomBoolean() ? randomDouble() : null,
            randomBoolean() ? randomDouble() : null,
            randomBoolean() ? randomDouble() : null);
    }

    public void testExpAvgIncrement() {
        TransformIndexerStats stats = new TransformIndexerStats();

        assertThat(stats.getExpAvgCheckpointDurationMs(), equalTo(0.0));
        assertThat(stats.getExpAvgDocumentsIndexed(), equalTo(0.0));
        assertThat(stats.getExpAvgDocumentsProcessed(), equalTo(0.0));

        stats.incrementCheckpointExponentialAverages(100, 20, 50);

        assertThat(stats.getExpAvgCheckpointDurationMs(), equalTo(100.0));
        assertThat(stats.getExpAvgDocumentsIndexed(), equalTo(20.0));
        assertThat(stats.getExpAvgDocumentsProcessed(), equalTo(50.0));

        stats.incrementCheckpointExponentialAverages(150, 23, 100);

        assertThat(stats.getExpAvgCheckpointDurationMs(), closeTo(109.090909, 0.0000001));
        assertThat(stats.getExpAvgDocumentsIndexed(), closeTo(20.54545454, 0.0000001));
        assertThat(stats.getExpAvgDocumentsProcessed(), closeTo(59.0909090, 0.0000001));
    }
}
