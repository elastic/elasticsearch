/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.job;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RollupIndexerTests extends ESTestCase {

    public void testCreateMetadataNoGroupConfig() {
        final Map<String, Object> metadata = RollupIndexer.createMetadata(null);
        assertNotNull(metadata);
        assertTrue(metadata.isEmpty());
    }

    public void testCreateMetadataWithDateHistogramGroupConfigOnly() {
        final DateHistogramGroupConfig dateHistogram = ConfigTestHelpers.randomDateHistogramGroupConfig(random());
        final GroupConfig groupConfig = new GroupConfig(dateHistogram);

        final Map<String, Object> metadata = RollupIndexer.createMetadata(groupConfig);
        assertEquals(1, metadata.size());
        assertTrue(metadata.containsKey("_rollup.interval"));
        Object value = metadata.get("_rollup.interval");
        assertThat(value, equalTo(dateHistogram.getInterval().toString()));
    }

    public void testCreateMetadata() {
        final DateHistogramGroupConfig dateHistogram = ConfigTestHelpers.randomDateHistogramGroupConfig(random());
        final HistogramGroupConfig histogram = ConfigTestHelpers.randomHistogramGroupConfig(random());
        final GroupConfig groupConfig = new GroupConfig(dateHistogram, histogram, null);

        final Map<String, Object> metadata = RollupIndexer.createMetadata(groupConfig);
        assertEquals(1, metadata.size());
        assertTrue(metadata.containsKey("_rollup.interval"));
        Object value = metadata.get("_rollup.interval");
        assertThat(value, equalTo(histogram.getInterval()));
    }
}

