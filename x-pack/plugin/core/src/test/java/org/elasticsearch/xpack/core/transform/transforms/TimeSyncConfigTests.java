/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TimeSyncConfigTests extends AbstractXContentSerializingTestCase<TimeSyncConfig> {

    public static TimeSyncConfig randomTimeSyncConfig() {
        return new TimeSyncConfig(randomAlphaOfLengthBetween(1, 10), new TimeValue(randomNonNegativeLong()));
    }

    @Override
    protected TimeSyncConfig doParseInstance(XContentParser parser) throws IOException {
        return TimeSyncConfig.fromXContent(parser, false);
    }

    @Override
    protected TimeSyncConfig createTestInstance() {
        return randomTimeSyncConfig();
    }

    @Override
    protected TimeSyncConfig mutateInstance(TimeSyncConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<TimeSyncConfig> instanceReader() {
        return TimeSyncConfig::new;
    }

    public void testDefaultDelay() {
        TimeSyncConfig config = new TimeSyncConfig(randomAlphaOfLength(10), null);
        assertThat(config.getDelay(), equalTo(TimeSyncConfig.DEFAULT_DELAY));
    }

    public void testGetRangeQueryWithSingleCheckpoint() {
        TimeSyncConfig config = new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(60));
        TransformCheckpoint checkpoint = new TransformCheckpoint("t_id", 123456789L, 1L, Collections.emptyMap(), 100000L);

        assertThat(
            config.getRangeQuery(checkpoint),
            is(equalTo(QueryBuilders.rangeQuery("timestamp").lte(100000L).format("epoch_millis")))
        );
    }

    public void testGetRangeQueryWithTwoCheckpoints() {
        TimeSyncConfig config = new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(60));
        TransformCheckpoint oldCheckpoint = new TransformCheckpoint("t_id", 100000000L, 1L, Collections.emptyMap(), 100000L);
        TransformCheckpoint newCheckpoint = new TransformCheckpoint("t_id", 123456789L, 2L, Collections.emptyMap(), 200000L);

        assertThat(
            config.getRangeQuery(oldCheckpoint, newCheckpoint),
            is(equalTo(QueryBuilders.rangeQuery("timestamp").gt(100000L).lte(200000L).format("epoch_millis")))
        );
    }

    public void testGetRangeQueryWithIdenticalCheckpointBounds() {
        TimeSyncConfig config = new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(60));
        TransformCheckpoint oldCheckpoint = new TransformCheckpoint("t_id", 100000000L, 1L, Collections.emptyMap(), 100000L);
        TransformCheckpoint newCheckpoint = new TransformCheckpoint("t_id", 100000001L, 2L, Collections.emptyMap(), 100000L);

        assertThat(
            config.getRangeQuery(oldCheckpoint, newCheckpoint),
            is(equalTo(QueryBuilders.rangeQuery("timestamp").gt(100000L).lte(100000L).format("epoch_millis")))
        );
    }
}
