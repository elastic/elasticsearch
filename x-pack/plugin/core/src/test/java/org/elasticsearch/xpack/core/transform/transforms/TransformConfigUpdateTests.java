/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests.randomTransformConfig;
import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.hamcrest.Matchers.equalTo;

public class TransformConfigUpdateTests extends AbstractSerializingTransformTestCase<TransformConfigUpdate> {

    public static TransformConfigUpdate randomTransformConfigUpdate() {
        return new TransformConfigUpdate(
            randomBoolean() ? null : randomSourceConfig(),
            randomBoolean() ? null : randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000)
        );
    }

    public static SyncConfig randomSyncConfig() {
        return TimeSyncConfigTests.randomTimeSyncConfig();
    }

    @Override
    protected TransformConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return TransformConfigUpdate.fromXContent(parser);
    }

    @Override
    protected TransformConfigUpdate createTestInstance() {
        return randomTransformConfigUpdate();
    }

    @Override
    protected Reader<TransformConfigUpdate> instanceReader() {
        return TransformConfigUpdate::new;
    }

    public void testIsNoop() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            TransformConfig config = randomTransformConfig();
            TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null);
            assertTrue("null update is not noop", update.isNoop(config));
            update = new TransformConfigUpdate(
                config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                config.getDescription()
            );
            assertTrue("equal update is not noop", update.isNoop(config));

            update = new TransformConfigUpdate(
                config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                "this is a new description"
            );
            assertFalse("true update is noop", update.isNoop(config));
        }
    }

    public void testApply() {
        TransformConfig config = new TransformConfig(
            "time-transform",
            randomSourceConfig(),
            randomDestConfig(),
            TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            TimeSyncConfigTests.randomTimeSyncConfig(),
            Collections.singletonMap("key", "value"),
            PivotConfigTests.randomPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.V_7_2_0.toString()
        );
        TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null);

        assertThat(config, equalTo(update.apply(config)));
        SourceConfig sourceConfig = new SourceConfig("the_new_index");
        DestConfig destConfig = new DestConfig("the_new_dest", "my_new_pipeline");
        TimeValue frequency = TimeValue.timeValueSeconds(10);
        SyncConfig syncConfig = new TimeSyncConfig("time_field", TimeValue.timeValueSeconds(30));
        String newDescription = "new description";
        update = new TransformConfigUpdate(sourceConfig, destConfig, frequency, syncConfig, newDescription);

        Map<String, String> headers = Collections.singletonMap("foo", "bar");
        update.setHeaders(headers);
        TransformConfig updatedConfig = update.apply(config);

        assertThat(updatedConfig.getSource(), equalTo(sourceConfig));
        assertThat(updatedConfig.getDestination(), equalTo(destConfig));
        assertThat(updatedConfig.getFrequency(), equalTo(frequency));
        assertThat(updatedConfig.getSyncConfig(), equalTo(syncConfig));
        assertThat(updatedConfig.getDescription(), equalTo(newDescription));
        assertThat(updatedConfig.getHeaders(), equalTo(headers));
        assertThat(updatedConfig.getVersion(), equalTo(Version.CURRENT));
    }

    public void testApplyWithSyncChange() {
        TransformConfig batchConfig = new TransformConfig(
            "batch-transform",
            randomSourceConfig(),
            randomDestConfig(),
            TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            null,
            null,
            PivotConfigTests.randomPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.CURRENT.toString()
        );

        TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, TimeSyncConfigTests.randomTimeSyncConfig(), null);

        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class, () -> update.apply(batchConfig));
        assertThat(
            ex.getMessage(),
            equalTo("Cannot change the current sync configuration of transform [batch-transform] from [null] to [time]")
        );

        TransformConfig timeSyncedConfig = new TransformConfig(
            "time-transform",
            randomSourceConfig(),
            randomDestConfig(),
            TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            TimeSyncConfigTests.randomTimeSyncConfig(),
            null,
            PivotConfigTests.randomPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.CURRENT.toString()
        );

        TransformConfigUpdate fooSyncUpdate = new TransformConfigUpdate(null, null, null, new FooSync(), null);
        ex = expectThrows(ElasticsearchStatusException.class, () -> fooSyncUpdate.apply(timeSyncedConfig));
        assertThat(
            ex.getMessage(),
            equalTo("Cannot change the current sync configuration of transform [time-transform] from [time] to [foo]")
        );

    }

    static class FooSync implements SyncConfig {

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public QueryBuilder getRangeQuery(TransformCheckpoint newCheckpoint) {
            return null;
        }

        @Override
        public QueryBuilder getRangeQuery(TransformCheckpoint oldCheckpoint, TransformCheckpoint newCheckpoint) {
            return null;
        }

        @Override
        public String getWriteableName() {
            return "foo";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }
}
