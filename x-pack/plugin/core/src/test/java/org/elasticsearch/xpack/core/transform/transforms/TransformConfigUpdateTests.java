/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.AbstractWireSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests.randomTransformConfig;
import static org.hamcrest.Matchers.equalTo;

public class TransformConfigUpdateTests extends AbstractWireSerializingTransformTestCase<TransformConfigUpdate> {

    public static TransformConfigUpdate randomTransformConfigUpdate() {
        return new TransformConfigUpdate(
            randomBoolean() ? null : randomSourceConfig(),
            randomBoolean() ? null : randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : SettingsConfigTests.randomSettingsConfig(),
            randomBoolean() ? null : randomRetentionPolicyConfig()
        );
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
            TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null, null, null);
            assertTrue("null update is not noop", update.isNoop(config));
            update = new TransformConfigUpdate(
                config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                config.getDescription(),
                config.getSettings(),
                config.getRetentionPolicyConfig()
            );
            assertTrue("equal update is not noop", update.isNoop(config));

            update = new TransformConfigUpdate(
                config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                "this is a new description",
                config.getSettings(),
                config.getRetentionPolicyConfig()
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
            randomSyncConfig(),
            Collections.singletonMap("key", "value"),
            PivotConfigTests.randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            SettingsConfigTests.randomNonEmptySettingsConfig(),
            randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.V_7_2_0.toString()
        );
        TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null, null, null);

        assertThat(config, equalTo(update.apply(config)));
        SourceConfig sourceConfig = new SourceConfig("the_new_index");
        DestConfig destConfig = new DestConfig("the_new_dest", "my_new_pipeline");
        TimeValue frequency = TimeValue.timeValueSeconds(10);
        SyncConfig syncConfig = new TimeSyncConfig("time_field", TimeValue.timeValueSeconds(30));
        String newDescription = "new description";
        SettingsConfig settings = new SettingsConfig(4_000, 4_000.400F, true, true);
        RetentionPolicyConfig retentionPolicyConfig = new TimeRetentionPolicyConfig("time_field", new TimeValue(60_000));
        update = new TransformConfigUpdate(
            sourceConfig,
            destConfig,
            frequency,
            syncConfig,
            newDescription,
            settings,
            retentionPolicyConfig
        );

        Map<String, String> headers = Collections.singletonMap("foo", "bar");
        update.setHeaders(headers);
        TransformConfig updatedConfig = update.apply(config);

        assertThat(updatedConfig.getSource(), equalTo(sourceConfig));
        assertThat(updatedConfig.getDestination(), equalTo(destConfig));
        assertThat(updatedConfig.getFrequency(), equalTo(frequency));
        assertThat(updatedConfig.getSyncConfig(), equalTo(syncConfig));
        assertThat(updatedConfig.getDescription(), equalTo(newDescription));
        assertThat(updatedConfig.getSettings(), equalTo(settings));
        assertThat(updatedConfig.getRetentionPolicyConfig(), equalTo(retentionPolicyConfig));
        assertThat(updatedConfig.getHeaders(), equalTo(headers));
        assertThat(updatedConfig.getVersion(), equalTo(Version.CURRENT));
    }

    public void testApplySettings() {
        TransformConfig config = new TransformConfig(
            "time-transform",
            randomSourceConfig(),
            randomDestConfig(),
            TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomSyncConfig(),
            Collections.singletonMap("key", "value"),
            PivotConfigTests.randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            SettingsConfigTests.randomNonEmptySettingsConfig(),
            randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.V_7_2_0.toString()
        );

        TransformConfigUpdate update = new TransformConfigUpdate(
            null,
            null,
            null,
            null,
            null,
            new SettingsConfig(4_000, null, (Boolean) null, null),
            null
        );
        TransformConfig updatedConfig = update.apply(config);

        // for settings we allow partial updates, so changing 1 setting should not overwrite the other
        // the parser handles explicit nulls, tested in @link{SettingsConfigTests}
        assertThat(updatedConfig.getSettings().getMaxPageSearchSize(), equalTo(4_000));
        assertThat(updatedConfig.getSettings().getDocsPerSecond(), equalTo(config.getSettings().getDocsPerSecond()));

        update = new TransformConfigUpdate(null, null, null, null, null, new SettingsConfig(null, 43.244F, (Boolean) null, null), null);
        updatedConfig = update.apply(updatedConfig);
        assertThat(updatedConfig.getSettings().getMaxPageSearchSize(), equalTo(4_000));
        assertThat(updatedConfig.getSettings().getDocsPerSecond(), equalTo(43.244F));

        // now reset to default using the magic -1
        update = new TransformConfigUpdate(null, null, null, null, null, new SettingsConfig(-1, null, (Boolean) null, null), null);
        updatedConfig = update.apply(updatedConfig);
        assertNull(updatedConfig.getSettings().getMaxPageSearchSize());
        assertThat(updatedConfig.getSettings().getDocsPerSecond(), equalTo(43.244F));

        update = new TransformConfigUpdate(null, null, null, null, null, new SettingsConfig(-1, -1F, (Boolean) null, null), null);
        updatedConfig = update.apply(updatedConfig);
        assertNull(updatedConfig.getSettings().getMaxPageSearchSize());
        assertNull(updatedConfig.getSettings().getDocsPerSecond());
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
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            SettingsConfigTests.randomNonEmptySettingsConfig(),
            randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.CURRENT.toString()
        );

        TransformConfigUpdate update = new TransformConfigUpdate(
            null,
            null,
            null,
            TimeSyncConfigTests.randomTimeSyncConfig(),
            null,
            null,
            null
        );

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
            randomSyncConfig(),
            null,
            PivotConfigTests.randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            SettingsConfigTests.randomNonEmptySettingsConfig(),
            randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.CURRENT.toString()
        );

        TransformConfigUpdate fooSyncUpdate = new TransformConfigUpdate(null, null, null, new FooSync(), null, null, null);
        ex = expectThrows(ElasticsearchStatusException.class, () -> fooSyncUpdate.apply(timeSyncedConfig));
        assertThat(
            ex.getMessage(),
            equalTo("Cannot change the current sync configuration of transform [time-transform] from [time] to [foo]")
        );

    }

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            TransformConfigUpdateTests::randomTransformConfigUpdate,
            this::toXContent,
            TransformConfigUpdate::fromXContent
        ).supportsUnknownFields(false).assertEqualsConsumer(this::assertEqualInstances).test();
    }

    private void toXContent(TransformConfigUpdate update, XContentBuilder builder) throws IOException {
        builder.startObject();
        if (update.getSource() != null) {
            builder.field(TransformField.SOURCE.getPreferredName(), update.getSource());
        }
        if (update.getDestination() != null) {
            builder.field(TransformField.DESTINATION.getPreferredName(), update.getDestination());
        }
        if (update.getFrequency() != null) {
            builder.field(TransformField.FREQUENCY.getPreferredName(), update.getFrequency().getStringRep());
        }
        if (update.getSyncConfig() != null) {
            builder.startObject(TransformField.SYNC.getPreferredName());
            builder.field(update.getSyncConfig().getWriteableName(), update.getSyncConfig());
            builder.endObject();
        }
        if (update.getDescription() != null) {
            builder.field(TransformField.DESCRIPTION.getPreferredName(), update.getDescription());
        }
        if (update.getSettings() != null) {
            builder.field(TransformField.SETTINGS.getPreferredName(), update.getSettings());
        }
        if (update.getRetentionPolicyConfig() != null) {
            builder.startObject(TransformField.RETENTION_POLICY.getPreferredName());
            builder.field(update.getRetentionPolicyConfig().getWriteableName(), update.getRetentionPolicyConfig());
            builder.endObject();
        }

        builder.endObject();
    }

    private static SyncConfig randomSyncConfig() {
        return TimeSyncConfigTests.randomTimeSyncConfig();
    }

    private static RetentionPolicyConfig randomRetentionPolicyConfig() {
        return TimeRetentionPolicyConfigTests.randomTimeRetentionPolicyConfig();
    }

    static class FooSync implements SyncConfig {

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
        public String getField() {
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
