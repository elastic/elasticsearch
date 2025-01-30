/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.AbstractWireSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests.randomMetadata;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests.randomRetentionPolicyConfig;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests.randomSyncConfig;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests.randomTransformConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TransformConfigUpdateTests extends AbstractWireSerializingTransformTestCase<TransformConfigUpdate> {

    public static TransformConfigUpdate randomTransformConfigUpdate() {
        return new TransformConfigUpdate(
            randomBoolean() ? null : randomSourceConfig(),
            randomBoolean() ? null : randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : SettingsConfigTests.randomSettingsConfig(),
            randomBoolean() ? null : randomMetadata(),
            randomBoolean() ? null : randomBoolean() ? randomRetentionPolicyConfig() : NullRetentionPolicyConfig.INSTANCE
        );
    }

    @Override
    protected TransformConfigUpdate createTestInstance() {
        return randomTransformConfigUpdate();
    }

    @Override
    protected TransformConfigUpdate mutateInstance(TransformConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<TransformConfigUpdate> instanceReader() {
        return TransformConfigUpdate::new;
    }

    public void testIsNoop() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            TransformConfig config = randomTransformConfig();
            TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null, null, null, null);
            assertTrue("null update should be no-op", update.isNoop(config));

            update = new TransformConfigUpdate(
                config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                config.getDescription(),
                config.getSettings(),
                config.getMetadata(),
                config.getRetentionPolicyConfig()
            );
            assertTrue("equal update should be no-op", update.isNoop(config));

            update = new TransformConfigUpdate(
                config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                "this is a new description",
                config.getSettings(),
                config.getMetadata(),
                config.getRetentionPolicyConfig()
            );
            assertFalse("true update should not be no-op", update.isNoop(config));
        }
    }

    public void testChangesSettings() {
        TransformConfig config = randomTransformConfig();
        TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null, null, null, null);
        assertFalse("null update does not change settings", update.changesSettings(config));

        update = new TransformConfigUpdate(null, null, null, null, null, config.getSettings(), null, null);
        assertFalse("equal update does not change settings", update.changesSettings(config));

        SettingsConfig newSettings = new SettingsConfig.Builder(config.getSettings()).setMaxPageSearchSize(
            Optional.ofNullable(config.getSettings().getMaxPageSearchSize()).orElse(0) + 1
        ).build();
        update = new TransformConfigUpdate(null, null, null, null, null, newSettings, null, null);
        assertTrue("true update changes settings", update.changesSettings(config));
    }

    public void testChangesHeaders() {
        TransformConfig config = randomTransformConfig();
        TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null, null, null, null);
        assertFalse("null update does not change headers", update.changesHeaders(config));

        update.setHeaders(config.getHeaders());
        assertFalse("equal update does not change headers", update.changesHeaders(config));

        Map<String, String> newHeaders = Map.of("new-key", "new-value");
        update.setHeaders(newHeaders);
        assertTrue("true update changes headers", update.changesHeaders(config));
    }

    public void testChangesDestIndex() {
        TransformConfig config = randomTransformConfig();
        TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null, null, null, null);
        assertFalse("null update does not change destination index", update.changesDestIndex(config));

        var newDestWithSameIndex = new DestConfig(config.getDestination().getIndex(), null, null);
        update = new TransformConfigUpdate(null, newDestWithSameIndex, null, null, null, null, null, null);
        assertFalse("equal update does not change destination index", update.changesDestIndex(config));

        var newDestWithNewIndex = new DestConfig(config.getDestination().getIndex() + "-new", null, null);
        update = new TransformConfigUpdate(null, newDestWithNewIndex, null, null, null, null, null, null);
        assertTrue("true update changes destination index", update.changesDestIndex(config));
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
            randomMetadata(),
            randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : TransformConfigVersion.V_7_2_0.toString()
        );
        TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null, null, null, null);

        assertThat(config, equalTo(update.apply(config)));
        SourceConfig sourceConfig = new SourceConfig("the_new_index");
        DestConfig destConfig = new DestConfig("the_new_dest", List.of(new DestAlias("my_new_alias", false)), "my_new_pipeline");
        TimeValue frequency = TimeValue.timeValueSeconds(10);
        SyncConfig syncConfig = new TimeSyncConfig("time_field", TimeValue.timeValueSeconds(30));
        String newDescription = "new description";
        SettingsConfig settings = new SettingsConfig.Builder().setMaxPageSearchSize(4_000)
            .setRequestsPerSecond(4_000.400F)
            .setDatesAsEpochMillis(true)
            .setAlignCheckpoints(true)
            .setUsePit(true)
            .setDeduceMappings(true)
            .setNumFailureRetries(10)
            .setUnattended(true)
            .build();
        Map<String, Object> newMetadata = randomMetadata();
        RetentionPolicyConfig retentionPolicyConfig = new TimeRetentionPolicyConfig("time_field", new TimeValue(60_000));
        update = new TransformConfigUpdate(
            sourceConfig,
            destConfig,
            frequency,
            syncConfig,
            newDescription,
            settings,
            newMetadata,
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
        // We only check for the existence of new entries. The map can also contain the old (random) entries.
        assertThat(updatedConfig.getMetadata(), equalTo(newMetadata));
        assertThat(updatedConfig.getRetentionPolicyConfig(), equalTo(retentionPolicyConfig));
        assertThat(updatedConfig.getHeaders(), equalTo(headers));
        assertThat(updatedConfig.getVersion(), equalTo(TransformConfigVersion.CURRENT));
    }

    public void testApplyRetentionPolicy() {
        TransformConfig config = TransformConfigTests.randomTransformConfig();

        RetentionPolicyConfig timeRetentionPolicyConfig = new TimeRetentionPolicyConfig("field", TimeValue.timeValueDays(1));
        TransformConfigUpdate setRetentionPolicy = new TransformConfigUpdate(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            timeRetentionPolicyConfig
        );
        config = setRetentionPolicy.apply(config);
        assertThat(config.getRetentionPolicyConfig(), is(equalTo(timeRetentionPolicyConfig)));

        TransformConfigUpdate clearRetentionPolicy = new TransformConfigUpdate(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            NullRetentionPolicyConfig.INSTANCE
        );
        config = clearRetentionPolicy.apply(config);
        assertThat(config.getRetentionPolicyConfig(), is(nullValue()));

        config = setRetentionPolicy.apply(config);
        assertThat(config.getRetentionPolicyConfig(), is(equalTo(timeRetentionPolicyConfig)));
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
            randomMetadata(),
            randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : TransformConfigVersion.V_7_2_0.toString()
        );

        TransformConfigUpdate update = new TransformConfigUpdate(
            null,
            null,
            null,
            null,
            null,
            new SettingsConfig.Builder().setMaxPageSearchSize(4_000).build(),
            null,
            null
        );
        TransformConfig updatedConfig = update.apply(config);

        // for settings we allow partial updates, so changing 1 setting should not overwrite the other
        // the parser handles explicit nulls, tested in @link{SettingsConfigTests}
        assertThat(updatedConfig.getSettings().getMaxPageSearchSize(), equalTo(4_000));
        assertThat(updatedConfig.getSettings().getDocsPerSecond(), equalTo(config.getSettings().getDocsPerSecond()));
        assertThat(updatedConfig.getSettings().getDatesAsEpochMillis(), equalTo(config.getSettings().getDatesAsEpochMillis()));
        assertThat(updatedConfig.getSettings().getAlignCheckpoints(), equalTo(config.getSettings().getAlignCheckpoints()));

        update = new TransformConfigUpdate(
            null,
            null,
            null,
            null,
            null,
            new SettingsConfig.Builder().setRequestsPerSecond(43.244F).build(),
            null,
            null
        );
        updatedConfig = update.apply(updatedConfig);
        assertThat(updatedConfig.getSettings().getMaxPageSearchSize(), equalTo(4_000));
        assertThat(updatedConfig.getSettings().getDocsPerSecond(), equalTo(43.244F));
        assertThat(updatedConfig.getSettings().getDatesAsEpochMillis(), equalTo(config.getSettings().getDatesAsEpochMillis()));
        assertThat(updatedConfig.getSettings().getAlignCheckpoints(), equalTo(config.getSettings().getAlignCheckpoints()));

        // now reset to default using the magic -1
        update = new TransformConfigUpdate(
            null,
            null,
            null,
            null,
            null,
            new SettingsConfig.Builder().setMaxPageSearchSize(null).build(),
            null,
            null
        );
        updatedConfig = update.apply(updatedConfig);
        assertNull(updatedConfig.getSettings().getMaxPageSearchSize());
        assertThat(updatedConfig.getSettings().getDocsPerSecond(), equalTo(43.244F));
        assertThat(updatedConfig.getSettings().getDatesAsEpochMillis(), equalTo(config.getSettings().getDatesAsEpochMillis()));
        assertThat(updatedConfig.getSettings().getAlignCheckpoints(), equalTo(config.getSettings().getAlignCheckpoints()));

        update = new TransformConfigUpdate(
            null,
            null,
            null,
            null,
            null,
            new SettingsConfig.Builder().setMaxPageSearchSize(null).setRequestsPerSecond(null).build(),
            null,
            null
        );
        updatedConfig = update.apply(updatedConfig);
        assertNull(updatedConfig.getSettings().getMaxPageSearchSize());
        assertNull(updatedConfig.getSettings().getDocsPerSecond());
        assertThat(updatedConfig.getSettings().getDatesAsEpochMillis(), equalTo(config.getSettings().getDatesAsEpochMillis()));
        assertThat(updatedConfig.getSettings().getAlignCheckpoints(), equalTo(config.getSettings().getAlignCheckpoints()));
    }

    public void testApplyMetadata() {
        Map<String, Object> oldMetadata = Map.of("foo", 123, "bar", 456);
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
            oldMetadata,
            randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : TransformConfigVersion.V_7_2_0.toString()
        );

        Map<String, Object> newMetadata = Map.of("bar", 789, "baz", 1000);
        TransformConfigUpdate update = new TransformConfigUpdate(null, null, null, null, null, null, newMetadata, null);
        TransformConfig updatedConfig = update.apply(config);

        // For metadata we apply full replace rather than partial update, so "foo" disappears.
        assertThat(updatedConfig.getMetadata(), equalTo(Map.of("bar", 789, "baz", 1000)));
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
            randomMetadata(),
            randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : TransformConfigVersion.CURRENT.toString()
        );

        TransformConfigUpdate update = new TransformConfigUpdate(
            null,
            null,
            null,
            TimeSyncConfigTests.randomTimeSyncConfig(),
            null,
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
            randomMetadata(),
            randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : TransformConfigVersion.CURRENT.toString()
        );

        TransformConfigUpdate fooSyncUpdate = new TransformConfigUpdate(null, null, null, new FooSync(), null, null, null, null);
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
        if (update.getMetadata() != null) {
            builder.field(TransformField.METADATA.getPreferredName(), update.getMetadata());
        }
        if (update.getRetentionPolicyConfig() != null) {
            if (NullRetentionPolicyConfig.INSTANCE.equals(update.getRetentionPolicyConfig())) {
                builder.nullField(TransformField.RETENTION_POLICY.getPreferredName());
            } else {
                builder.startObject(TransformField.RETENTION_POLICY.getPreferredName());
                builder.field(update.getRetentionPolicyConfig().getWriteableName(), update.getRetentionPolicyConfig());
                builder.endObject();
            }
        }

        builder.endObject();
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
