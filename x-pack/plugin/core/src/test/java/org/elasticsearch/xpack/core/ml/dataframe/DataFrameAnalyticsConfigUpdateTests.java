/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests.randomValidId;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class DataFrameAnalyticsConfigUpdateTests extends AbstractSerializingTestCase<DataFrameAnalyticsConfigUpdate> {

    @Override
    protected DataFrameAnalyticsConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsConfigUpdate.PARSER.apply(parser, null).build();
    }

    @Override
    protected DataFrameAnalyticsConfigUpdate createTestInstance() {
        return randomUpdate(randomValidId());
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsConfigUpdate> instanceReader() {
        return DataFrameAnalyticsConfigUpdate::new;
    }

    public static DataFrameAnalyticsConfigUpdate randomUpdate(String id) {
        DataFrameAnalyticsConfigUpdate.Builder builder = new DataFrameAnalyticsConfigUpdate.Builder(id);
        if (randomBoolean()) {
            builder.setDescription(randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            builder.setModelMemoryLimit(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            builder.setAllowLazyStart(randomBoolean());
        }
        if (randomBoolean()) {
            builder.setMaxNumThreads(randomIntBetween(1, 20));
        }
        return builder.build();
    }

    public void testMergeWithConfig_UpdatedDescription() {
        String id = randomValidId();
        DataFrameAnalyticsConfig config =
            DataFrameAnalyticsConfigTests.createRandomBuilder(id).setDescription("old description").build();
        DataFrameAnalyticsConfigUpdate update =
            new DataFrameAnalyticsConfigUpdate.Builder(id).setDescription("new description").build();
        assertThat(
            update.mergeWithConfig(config).build(),
            is(equalTo(new DataFrameAnalyticsConfig.Builder(config).setDescription("new description").build())));
    }

    public void testMergeWithConfig_UpdatedModelMemoryLimit() {
        String id = randomValidId();
        DataFrameAnalyticsConfig config =
            DataFrameAnalyticsConfigTests.createRandomBuilder(id).setModelMemoryLimit(new ByteSizeValue(1024)).build();
        DataFrameAnalyticsConfigUpdate update =
            new DataFrameAnalyticsConfigUpdate.Builder(id).setModelMemoryLimit(new ByteSizeValue(2048)).build();
        assertThat(
            update.mergeWithConfig(config).build(),
            is(equalTo(new DataFrameAnalyticsConfig.Builder(config).setModelMemoryLimit(new ByteSizeValue(2048)).build())));
    }

    public void testMergeWithConfig_UpdatedAllowLazyStart() {
        String id = randomValidId();
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfigTests.createRandomBuilder(id).setAllowLazyStart(false).build();
        DataFrameAnalyticsConfigUpdate update = new DataFrameAnalyticsConfigUpdate.Builder(id).setAllowLazyStart(true).build();
        assertThat(
            update.mergeWithConfig(config).build(),
            is(equalTo(new DataFrameAnalyticsConfig.Builder(config).setAllowLazyStart(true).build())));
    }

    public void testMergeWithConfig_UpdatedMaxNumThreads() {
        String id = randomValidId();
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfigTests.createRandomBuilder(id).setMaxNumThreads(3).build();
        DataFrameAnalyticsConfigUpdate update = new DataFrameAnalyticsConfigUpdate.Builder(id).setMaxNumThreads(5).build();
        assertThat(
            update.mergeWithConfig(config).build(),
            is(equalTo(new DataFrameAnalyticsConfig.Builder(config).setMaxNumThreads(5).build())));
    }

    public void testMergeWithConfig_UpdatedAllUpdatableProperties() {
        String id = randomValidId();
        DataFrameAnalyticsConfig config =
            DataFrameAnalyticsConfigTests.createRandomBuilder(id)
                .setDescription("old description")
                .setModelMemoryLimit(new ByteSizeValue(1024))
                .setAllowLazyStart(false)
                .setMaxNumThreads(1)
                .build();
        DataFrameAnalyticsConfigUpdate update =
            new DataFrameAnalyticsConfigUpdate.Builder(id)
                .setDescription("new description")
                .setModelMemoryLimit(new ByteSizeValue(2048))
                .setAllowLazyStart(true)
                .setMaxNumThreads(4)
                .build();
        assertThat(
            update.mergeWithConfig(config).build(),
            is(equalTo(
                new DataFrameAnalyticsConfig.Builder(config)
                    .setDescription("new description")
                    .setModelMemoryLimit(new ByteSizeValue(2048))
                    .setAllowLazyStart(true)
                    .setMaxNumThreads(4)
                    .build())));
    }

    public void testMergeWithConfig_NoopUpdate() {
        String id = randomValidId();

        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfigTests.createRandom(id);
        DataFrameAnalyticsConfigUpdate update = new DataFrameAnalyticsConfigUpdate.Builder(id).build();
        assertThat(update.mergeWithConfig(config).build(), is(equalTo(config)));
    }

    public void testMergeWithConfig_GivenRandomUpdates_AssertImmutability() {
        String id = randomValidId();

        for (int i = 0; i < 100; ++i) {
            DataFrameAnalyticsConfig config = DataFrameAnalyticsConfigTests.createRandom(id);
            DataFrameAnalyticsConfigUpdate update;
            do {
                update = randomUpdate(id);
            } while (isNoop(config, update));

            assertThat(update.mergeWithConfig(config).build(), is(not(equalTo(config))));
        }
    }

    public void testMergeWithConfig_failBecauseTargetConfigHasDifferentId() {
        String id = randomValidId();

        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfigTests.createRandom(id);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> randomUpdate(id + "_2").mergeWithConfig(config));
        assertThat(e.getMessage(), containsString("different id"));
    }

    public void testRequiresRestart_DescriptionUpdateDoesNotRequireRestart() {
        String id = randomValidId();
        DataFrameAnalyticsConfig config =
            DataFrameAnalyticsConfigTests.createRandomBuilder(id).setDescription("old description").build();
        DataFrameAnalyticsConfigUpdate update =
            new DataFrameAnalyticsConfigUpdate.Builder(id).setDescription("new description").build();

        assertThat(update.requiresRestart(config), is(false));
    }

    public void testRequiresRestart_ModelMemoryLimitUpdateRequiresRestart() {
        String id = randomValidId();
        DataFrameAnalyticsConfig config =
            DataFrameAnalyticsConfigTests.createRandomBuilder(id).setModelMemoryLimit(new ByteSizeValue(1024)).build();
        DataFrameAnalyticsConfigUpdate update =
            new DataFrameAnalyticsConfigUpdate.Builder(id).setModelMemoryLimit(new ByteSizeValue(2048)).build();

        assertThat(update.requiresRestart(config), is(true));
    }

    public void testRequiresRestart_MaxNumThreadsUpdateRequiresRestart() {
        String id = randomValidId();
        DataFrameAnalyticsConfig config =
            DataFrameAnalyticsConfigTests.createRandomBuilder(id).setMaxNumThreads(1).build();
        DataFrameAnalyticsConfigUpdate update = new DataFrameAnalyticsConfigUpdate.Builder(id).setMaxNumThreads(8).build();

        assertThat(update.requiresRestart(config), is(true));
    }

    public void testCtor_GivenMaxNumberThreadsIsZero() {
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> new DataFrameAnalyticsConfigUpdate.Builder("test").setMaxNumThreads(0).build());

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("[max_num_threads] must be a positive integer"));
    }

    public void testCtor_GivenMaxNumberThreadsIsNegative() {
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> new DataFrameAnalyticsConfigUpdate.Builder("test").setMaxNumThreads(randomIntBetween(Integer.MIN_VALUE, 0)).build());

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("[max_num_threads] must be a positive integer"));
    }

    public void testGetUpdatedFields_GivenAll() {
        DataFrameAnalyticsConfigUpdate update = new DataFrameAnalyticsConfigUpdate.Builder("test_job")
            .setDescription("new description")
            .setModelMemoryLimit(new ByteSizeValue(1024))
            .setAllowLazyStart(true)
            .setMaxNumThreads(8)
            .build();

        assertThat(update.getUpdatedFields(), contains("allow_lazy_start", "description", "max_num_threads", "model_memory_limit"));
    }

    public void testGetUpdatedFields_GivenAllowLazyStart() {
        DataFrameAnalyticsConfigUpdate update = new DataFrameAnalyticsConfigUpdate.Builder("test_job")
            .setAllowLazyStart(false)
            .build();

        assertThat(update.getUpdatedFields(), contains("allow_lazy_start"));
    }

    public void testGetUpdatedFields_GivenDescription() {
        DataFrameAnalyticsConfigUpdate update = new DataFrameAnalyticsConfigUpdate.Builder("test_job")
            .setDescription("new description")
            .build();

        assertThat(update.getUpdatedFields(), contains("description"));
    }

    public void testGetUpdatedFields_GivenMaxNumThreads() {
        DataFrameAnalyticsConfigUpdate update = new DataFrameAnalyticsConfigUpdate.Builder("test_job")
            .setMaxNumThreads(3)
            .build();

        assertThat(update.getUpdatedFields(), contains("max_num_threads"));
    }

    public void testGetUpdatedFields_GivenModelMemoryLimit() {
        DataFrameAnalyticsConfigUpdate update = new DataFrameAnalyticsConfigUpdate.Builder("test_job")
            .setModelMemoryLimit(new ByteSizeValue(1024))
            .build();

        assertThat(update.getUpdatedFields(), contains("model_memory_limit"));
    }

    private boolean isNoop(DataFrameAnalyticsConfig config, DataFrameAnalyticsConfigUpdate update) {
        return (update.getDescription() == null || Objects.equals(config.getDescription(), update.getDescription()))
            && (update.getModelMemoryLimit() == null || Objects.equals(config.getModelMemoryLimit(), update.getModelMemoryLimit()))
            && (update.isAllowLazyStart() == null || Objects.equals(config.isAllowLazyStart(), update.isAllowLazyStart()))
            && (update.getMaxNumThreads() == null || Objects.equals(config.getMaxNumThreads(), update.getMaxNumThreads()));
    }
}
