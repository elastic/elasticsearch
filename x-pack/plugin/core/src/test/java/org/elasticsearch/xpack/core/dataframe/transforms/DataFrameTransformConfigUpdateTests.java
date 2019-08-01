/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigTests.randomDataFrameTransformConfig;
import static org.elasticsearch.xpack.core.dataframe.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.dataframe.transforms.SourceConfigTests.randomSourceConfig;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameTransformConfigUpdateTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformConfigUpdate> {

    public static DataFrameTransformConfigUpdate randomDataFrameTransformConfigUpdate() {
        return new DataFrameTransformConfigUpdate(
            randomBoolean() ? null : randomSourceConfig(),
            randomBoolean() ? null : randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000));
    }

    public static SyncConfig randomSyncConfig() {
        return TimeSyncConfigTests.randomTimeSyncConfig();
    }

    @Override
    protected DataFrameTransformConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformConfigUpdate.fromXContent(parser);
    }

    @Override
    protected DataFrameTransformConfigUpdate createTestInstance() {
        return randomDataFrameTransformConfigUpdate();
    }

    @Override
    protected Reader<DataFrameTransformConfigUpdate> instanceReader() {
        return DataFrameTransformConfigUpdate::new;
    }

    public void testIsNoop() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            DataFrameTransformConfig config = randomDataFrameTransformConfig();
            DataFrameTransformConfigUpdate update = new DataFrameTransformConfigUpdate(null, null, null, null, null);
            assertTrue("null update is not noop", update.isNoop(config));
            update = new DataFrameTransformConfigUpdate(config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                config.getDescription());
            assertTrue("equal update is not noop", update.isNoop(config));

            update = new DataFrameTransformConfigUpdate(config.getSource(),
                config.getDestination(),
                config.getFrequency(),
                config.getSyncConfig(),
                "this is a new description");
            assertFalse("true update is noop", update.isNoop(config));
        }
    }

    public void testApply() {
        DataFrameTransformConfig config = randomDataFrameTransformConfig();
        DataFrameTransformConfigUpdate update = new DataFrameTransformConfigUpdate(null, null, null, null, null);

        assertThat(config, equalTo(update.apply(config)));
        SourceConfig sourceConfig = new SourceConfig("the_new_index");
        DestConfig destConfig = new DestConfig("the_new_dest", "my_new_pipeline");
        TimeValue frequency = TimeValue.timeValueSeconds(10);
        SyncConfig syncConfig = new TimeSyncConfig("time_field", TimeValue.timeValueSeconds(30));
        String newDescription = "new description";
        update = new DataFrameTransformConfigUpdate(sourceConfig, destConfig, frequency, syncConfig, newDescription);

        DataFrameTransformConfig updatedConfig = update.apply(config);

        assertThat(updatedConfig.getSource(), equalTo(sourceConfig));
        assertThat(updatedConfig.getDestination(), equalTo(destConfig));
        assertThat(updatedConfig.getFrequency(), equalTo(frequency));
        assertThat(updatedConfig.getSyncConfig(), equalTo(syncConfig));
        assertThat(updatedConfig.getDescription(), equalTo(newDescription));
    }
}
