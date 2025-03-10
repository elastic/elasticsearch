/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.BasicDataStreamLifecycle.RetentionSource.DATA_STREAM_CONFIGURATION;
import static org.elasticsearch.cluster.metadata.BasicDataStreamLifecycle.RetentionSource.DEFAULT_GLOBAL_RETENTION;
import static org.elasticsearch.cluster.metadata.BasicDataStreamLifecycle.RetentionSource.MAX_GLOBAL_RETENTION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class BasicDataStreamLifecycleTests extends ESTestCase {

    public void testXContentSerializationWithEffectiveRetention() throws IOException {
        BasicDataStreamLifecycle lifecycle = randomBasicLifecycle();
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            DataStreamGlobalRetention globalRetention = DataStreamGlobalRetentionTests.randomGlobalRetention();
            ToXContent.Params withEffectiveRetention = new ToXContent.MapParams(DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAMS);
            builder.startObject();
            lifecycle.retentionToXContentFragment(builder, withEffectiveRetention, globalRetention, false);
            builder.endObject();
            String serialized = Strings.toString(builder);
            // We check that even if there was no retention provided by the user, the global retention applies
            if (lifecycle.dataRetention() == null) {
                assertThat(serialized, not(containsString("data_retention")));
            } else {
                assertThat(serialized, containsString("data_retention"));
            }
            boolean globalRetentionIsNotNull = globalRetention.defaultRetention() != null || globalRetention.maxRetention() != null;
            boolean configuredLifeCycleIsNotNull = lifecycle.dataRetention() != null;
            if (lifecycle.isEnabled() && (globalRetentionIsNotNull || configuredLifeCycleIsNotNull)) {
                assertThat(serialized, containsString("effective_retention"));
            } else {
                assertThat(serialized, not(containsString("effective_retention")));
            }
        }
    }

    public void testEffectiveRetention() {
        // No retention in the data stream lifecycle
        {
            BasicDataStreamLifecycle noRetentionLifecycle = createBasicLifecycle(true, null);
            TimeValue maxRetention = TimeValue.timeValueDays(randomIntBetween(50, 100));
            TimeValue defaultRetention = TimeValue.timeValueDays(randomIntBetween(1, 50));
            Tuple<TimeValue, BasicDataStreamLifecycle.RetentionSource> effectiveDataRetentionWithSource = noRetentionLifecycle
                .getEffectiveDataRetentionWithSource(null, randomBoolean());
            assertThat(effectiveDataRetentionWithSource.v1(), nullValue());
            assertThat(effectiveDataRetentionWithSource.v2(), equalTo(DATA_STREAM_CONFIGURATION));

            effectiveDataRetentionWithSource = noRetentionLifecycle.getEffectiveDataRetentionWithSource(
                new DataStreamGlobalRetention(null, maxRetention),
                false
            );
            assertThat(effectiveDataRetentionWithSource.v1(), equalTo(maxRetention));
            assertThat(effectiveDataRetentionWithSource.v2(), equalTo(MAX_GLOBAL_RETENTION));

            effectiveDataRetentionWithSource = noRetentionLifecycle.getEffectiveDataRetentionWithSource(
                new DataStreamGlobalRetention(defaultRetention, null),
                false
            );
            assertThat(effectiveDataRetentionWithSource.v1(), equalTo(defaultRetention));
            assertThat(effectiveDataRetentionWithSource.v2(), equalTo(DEFAULT_GLOBAL_RETENTION));

            effectiveDataRetentionWithSource = noRetentionLifecycle.getEffectiveDataRetentionWithSource(
                new DataStreamGlobalRetention(defaultRetention, maxRetention),
                false
            );
            assertThat(effectiveDataRetentionWithSource.v1(), equalTo(defaultRetention));
            assertThat(effectiveDataRetentionWithSource.v2(), equalTo(DEFAULT_GLOBAL_RETENTION));
        }

        // With retention in the data stream lifecycle
        {
            TimeValue dataStreamRetention = TimeValue.timeValueDays(randomIntBetween(5, 100));
            BasicDataStreamLifecycle lifecycleRetention = createBasicLifecycle(true, dataStreamRetention);
            TimeValue defaultRetention = TimeValue.timeValueDays(randomIntBetween(1, (int) dataStreamRetention.getDays() - 1));

            Tuple<TimeValue, BasicDataStreamLifecycle.RetentionSource> effectiveDataRetentionWithSource = lifecycleRetention
                .getEffectiveDataRetentionWithSource(null, randomBoolean());
            assertThat(effectiveDataRetentionWithSource.v1(), equalTo(dataStreamRetention));
            assertThat(effectiveDataRetentionWithSource.v2(), equalTo(DATA_STREAM_CONFIGURATION));

            effectiveDataRetentionWithSource = lifecycleRetention.getEffectiveDataRetentionWithSource(
                new DataStreamGlobalRetention(defaultRetention, null),
                false
            );
            assertThat(effectiveDataRetentionWithSource.v1(), equalTo(dataStreamRetention));
            assertThat(effectiveDataRetentionWithSource.v2(), equalTo(DATA_STREAM_CONFIGURATION));

            TimeValue maxGlobalRetention = randomBoolean() ? dataStreamRetention : TimeValue.timeValueDays(dataStreamRetention.days() + 1);
            effectiveDataRetentionWithSource = lifecycleRetention.getEffectiveDataRetentionWithSource(
                new DataStreamGlobalRetention(defaultRetention, maxGlobalRetention),
                false
            );
            assertThat(effectiveDataRetentionWithSource.v1(), equalTo(dataStreamRetention));
            assertThat(effectiveDataRetentionWithSource.v2(), equalTo(DATA_STREAM_CONFIGURATION));

            TimeValue maxRetentionLessThanDataStream = TimeValue.timeValueDays(dataStreamRetention.days() - 1);
            effectiveDataRetentionWithSource = lifecycleRetention.getEffectiveDataRetentionWithSource(
                new DataStreamGlobalRetention(
                    randomBoolean()
                        ? null
                        : TimeValue.timeValueDays(randomIntBetween(1, (int) (maxRetentionLessThanDataStream.days() - 1))),
                    maxRetentionLessThanDataStream
                ),
                false
            );
            assertThat(effectiveDataRetentionWithSource.v1(), equalTo(maxRetentionLessThanDataStream));
            assertThat(effectiveDataRetentionWithSource.v2(), equalTo(MAX_GLOBAL_RETENTION));
        }

        // Global retention does not apply to internal data streams
        {
            // Pick the values in such a way that global retention should have kicked in
            boolean dataStreamWithRetention = randomBoolean();
            TimeValue dataStreamRetention = dataStreamWithRetention ? TimeValue.timeValueDays(365) : null;
            DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(
                TimeValue.timeValueDays(7),
                TimeValue.timeValueDays(90)
            );
            BasicDataStreamLifecycle lifecycle = createBasicLifecycle(true, dataStreamRetention);

            // Verify that global retention should have kicked in
            var effectiveDataRetentionWithSource = lifecycle.getEffectiveDataRetentionWithSource(globalRetention, false);
            if (dataStreamWithRetention) {
                assertThat(effectiveDataRetentionWithSource.v1(), equalTo(globalRetention.maxRetention()));
                assertThat(effectiveDataRetentionWithSource.v2(), equalTo(MAX_GLOBAL_RETENTION));
            } else {
                assertThat(effectiveDataRetentionWithSource.v1(), equalTo(globalRetention.defaultRetention()));
                assertThat(effectiveDataRetentionWithSource.v2(), equalTo(DEFAULT_GLOBAL_RETENTION));
            }

            // Now verify that internal data streams do not use global retention
            // Verify that global retention should have kicked in
            effectiveDataRetentionWithSource = lifecycle.getEffectiveDataRetentionWithSource(globalRetention, true);
            if (dataStreamWithRetention) {
                assertThat(effectiveDataRetentionWithSource.v1(), equalTo(dataStreamRetention));
            } else {
                assertThat(effectiveDataRetentionWithSource.v1(), nullValue());
            }
            assertThat(effectiveDataRetentionWithSource.v2(), equalTo(DATA_STREAM_CONFIGURATION));
        }
    }

    public void testEffectiveRetentionParams() {
        Map<String, String> initialParams = randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        ToXContent.Params params = BasicDataStreamLifecycle.addEffectiveRetentionParams(new ToXContent.MapParams(initialParams));
        assertThat(params.paramAsBoolean(DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAM_NAME, false), equalTo(true));
        for (String key : initialParams.keySet()) {
            assertThat(initialParams.get(key), equalTo(params.param(key)));
        }
    }

    private BasicDataStreamLifecycle randomBasicLifecycle() {
        return createBasicLifecycle(randomBoolean(), randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(1, 10)));
    }

    private BasicDataStreamLifecycle createBasicLifecycle(boolean enabled, TimeValue retention) {
        return new BasicDataStreamLifecycle() {

            @Override
            public TimeValue dataRetention() {
                return retention;
            }

            @Override
            public boolean isEnabled() {
                return enabled;
            }
        };
    }
}
