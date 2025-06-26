/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ExplainIndexDataStreamLifecycleTests extends AbstractWireSerializingTestCase<ExplainIndexDataStreamLifecycle> {

    public void testGetGenerationTime() {
        long now = System.currentTimeMillis();
        {
            ExplainIndexDataStreamLifecycle explainIndexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                randomAlphaOfLengthBetween(10, 30),
                true,
                randomBoolean(),
                now,
                randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
                null,
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                randomBoolean()
                    ? new ErrorEntry(
                        System.currentTimeMillis(),
                        new NullPointerException("bad times").getMessage(),
                        System.currentTimeMillis(),
                        randomIntBetween(0, 30)
                    )
                    : null
            );
            assertThat(explainIndexDataStreamLifecycle.getGenerationTime(() -> now + 50L), is(nullValue()));
            explainIndexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                randomAlphaOfLengthBetween(10, 30),
                true,
                randomBoolean(),
                now,
                randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
                TimeValue.timeValueMillis(now + 100),
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                randomBoolean()
                    ? new ErrorEntry(
                        System.currentTimeMillis(),
                        new NullPointerException("bad times").getMessage(),
                        System.currentTimeMillis(),
                        randomIntBetween(0, 30)
                    )
                    : null
            );
            assertThat(explainIndexDataStreamLifecycle.getGenerationTime(() -> now + 500L), is(TimeValue.timeValueMillis(400)));
        }
        {
            // null for unmanaged index
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                false,
                randomBoolean(),
                null,
                null,
                null,
                null,
                null
            );
            assertThat(indexDataStreamLifecycle.getGenerationTime(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                true,
                randomBoolean(),
                now,
                now + 80L, // rolled over in the future (clocks are funny that way)
                TimeValue.timeValueMillis(now + 100L),
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                null
            );
            assertThat(indexDataStreamLifecycle.getGenerationTime(() -> now), is(TimeValue.ZERO));
        }
    }

    public void testGetTimeSinceIndexCreation() {
        long now = System.currentTimeMillis();
        {
            ExplainIndexDataStreamLifecycle randomIndexDataStreamLifecycleExplanation = createManagedIndexDataStreamLifecycleExplanation(
                now,
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
            );
            assertThat(
                randomIndexDataStreamLifecycleExplanation.getTimeSinceIndexCreation(() -> now + 75L),
                is(TimeValue.timeValueMillis(75))
            );
        }
        {
            // null for unmanaged index
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                false,
                randomBoolean(),
                null,
                null,
                null,
                null,
                null
            );
            assertThat(indexDataStreamLifecycle.getTimeSinceIndexCreation(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                true,
                randomBoolean(),
                now + 80L, // created in the future (clocks are funny that way)
                null,
                null,
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                null
            );
            assertThat(indexDataStreamLifecycle.getTimeSinceIndexCreation(() -> now), is(TimeValue.ZERO));
        }
    }

    public void testGetTimeSinceRollover() {
        long now = System.currentTimeMillis();
        {
            ExplainIndexDataStreamLifecycle randomIndexDataStreamLifecycleExplanation = createManagedIndexDataStreamLifecycleExplanation(
                now,
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
            );
            if (randomIndexDataStreamLifecycleExplanation.getRolloverDate() == null) {
                // age calculated since creation date
                assertThat(randomIndexDataStreamLifecycleExplanation.getTimeSinceRollover(() -> now + 50L), is(nullValue()));
            } else {
                assertThat(
                    randomIndexDataStreamLifecycleExplanation.getTimeSinceRollover(
                        () -> randomIndexDataStreamLifecycleExplanation.getRolloverDate() + 75L
                    ),
                    is(TimeValue.timeValueMillis(75))
                );
            }
        }
        {
            // null for unmanaged index
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                false,
                randomBoolean(),
                null,
                null,
                null,
                null,
                null
            );
            assertThat(indexDataStreamLifecycle.getTimeSinceRollover(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                true,
                randomBoolean(),
                now - 50L,
                now + 100L, // rolled over in the future
                TimeValue.timeValueMillis(now),
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                null
            );
            assertThat(indexDataStreamLifecycle.getTimeSinceRollover(() -> now), is(TimeValue.ZERO));
        }
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws Exception {
        TimeValue configuredRetention = TimeValue.timeValueDays(100);
        TimeValue globalDefaultRetention = TimeValue.timeValueDays(10);
        TimeValue globalMaxRetention = TimeValue.timeValueDays(50);
        DataStreamLifecycle dataStreamLifecycle = DataStreamLifecycle.createDataLifecycle(true, configuredRetention, null);
        {
            boolean isSystemDataStream = true;
            ExplainIndexDataStreamLifecycle explainIndexDataStreamLifecycle = createManagedIndexDataStreamLifecycleExplanation(
                System.currentTimeMillis(),
                dataStreamLifecycle,
                isSystemDataStream
            );
            Map<String, Object> resultMap = getXContentMap(explainIndexDataStreamLifecycle, globalDefaultRetention, globalMaxRetention);
            Map<String, Object> lifecycleResult = (Map<String, Object>) resultMap.get("lifecycle");
            assertThat(lifecycleResult.get("data_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("effective_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("retention_determined_by"), equalTo("data_stream_configuration"));
        }
        {
            boolean isSystemDataStream = false;
            ExplainIndexDataStreamLifecycle explainIndexDataStreamLifecycle = createManagedIndexDataStreamLifecycleExplanation(
                System.currentTimeMillis(),
                dataStreamLifecycle,
                isSystemDataStream
            );
            Map<String, Object> resultMap = getXContentMap(explainIndexDataStreamLifecycle, globalDefaultRetention, globalMaxRetention);
            Map<String, Object> lifecycleResult = (Map<String, Object>) resultMap.get("lifecycle");
            assertThat(lifecycleResult.get("data_retention"), equalTo(configuredRetention.getStringRep()));
            assertThat(lifecycleResult.get("effective_retention"), equalTo(globalMaxRetention.getStringRep()));
            assertThat(lifecycleResult.get("retention_determined_by"), equalTo("max_global_retention"));
        }
    }

    /*
     * Calls toXContent on the given explainIndexDataStreamLifecycle, and converts the response to a Map
     */
    private Map<String, Object> getXContentMap(
        ExplainIndexDataStreamLifecycle explainIndexDataStreamLifecycle,
        TimeValue globalDefaultRetention,
        TimeValue globalMaxRetention
    ) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            ToXContent.Params params = new ToXContent.MapParams(DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAMS);
            RolloverConfiguration rolloverConfiguration = null;
            DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(globalDefaultRetention, globalMaxRetention);
            explainIndexDataStreamLifecycle.toXContent(builder, params, rolloverConfiguration, globalRetention);
            String serialized = Strings.toString(builder);
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), serialized, randomBoolean());
        }
    }

    @Override
    protected Writeable.Reader<ExplainIndexDataStreamLifecycle> instanceReader() {
        return ExplainIndexDataStreamLifecycle::new;
    }

    @Override
    protected ExplainIndexDataStreamLifecycle createTestInstance() {
        return createManagedIndexDataStreamLifecycleExplanation(
            System.nanoTime(),
            randomBoolean() ? DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE : null
        );
    }

    @Override
    protected ExplainIndexDataStreamLifecycle mutateInstance(ExplainIndexDataStreamLifecycle instance) throws IOException {
        return createManagedIndexDataStreamLifecycleExplanation(
            System.nanoTime(),
            randomBoolean() ? DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE : null
        );
    }

    private static ExplainIndexDataStreamLifecycle createManagedIndexDataStreamLifecycleExplanation(
        long now,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        return createManagedIndexDataStreamLifecycleExplanation(now, lifecycle, randomBoolean());
    }

    private static ExplainIndexDataStreamLifecycle createManagedIndexDataStreamLifecycleExplanation(
        long now,
        @Nullable DataStreamLifecycle lifecycle,
        boolean isSystemDataStream
    ) {
        return new ExplainIndexDataStreamLifecycle(
            randomAlphaOfLengthBetween(10, 30),
            true,
            isSystemDataStream,
            now,
            randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
            TimeValue.timeValueMillis(now),
            lifecycle,
            randomBoolean()
                ? new ErrorEntry(
                    System.currentTimeMillis(),
                    new NullPointerException("bad times").getMessage(),
                    System.currentTimeMillis(),
                    randomIntBetween(0, 30)
                )
                : null
        );
    }

}
