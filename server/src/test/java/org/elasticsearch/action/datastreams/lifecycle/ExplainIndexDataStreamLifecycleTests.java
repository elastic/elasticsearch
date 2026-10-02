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
import org.elasticsearch.test.TransportVersionUtils;
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
                    : null,
                null,
                false
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
                    : null,
                null,
                false
            );
            assertThat(explainIndexDataStreamLifecycle.getGenerationTime(() -> now + 500L), is(TimeValue.timeValueMillis(400)));
        }
        {
            // null for unmanaged index
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = ExplainIndexDataStreamLifecycle.unmanagedIndex("my-index");
            assertThat(indexDataStreamLifecycle.getGenerationTime(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                true,
                randomBoolean(),
                now,
                now + 80L,
                TimeValue.timeValueMillis(now + 100L),
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                null,
                null,
                false
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
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = ExplainIndexDataStreamLifecycle.unmanagedIndex("my-index");
            assertThat(indexDataStreamLifecycle.getTimeSinceIndexCreation(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                true,
                randomBoolean(),
                now + 80L,
                null,
                null,
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                null,
                null,
                false
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
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = ExplainIndexDataStreamLifecycle.unmanagedIndex("my-index");
            assertThat(indexDataStreamLifecycle.getTimeSinceRollover(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataStreamLifecycle indexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                true,
                randomBoolean(),
                now - 50L,
                now + 100L,
                TimeValue.timeValueMillis(now),
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                null,
                null,
                false
            );
            assertThat(indexDataStreamLifecycle.getTimeSinceRollover(() -> now), is(TimeValue.ZERO));
        }
    }

    @SuppressWarnings("unchecked")
    public void testFrozenTransitionXContent() throws IOException {
        ExplainIndexDataStreamLifecycle withFrozen = new ExplainIndexDataStreamLifecycle(
            "my-index",
            true,
            randomBoolean(),
            System.currentTimeMillis(),
            null,
            null,
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
            null,
            FrozenTransitionStatus.NOT_SUPPORTED,
            false
        );
        Map<String, Object> withFrozenMap = getXContentMap(withFrozen, null, null);
        assertThat(withFrozenMap.get("frozen_transition_status"), is("not_supported"));
        assertThat(withFrozenMap.containsKey("frozen"), is(false));

        ExplainIndexDataStreamLifecycle withoutFrozen = new ExplainIndexDataStreamLifecycle(
            "my-index",
            true,
            randomBoolean(),
            System.currentTimeMillis(),
            null,
            null,
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
            null,
            null,
            false
        );
        Map<String, Object> withoutFrozenMap = getXContentMap(withoutFrozen, null, null);
        assertThat(withoutFrozenMap.containsKey("frozen_transition_status"), is(false));
    }

    public void testOldTransportVersionDropsFrozenTransitionStatus() throws IOException {
        ExplainIndexDataStreamLifecycle withFrozen = new ExplainIndexDataStreamLifecycle(
            "my-index",
            true,
            randomBoolean(),
            System.currentTimeMillis(),
            null,
            null,
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
            null,
            FrozenTransitionStatus.RUNNING,
            false
        );
        ExplainIndexDataStreamLifecycle roundTripped = copyInstance(
            withFrozen,
            TransportVersionUtils.randomVersionNotSupporting(ExplainIndexDataStreamLifecycle.EXPLAIN_INDEX_FROZEN_TRANSITION)
        );
        assertThat(roundTripped.getFrozenTransitionStatus(), is(nullValue()));
    }

    @SuppressWarnings("unchecked")
    public void testLifecycleEnabledByDefaultXContent() throws IOException {
        {
            // an index managed by the default lifecycle reports it and has no configured lifecycle to display
            ExplainIndexDataStreamLifecycle enabledByDefault = createManagedIndexDataStreamLifecycleExplanation(
                System.currentTimeMillis(),
                null,
                randomBoolean(),
                randomFrozenTransitionStatusOrNull(),
                true
            );
            assertThat(enabledByDefault.isLifecycleEnabledByDefault(), is(true));
            Map<String, Object> resultMap = getXContentMap(enabledByDefault, null, null);
            assertThat(resultMap.get("managed_by_lifecycle"), is(true));
            assertThat(resultMap.get("lifecycle_enabled_by_default"), is(true));
            assertThat(resultMap.containsKey("lifecycle"), is(false));
        }
        {
            // an index managed by a configured lifecycle does not report the lifecycle as enabled by default
            ExplainIndexDataStreamLifecycle configured = createManagedIndexDataStreamLifecycleExplanation(
                System.currentTimeMillis(),
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
                randomBoolean(),
                randomFrozenTransitionStatusOrNull(),
                false
            );
            assertThat(configured.isLifecycleEnabledByDefault(), is(false));
            Map<String, Object> resultMap = getXContentMap(configured, null, null);
            assertThat(resultMap.containsKey("lifecycle_enabled_by_default"), is(false));
            assertThat(((Map<String, Object>) resultMap.get("lifecycle")).get("enabled"), is(true));
        }
        {
            // an unmanaged index displays only its name and that it is not managed
            ExplainIndexDataStreamLifecycle unmanaged = ExplainIndexDataStreamLifecycle.unmanagedIndex("my-index");
            assertThat(unmanaged.isLifecycleEnabledByDefault(), is(false));
            Map<String, Object> resultMap = getXContentMap(unmanaged, null, null);
            assertThat(resultMap, equalTo(Map.of("index", "my-index", "managed_by_lifecycle", false)));
        }
    }

    public void testUnmanagedIndex() throws IOException {
        ExplainIndexDataStreamLifecycle unmanaged = ExplainIndexDataStreamLifecycle.unmanagedIndex("my-index");
        assertThat(unmanaged.getIndex(), is("my-index"));
        assertThat(unmanaged.isManagedByLifecycle(), is(false));
        assertThat(unmanaged.getIndexCreationDate(), is(nullValue()));
        assertThat(unmanaged.getRolloverDate(), is(nullValue()));
        assertThat(unmanaged.getLifecycle(), is(nullValue()));
        assertThat(unmanaged.getError(), is(nullValue()));
        assertThat(unmanaged.getFrozenTransitionStatus(), is(nullValue()));
        assertThat(unmanaged.isLifecycleEnabledByDefault(), is(false));
        assertThat(copyInstance(unmanaged), equalTo(unmanaged));
    }

    public void testLifecycleEnabledByDefaultSerialization() throws IOException {
        ExplainIndexDataStreamLifecycle enabledByDefault = createManagedIndexDataStreamLifecycleExplanation(
            System.currentTimeMillis(),
            null,
            randomBoolean(),
            randomFrozenTransitionStatusOrNull(),
            true
        );
        {
            ExplainIndexDataStreamLifecycle roundTripped = copyInstance(
                enabledByDefault,
                TransportVersionUtils.randomVersionSupporting(ExplainIndexDataStreamLifecycle.EXPLAIN_INDEX_DEFAULT_LIFECYCLE)
            );
            assertThat(roundTripped.isLifecycleEnabledByDefault(), is(true));
            assertThat(roundTripped, equalTo(enabledByDefault));
        }
        {
            // older nodes are not aware of the default lifecycle, so the flag is dropped
            ExplainIndexDataStreamLifecycle roundTripped = copyInstance(
                enabledByDefault,
                TransportVersionUtils.randomVersionNotSupporting(ExplainIndexDataStreamLifecycle.EXPLAIN_INDEX_DEFAULT_LIFECYCLE)
            );
            assertThat(roundTripped.isLifecycleEnabledByDefault(), is(false));
            assertThat(roundTripped.isManagedByLifecycle(), is(true));
            assertThat(roundTripped.getIndex(), is(enabledByDefault.getIndex()));
        }
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws Exception {
        TimeValue configuredRetention = TimeValue.timeValueDays(100);
        TimeValue globalDefaultRetention = TimeValue.timeValueDays(10);
        TimeValue globalMaxRetention = TimeValue.timeValueDays(50);
        DataStreamLifecycle dataStreamLifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .enabled(true)
            .dataRetention(configuredRetention)
            .build();
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
        return randomManagedIndexDataStreamLifecycleExplanation();
    }

    @Override
    protected ExplainIndexDataStreamLifecycle mutateInstance(ExplainIndexDataStreamLifecycle instance) throws IOException {
        return randomManagedIndexDataStreamLifecycleExplanation();
    }

    private static ExplainIndexDataStreamLifecycle randomManagedIndexDataStreamLifecycleExplanation() {
        DataStreamLifecycle lifecycle = randomBoolean() ? DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE : null;
        return createManagedIndexDataStreamLifecycleExplanation(
            System.nanoTime(),
            lifecycle,
            randomBoolean(),
            randomFrozenTransitionStatusOrNull(),
            // the lifecycle can be enabled by default only when there is no configured lifecycle
            lifecycle == null && randomBoolean()
        );
    }

    private static FrozenTransitionStatus randomFrozenTransitionStatusOrNull() {
        return randomBoolean() ? randomFrom(FrozenTransitionStatus.values()) : null;
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
        return createManagedIndexDataStreamLifecycleExplanation(now, lifecycle, isSystemDataStream, null, false);
    }

    private static ExplainIndexDataStreamLifecycle createManagedIndexDataStreamLifecycleExplanation(
        long now,
        @Nullable DataStreamLifecycle lifecycle,
        boolean isSystemDataStream,
        @Nullable FrozenTransitionStatus frozenTransitionStatus,
        boolean lifecycleEnabledByDefault
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
                : null,
            frozenTransitionStatus,
            lifecycleEnabledByDefault
        );
    }

}
