/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ResettableValue;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.test.ESIntegTestCase.client;
import static org.elasticsearch.test.ESTestCase.frequently;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.junit.Assert.assertTrue;

/**
 * Provides helper methods that can be used to tests. Examples of the functionalities it provides are:
 * - random lifecycle generation
 * - putting a composable template
 * - creating a data stream model
 */
public class DataStreamLifecycleFixtures {

    public static DataStream createDataStream(
        Metadata.Builder builder,
        String dataStreamName,
        int backingIndicesCount,
        Settings.Builder backingIndicesSettings,
        @Nullable DataStreamLifecycle lifecycle,
        Long now
    ) {
        return createDataStream(builder, dataStreamName, backingIndicesCount, 0, backingIndicesSettings, lifecycle, null, now);
    }

    public static DataStream createDataStream(
        Metadata.Builder builder,
        String dataStreamName,
        int backingIndicesCount,
        int failureIndicesCount,
        Settings.Builder backingIndicesSettings,
        @Nullable DataStreamLifecycle dataLifecycle,
        @Nullable DataStreamLifecycle failuresLifecycle,
        Long now
    ) {
        final List<Index> backingIndices = new ArrayList<>();
        final List<Index> failureIndices = new ArrayList<>();
        for (int k = 1; k <= backingIndicesCount; k++) {
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k))
                .settings(backingIndicesSettings)
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(now - 3000L);
            if (k < backingIndicesCount) {
                // add rollover info only for non-write indices
                MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(now - 2000L));
                indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), now - 2000L));
            }
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            builder.put(indexMetadata, false);
            backingIndices.add(indexMetadata.getIndex());
        }
        for (int k = 1; k <= failureIndicesCount; k++) {
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultFailureStoreName(dataStreamName, k, now))
                .settings(backingIndicesSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(now - 3000L);
            if (k < failureIndicesCount) {
                // add rollover info only for non-write indices
                MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(now - 2000L));
                indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), now - 2000L));
            }
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            builder.put(indexMetadata, false);
            failureIndices.add(indexMetadata.getIndex());
        }
        return newInstance(
            dataStreamName,
            backingIndices,
            backingIndicesCount,
            null,
            false,
            dataLifecycle,
            failureIndices,
            new DataStreamOptions(
                DataStreamFailureStore.builder().enabled(failureIndices.isEmpty() == false).lifecycle(failuresLifecycle).build()
            )
        );
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamLifecycle.Template lifecycle
    ) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(
                    Template.builder()
                        .settings(settings)
                        .mappings(mappings == null ? null : CompressedXContent.fromJSON(mappings))
                        .lifecycle(lifecycle)
                )
                .metadata(metadata)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertTrue(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet().isAcknowledged());
    }

    static DataStreamLifecycle.Template randomDataLifecycleTemplate() {
        return DataStreamLifecycle.createDataLifecycleTemplate(
            frequently(),
            randomResettable(ESTestCase::randomTimeValue),
            randomResettable(DataStreamLifecycleFixtures::randomDownsamplingRounds)
        );
    }

    private static <T> ResettableValue<T> randomResettable(Supplier<T> supplier) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> ResettableValue.undefined();
            case 1 -> ResettableValue.reset();
            case 2 -> ResettableValue.create(supplier.get());
            default -> throw new IllegalStateException("Unknown randomisation path");
        };
    }

    private static List<DataStreamLifecycle.DownsamplingRound> randomDownsamplingRounds() {
        var count = randomIntBetween(0, 9);
        List<DataStreamLifecycle.DownsamplingRound> rounds = new ArrayList<>();
        var previous = new DataStreamLifecycle.DownsamplingRound(
            TimeValue.timeValueDays(randomIntBetween(1, 365)),
            new DownsampleConfig(new DateHistogramInterval(randomIntBetween(1, 24) + "h"))
        );
        rounds.add(previous);
        for (int i = 0; i < count; i++) {
            DataStreamLifecycle.DownsamplingRound round = nextRound(previous);
            rounds.add(round);
            previous = round;
        }
        return rounds;
    }

    private static DataStreamLifecycle.DownsamplingRound nextRound(DataStreamLifecycle.DownsamplingRound previous) {
        var after = TimeValue.timeValueDays(previous.after().days() + randomIntBetween(1, 10));
        var fixedInterval = new DownsampleConfig(
            new DateHistogramInterval((previous.config().getFixedInterval().estimateMillis() * randomIntBetween(2, 5)) + "ms")
        );
        return new DataStreamLifecycle.DownsamplingRound(after, fixedInterval);
    }
}
