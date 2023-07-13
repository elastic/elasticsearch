/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.lucene.tests.util.LuceneTestCase.rarely;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.test.ESIntegTestCase.client;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomMillisUpToYear9999;
import static org.junit.Assert.assertTrue;

/**
 * Provides helper methods that can be used to tests. Examples of the functionalities it provides are:
 * - random lifecycle generation
 * - putting a composable template
 * - creating a data stream model
 */
public class DLMFixtures {

    public static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    static DataStream createDataStream(
        Metadata.Builder builder,
        String dataStreamName,
        int backingIndicesCount,
        Settings.Builder backingIndicesSettings,
        @Nullable DataLifecycle lifecycle,
        Long now
    ) {
        final List<Index> backingIndices = new ArrayList<>();
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
        return newInstance(dataStreamName, backingIndices, backingIndicesCount, null, false, lifecycle);
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataLifecycle lifecycle
    ) throws IOException {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            new ComposableIndexTemplate(
                patterns,
                new Template(settings, mappings == null ? null : CompressedXContent.fromJSON(mappings), null, lifecycle),
                null,
                null,
                null,
                metadata,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
            )
        );
        assertTrue(client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet().isAcknowledged());
    }

    static DataLifecycle randomLifecycle() {
        return rarely() ? Template.NO_LIFECYCLE : new DataLifecycle(randomRetention(), randomDownsampling());
    }

    @Nullable
    private static DataLifecycle.Retention randomRetention() {
        return switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> DataLifecycle.Retention.NULL;
            default -> new DataLifecycle.Retention(TimeValue.timeValueMillis(randomMillisUpToYear9999()));
        };
    }

    @Nullable
    private static DataLifecycle.Downsampling randomDownsampling() {
        return switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> DataLifecycle.Downsampling.NULL;
            default -> {
                var count = randomIntBetween(0, 10);
                List<DataLifecycle.Downsampling.Round> rounds = new ArrayList<>();
                var previous = new DataLifecycle.Downsampling.Round(
                    TimeValue.timeValueDays(randomIntBetween(1, 365)),
                    new DownsampleConfig(new DateHistogramInterval(randomIntBetween(1, 24) + "h"), TIMEOUT)
                );
                rounds.add(previous);
                for (int i = 0; i < count; i++) {
                    DataLifecycle.Downsampling.Round round = nextRound(previous);
                    rounds.add(round);
                    previous = round;
                }
                yield new DataLifecycle.Downsampling(rounds);
            }
        };
    }

    private static DataLifecycle.Downsampling.Round nextRound(DataLifecycle.Downsampling.Round previous) {
        var after = TimeValue.timeValueDays(previous.after().days() + randomIntBetween(1, 10));
        var fixedInterval = new DownsampleConfig(
            new DateHistogramInterval((previous.config().getFixedInterval().estimateMillis() * randomIntBetween(2, 5)) + "ms"),
            TIMEOUT
        );
        return new DataLifecycle.Downsampling.Round(after, fixedInterval);
    }
}
