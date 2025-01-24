/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.generateTsdbMapping;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.composeDataLifecycles;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.datastreams.MetadataDataStreamRolloverServiceTests.createSettingsProvider;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Variant of MetadataIndexTemplateServiceTests in server module, but with {@link DataStreamIndexSettingsProvider}.
 */
public class MetadataIndexTemplateServiceTests extends ESSingleNodeTestCase {

    public void testRequireRoutingPath() throws Exception {
        final var service = getMetadataIndexTemplateService();
        {
            // Missing routing path should fail validation
            var componentTemplate = new ComponentTemplate(new Template(null, new CompressedXContent("{}"), null), null, null);
            var state = service.addComponentTemplate(ClusterState.EMPTY_STATE, true, "1", componentTemplate);
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(Collections.singletonList("logs-*-*"))
                .template(new Template(builder().put("index.mode", "time_series").build(), null, null))
                .componentTemplates(List.of("1"))
                .priority(100L)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build();
            var e = expectThrows(InvalidIndexTemplateException.class, () -> service.addIndexTemplateV2(state, false, "1", indexTemplate));
            assertThat(e.getMessage(), containsString("[index.mode=time_series] requires a non-empty [index.routing_path]"));
        }
        {
            // Routing path fetched from mapping of component template
            var state = ClusterState.EMPTY_STATE;
            var componentTemplate = new ComponentTemplate(
                new Template(null, new CompressedXContent(generateTsdbMapping()), null),
                null,
                null
            );
            state = service.addComponentTemplate(state, true, "1", componentTemplate);
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(Collections.singletonList("logs-*-*"))
                .template(new Template(builder().put("index.mode", "time_series").build(), null, null))
                .componentTemplates(List.of("1"))
                .priority(100L)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build();
            state = service.addIndexTemplateV2(state, false, "1", indexTemplate);
            assertThat(state.getMetadata().templatesV2().get("1"), equalTo(indexTemplate));
        }
        {
            // Routing path defined in component template
            var state = ClusterState.EMPTY_STATE;
            var componentTemplate = new ComponentTemplate(
                new Template(builder().put("index.mode", "time_series").put("index.routing_path", "uid").build(), null, null),
                null,
                null
            );
            state = service.addComponentTemplate(state, true, "1", componentTemplate);
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(Collections.singletonList("logs-*-*"))
                .template(new Template(null, null, null))
                .componentTemplates(List.of("1"))
                .priority(100L)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build();
            state = service.addIndexTemplateV2(state, false, "1", indexTemplate);
            assertThat(state.getMetadata().templatesV2().get("1"), equalTo(indexTemplate));
        }
        {
            // Routing path defined in index template
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(Collections.singletonList("logs-*-*"))
                .template(new Template(builder().put("index.mode", "time_series").put("index.routing_path", "uid").build(), null, null))
                .componentTemplates(List.of("1"))
                .priority(100L)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build();
            var state = service.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "1", indexTemplate);
            assertThat(state.getMetadata().templatesV2().get("1"), equalTo(indexTemplate));
        }
        {
            // Routing fetched from mapping in index template
            var indexTemplate = ComposableIndexTemplate.builder()
                .indexPatterns(Collections.singletonList("logs-*-*"))
                .template(
                    new Template(builder().put("index.mode", "time_series").build(), new CompressedXContent(generateTsdbMapping()), null)
                )
                .componentTemplates(List.of("1"))
                .priority(100L)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build();
            var state = service.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "1", indexTemplate);
            assertThat(state.getMetadata().templatesV2().get("1"), equalTo(indexTemplate));
        }
    }

    public void testLifecycleComposition() {
        // No lifecycles result to null
        {
            List<DataStreamLifecycle> lifecycles = List.of();
            assertThat(composeDataLifecycles(lifecycles), nullValue());
        }
        // One lifecycle results to this lifecycle as the final
        {
            DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
                .dataRetention(randomRetention())
                .downsampling(randomDownsampling())
                .build();
            List<DataStreamLifecycle> lifecycles = List.of(lifecycle);
            DataStreamLifecycle result = composeDataLifecycles(lifecycles);
            // Defaults to true
            assertThat(result.isEnabled(), equalTo(true));
            assertThat(result.getDataStreamRetention(), equalTo(lifecycle.getDataStreamRetention()));
            assertThat(result.getDownsamplingRounds(), equalTo(lifecycle.getDownsamplingRounds()));
        }
        // If the last lifecycle is missing a property (apart from enabled) we keep the latest from the previous ones
        // Enabled is always true unless it's explicitly set to false
        {
            DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
                .enabled(false)
                .dataRetention(randomNonEmptyRetention())
                .downsampling(randomNonEmptyDownsampling())
                .build();
            List<DataStreamLifecycle> lifecycles = List.of(lifecycle, new DataStreamLifecycle());
            DataStreamLifecycle result = composeDataLifecycles(lifecycles);
            assertThat(result.isEnabled(), equalTo(true));
            assertThat(result.getDataStreamRetention(), equalTo(lifecycle.getDataStreamRetention()));
            assertThat(result.getDownsamplingRounds(), equalTo(lifecycle.getDownsamplingRounds()));
        }
        // If both lifecycle have all properties, then the latest one overwrites all the others
        {
            DataStreamLifecycle lifecycle1 = DataStreamLifecycle.newBuilder()
                .enabled(false)
                .dataRetention(randomNonEmptyRetention())
                .downsampling(randomNonEmptyDownsampling())
                .build();
            DataStreamLifecycle lifecycle2 = DataStreamLifecycle.newBuilder()
                .enabled(true)
                .dataRetention(randomNonEmptyRetention())
                .downsampling(randomNonEmptyDownsampling())
                .build();
            List<DataStreamLifecycle> lifecycles = List.of(lifecycle1, lifecycle2);
            DataStreamLifecycle result = composeDataLifecycles(lifecycles);
            assertThat(result.isEnabled(), equalTo(lifecycle2.isEnabled()));
            assertThat(result.getDataStreamRetention(), equalTo(lifecycle2.getDataStreamRetention()));
            assertThat(result.getDownsamplingRounds(), equalTo(lifecycle2.getDownsamplingRounds()));
        }
    }

    private MetadataIndexTemplateService getMetadataIndexTemplateService() {
        var indicesService = getInstanceFromNode(IndicesService.class);
        var clusterService = getInstanceFromNode(ClusterService.class);
        var indexSettingProviders = new IndexSettingProviders(Set.of(createSettingsProvider(xContentRegistry())));
        var createIndexService = new MetadataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            indicesService,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000)),
            new Environment(builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(), null),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            null,
            xContentRegistry(),
            EmptySystemIndices.INSTANCE,
            true,
            indexSettingProviders
        );
        return new MetadataIndexTemplateService(
            clusterService,
            createIndexService,
            indicesService,
            new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS),
            xContentRegistry(),
            EmptySystemIndices.INSTANCE,
            indexSettingProviders,
            DataStreamGlobalRetentionSettings.create(ClusterSettings.createBuiltInClusterSettings())
        );
    }

    public static ShardLimitValidator createTestShardLimitService(int maxShardsPerNode) {
        // Use a mocked clusterService - for unit tests we won't be updating the setting anyway.
        ClusterService clusterService = mock(ClusterService.class);
        Settings limitOnlySettings = Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode).build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(limitOnlySettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        return new ShardLimitValidator(limitOnlySettings, clusterService);
    }

    @Nullable
    private static DataStreamLifecycle.Retention randomRetention() {
        return switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> DataStreamLifecycle.Retention.NULL;
            default -> randomNonEmptyRetention();
        };
    }

    private static DataStreamLifecycle.Retention randomNonEmptyRetention() {
        return new DataStreamLifecycle.Retention(TimeValue.timeValueMillis(randomMillisUpToYear9999()));
    }

    @Nullable
    private static DataStreamLifecycle.Downsampling randomDownsampling() {
        return switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> DataStreamLifecycle.Downsampling.NULL;
            default -> randomNonEmptyDownsampling();
        };
    }

    private static DataStreamLifecycle.Downsampling randomNonEmptyDownsampling() {
        var count = randomIntBetween(0, 9);
        List<DataStreamLifecycle.Downsampling.Round> rounds = new ArrayList<>();
        var previous = new DataStreamLifecycle.Downsampling.Round(
            TimeValue.timeValueDays(randomIntBetween(1, 365)),
            new DownsampleConfig(new DateHistogramInterval(randomIntBetween(1, 24) + "h"))
        );
        rounds.add(previous);
        for (int i = 0; i < count; i++) {
            DataStreamLifecycle.Downsampling.Round round = nextRound(previous);
            rounds.add(round);
            previous = round;
        }
        return new DataStreamLifecycle.Downsampling(rounds);
    }

    private static DataStreamLifecycle.Downsampling.Round nextRound(DataStreamLifecycle.Downsampling.Round previous) {
        var after = TimeValue.timeValueDays(previous.after().days() + randomIntBetween(1, 10));
        var fixedInterval = new DownsampleConfig(
            new DateHistogramInterval((previous.config().getFixedInterval().estimateMillis() * randomIntBetween(2, 5)) + "ms")
        );
        return new DataStreamLifecycle.Downsampling.Round(after, fixedInterval);
    }
}
