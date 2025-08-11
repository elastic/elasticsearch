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
import org.elasticsearch.cluster.metadata.ResettableValue;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
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
            List<DataStreamLifecycle.Template> lifecycles = List.of();
            assertThat(composeDataLifecycles(lifecycles), nullValue());
        }
        // One lifecycle results to this lifecycle as the final
        {
            DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.createDataLifecycleTemplate(
                true,
                randomRetention(),
                randomDownsampling()
            );
            List<DataStreamLifecycle.Template> lifecycles = List.of(lifecycle);
            DataStreamLifecycle result = composeDataLifecycles(lifecycles).build();
            // Defaults to true
            assertThat(result.enabled(), equalTo(true));
            assertThat(result.dataRetention(), equalTo(lifecycle.dataRetention().get()));
            assertThat(result.downsampling(), equalTo(lifecycle.downsampling().get()));
        }
        // If the last lifecycle is missing a property (apart from enabled) we keep the latest from the previous ones
        // Enabled is always true unless it's explicitly set to false
        {
            DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.createDataLifecycleTemplate(
                false,
                randomPositiveTimeValue(),
                randomRounds()
            );
            List<DataStreamLifecycle.Template> lifecycles = List.of(lifecycle, DataStreamLifecycle.Template.DATA_DEFAULT);
            DataStreamLifecycle result = composeDataLifecycles(lifecycles).build();
            assertThat(result.enabled(), equalTo(true));
            assertThat(result.dataRetention(), equalTo(lifecycle.dataRetention().get()));
            assertThat(result.downsampling(), equalTo(lifecycle.downsampling().get()));
        }
        // If both lifecycle have all properties, then the latest one overwrites all the others
        {
            DataStreamLifecycle.Template lifecycle1 = DataStreamLifecycle.createDataLifecycleTemplate(
                false,
                randomPositiveTimeValue(),
                randomRounds()
            );
            DataStreamLifecycle.Template lifecycle2 = DataStreamLifecycle.createDataLifecycleTemplate(
                true,
                randomPositiveTimeValue(),
                randomRounds()
            );
            List<DataStreamLifecycle.Template> lifecycles = List.of(lifecycle1, lifecycle2);
            DataStreamLifecycle result = composeDataLifecycles(lifecycles).build();
            assertThat(result.enabled(), equalTo(lifecycle2.enabled()));
            assertThat(result.dataRetention(), equalTo(lifecycle2.dataRetention().get()));
            assertThat(result.downsampling(), equalTo(lifecycle2.downsampling().get()));
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

    private static List<DataStreamLifecycle.DownsamplingRound> randomRounds() {
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

    private static ResettableValue<TimeValue> randomRetention() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> ResettableValue.undefined();
            case 1 -> ResettableValue.reset();
            case 2 -> ResettableValue.create(TimeValue.timeValueDays(randomIntBetween(1, 100)));
            default -> throw new IllegalStateException("Unknown randomisation path");
        };
    }

    private static ResettableValue<List<DataStreamLifecycle.DownsamplingRound>> randomDownsampling() {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> ResettableValue.reset();
            case 1 -> ResettableValue.create(randomRounds());
            default -> throw new IllegalStateException("Unknown randomisation path");
        };
    }
}
