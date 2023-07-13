/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataLifecycle;
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
import java.util.concurrent.TimeUnit;

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

    public static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    public void testRequireRoutingPath() throws Exception {
        final var service = getMetadataIndexTemplateService();
        {
            // Missing routing path should fail validation
            var componentTemplate = new ComponentTemplate(new Template(null, new CompressedXContent("{}"), null), null, null);
            var state = service.addComponentTemplate(ClusterState.EMPTY_STATE, true, "1", componentTemplate);
            var indexTemplate = new ComposableIndexTemplate(
                Collections.singletonList("logs-*-*"),
                new Template(builder().put("index.mode", "time_series").build(), null, null),
                List.of("1"),
                100L,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(false, false),
                null
            );
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
            var indexTemplate = new ComposableIndexTemplate(
                Collections.singletonList("logs-*-*"),
                new Template(builder().put("index.mode", "time_series").build(), null, null),
                List.of("1"),
                100L,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(false, false),
                null
            );
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
            var indexTemplate = new ComposableIndexTemplate(
                Collections.singletonList("logs-*-*"),
                new Template(null, null, null),
                List.of("1"),
                100L,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(false, false),
                null
            );
            state = service.addIndexTemplateV2(state, false, "1", indexTemplate);
            assertThat(state.getMetadata().templatesV2().get("1"), equalTo(indexTemplate));
        }
        {
            // Routing path defined in index template
            var indexTemplate = new ComposableIndexTemplate(
                Collections.singletonList("logs-*-*"),
                new Template(builder().put("index.mode", "time_series").put("index.routing_path", "uid").build(), null, null),
                List.of("1"),
                100L,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(false, false),
                null
            );
            var state = service.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "1", indexTemplate);
            assertThat(state.getMetadata().templatesV2().get("1"), equalTo(indexTemplate));
        }
        {
            // Routing fetched from mapping in index template
            var indexTemplate = new ComposableIndexTemplate(
                Collections.singletonList("logs-*-*"),
                new Template(builder().put("index.mode", "time_series").build(), new CompressedXContent(generateTsdbMapping()), null),
                List.of("1"),
                100L,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(false, false),
                null
            );
            var state = service.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "1", indexTemplate);
            assertThat(state.getMetadata().templatesV2().get("1"), equalTo(indexTemplate));
        }
    }

    public void testLifecycleComposition() {
        // No lifecycles result to null
        {
            List<DataLifecycle> lifecycles = List.of();
            assertThat(composeDataLifecycles(lifecycles), nullValue());
        }
        // One lifecycle results to this lifecycle as the final
        {
            DataLifecycle lifecycle = new DataLifecycle(randomRetention(), randomDownsampling());
            List<DataLifecycle> lifecycles = List.of(lifecycle);
            DataLifecycle result = composeDataLifecycles(lifecycles);
            assertThat(result.getEffectiveDataRetention(), equalTo(lifecycle.getEffectiveDataRetention()));
            assertThat(result.getDownsamplingRounds(), equalTo(lifecycle.getDownsamplingRounds()));
        }
        // If the last lifecycle is missing a property we keep the latest from the previous ones
        {
            DataLifecycle lifecycle = new DataLifecycle(randomNonEmptyRetention(), randomNonEmptyDownsampling());
            List<DataLifecycle> lifecycles = List.of(lifecycle, new DataLifecycle());
            DataLifecycle result = composeDataLifecycles(lifecycles);
            assertThat(result.getEffectiveDataRetention(), equalTo(lifecycle.getEffectiveDataRetention()));
            assertThat(result.getDownsamplingRounds(), equalTo(lifecycle.getDownsamplingRounds()));
        }
        // If both lifecycle have all properties, then the latest one overwrites all the others
        {
            DataLifecycle lifecycle1 = new DataLifecycle(randomNonEmptyRetention(), randomNonEmptyDownsampling());
            DataLifecycle lifecycle2 = new DataLifecycle(randomNonEmptyRetention(), randomNonEmptyDownsampling());
            List<DataLifecycle> lifecycles = List.of(lifecycle1, lifecycle2);
            DataLifecycle result = composeDataLifecycles(lifecycles);
            assertThat(result.getEffectiveDataRetention(), equalTo(lifecycle2.getEffectiveDataRetention()));
            assertThat(result.getDownsamplingRounds(), equalTo(lifecycle2.getDownsamplingRounds()));
        }
        // If the last lifecycle is explicitly null, the result is also null
        {
            DataLifecycle lifecycle1 = new DataLifecycle(randomNonEmptyRetention(), randomNonEmptyDownsampling());
            DataLifecycle lifecycle2 = new DataLifecycle(randomNonEmptyRetention(), randomNonEmptyDownsampling());
            List<DataLifecycle> lifecycles = List.of(lifecycle1, lifecycle2, Template.NO_LIFECYCLE);
            assertThat(composeDataLifecycles(lifecycles), nullValue());
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
            indexSettingProviders
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
    private static DataLifecycle.Retention randomRetention() {
        return switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> DataLifecycle.Retention.NULL;
            default -> randomNonEmptyRetention();
        };
    }

    private static DataLifecycle.Retention randomNonEmptyRetention() {
        return new DataLifecycle.Retention(TimeValue.timeValueMillis(randomMillisUpToYear9999()));
    }

    @Nullable
    private static DataLifecycle.Downsampling randomDownsampling() {
        return switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> DataLifecycle.Downsampling.NULL;
            default -> randomNonEmptyDownsampling();
        };
    }

    private static DataLifecycle.Downsampling randomNonEmptyDownsampling() {
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
        return new DataLifecycle.Downsampling(rounds);
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
