/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@SuppressForbidden(reason = "Uses IndexMetadata#getForecastedWriteLoad to validate the computation")
public class WriteLoadForecasterIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, FakeLicenseWriteLoadForecasterPlugin.class);
    }

    @Before
    public void ensureValidLicense() {
        setHasValidLicense(true);
    }

    public void testWriteLoadForecastGetsPopulatedDuringRollovers() throws Exception {
        final String dataStreamName = "logs-es";
        setUpDataStreamWriteDocsAndRollover(dataStreamName);

        final ClusterState clusterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        final Metadata metadata = clusterState.getMetadata();
        final DataStream dataStream = metadata.dataStreams().get(dataStreamName);
        final IndexMetadata writeIndexMetadata = metadata.getIndexSafe(dataStream.getWriteIndex());

        final OptionalDouble indexMetadataForecastedWriteLoad = writeIndexMetadata.getForecastedWriteLoad();
        assertThat(indexMetadataForecastedWriteLoad.isPresent(), is(equalTo(true)));
        assertThat(indexMetadataForecastedWriteLoad.getAsDouble(), is(greaterThan(0.0)));

        final WriteLoadForecaster writeLoadForecaster = internalCluster().getCurrentMasterNodeInstance(WriteLoadForecaster.class);
        final OptionalDouble forecastedWriteLoad = writeLoadForecaster.getForecastedWriteLoad(writeIndexMetadata);

        assertThat(forecastedWriteLoad.isPresent(), is(equalTo(true)));
        assertThat(forecastedWriteLoad.getAsDouble(), is(equalTo(indexMetadataForecastedWriteLoad.getAsDouble())));

        assertAllPreviousForecastsAreClearedAfterRollover(dataStream, metadata);

        setHasValidLicense(false);

        final OptionalDouble forecastedWriteLoadAfterLicenseChange = writeLoadForecaster.getForecastedWriteLoad(writeIndexMetadata);
        assertThat(forecastedWriteLoadAfterLicenseChange.isPresent(), is(equalTo(false)));
    }

    public void testWriteLoadForecastDoesNotGetPopulatedWithInvalidLicense() throws Exception {
        setHasValidLicense(false);

        final String dataStreamName = "logs-es";
        setUpDataStreamWriteDocsAndRollover(dataStreamName);

        final ClusterState clusterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        final DataStream dataStream = clusterState.getMetadata().dataStreams().get(dataStreamName);
        final IndexMetadata writeIndexMetadata = clusterState.metadata().getIndexSafe(dataStream.getWriteIndex());

        assertThat(writeIndexMetadata.getForecastedWriteLoad().isPresent(), is(equalTo(false)));
    }

    public void testWriteLoadForecastIsOverriddenBySetting() throws Exception {
        final double writeLoadForecastOverride = randomDoubleBetween(64, 128, true);
        final String dataStreamName = "logs-es";
        setUpDataStreamWriteDocsAndRollover(
            dataStreamName,
            Settings.builder()
                .put(WriteLoadForecasterPlugin.OVERRIDE_WRITE_LOAD_FORECAST_SETTING.getKey(), writeLoadForecastOverride)
                .build()
        );

        final ClusterState clusterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        final Metadata metadata = clusterState.metadata();
        final DataStream dataStream = metadata.dataStreams().get(dataStreamName);
        final IndexMetadata writeIndexMetadata = metadata.getIndexSafe(dataStream.getWriteIndex());

        final OptionalDouble indexMetadataForecastedWriteLoad = writeIndexMetadata.getForecastedWriteLoad();
        assertThat(indexMetadataForecastedWriteLoad.isPresent(), is(equalTo(true)));
        assertThat(indexMetadataForecastedWriteLoad.getAsDouble(), is(greaterThan(0.0)));

        final WriteLoadForecaster writeLoadForecaster = internalCluster().getCurrentMasterNodeInstance(WriteLoadForecaster.class);
        final OptionalDouble forecastedWriteLoad = writeLoadForecaster.getForecastedWriteLoad(writeIndexMetadata);

        assertThat(forecastedWriteLoad.isPresent(), is(equalTo(true)));
        assertThat(forecastedWriteLoad.getAsDouble(), is(equalTo(writeLoadForecastOverride)));
        assertThat(forecastedWriteLoad.getAsDouble(), is(not(equalTo(indexMetadataForecastedWriteLoad.getAsDouble()))));

        assertAllPreviousForecastsAreClearedAfterRollover(dataStream, metadata);

        setHasValidLicense(false);

        final OptionalDouble forecastedWriteLoadAfterLicenseChange = writeLoadForecaster.getForecastedWriteLoad(writeIndexMetadata);
        assertThat(forecastedWriteLoadAfterLicenseChange.isPresent(), is(equalTo(false)));
    }

    private void setUpDataStreamWriteDocsAndRollover(String dataStreamName) throws Exception {
        setUpDataStreamWriteDocsAndRollover(dataStreamName, Settings.EMPTY);
    }

    private void setUpDataStreamWriteDocsAndRollover(String dataStreamName, Settings extraIndexTemplateSettings) throws Exception {
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(0, 1);
        final Settings indexSettings = Settings.builder()
            .put(extraIndexTemplateSettings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .build();

        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("my-template").indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("logs-*"))
                        .template(new Template(indexSettings, null, null))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build()
                )
            )
        );
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
            ).actionGet()
        );

        final int numberOfRollovers = randomIntBetween(5, 10);
        for (int i = 0; i < numberOfRollovers; i++) {

            assertBusy(() -> {
                for (int j = 0; j < 10; j++) {
                    indexDocs(dataStreamName, randomIntBetween(100, 200));
                }

                final ClusterState clusterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
                final DataStream dataStream = clusterState.getMetadata().dataStreams().get(dataStreamName);
                final String writeIndex = dataStream.getWriteIndex().getName();
                final IndicesStatsResponse indicesStatsResponse = indicesAdmin().prepareStats(writeIndex).get();
                for (IndexShardStats indexShardStats : indicesStatsResponse.getIndex(writeIndex).getIndexShards().values()) {
                    for (ShardStats shard : indexShardStats.getShards()) {
                        final IndexingStats.Stats shardIndexingStats = shard.getStats().getIndexing().getTotal();
                        // Ensure that we have enough clock granularity before rolling over to ensure that we capture _some_ write load
                        assertThat(shardIndexingStats.getTotalActiveTimeInMillis(), is(greaterThan(0L)));
                        assertThat(shardIndexingStats.getWriteLoad(), is(greaterThan(0.0)));
                    }
                }
            });

            assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());
        }
    }

    static void indexDocs(String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(Strings.format("{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        client().bulk(bulkRequest).actionGet();
    }

    private void setHasValidLicense(boolean hasValidLicense) {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(FakeLicenseWriteLoadForecasterPlugin.class).forEach(p -> p.setHasValidLicense(hasValidLicense));
        }
    }

    private void assertAllPreviousForecastsAreClearedAfterRollover(DataStream dataStream, Metadata metadata) {
        final WriteLoadForecaster writeLoadForecaster = internalCluster().getCurrentMasterNodeInstance(WriteLoadForecaster.class);

        for (Index index : dataStream.getIndices()) {
            if (index.equals(dataStream.getWriteIndex())) {
                continue;
            }
            final IndexMetadata backingIndexMetadata = metadata.getIndexSafe(index);
            final OptionalDouble backingIndexForecastedWriteLoad = writeLoadForecaster.getForecastedWriteLoad(backingIndexMetadata);
            assertThat(backingIndexForecastedWriteLoad.isEmpty(), is(equalTo(true)));
            assertThat(backingIndexMetadata.getForecastedWriteLoad().isEmpty(), is(equalTo(true)));
            assertThat(
                backingIndexMetadata.getSettings().hasValue(WriteLoadForecasterPlugin.OVERRIDE_WRITE_LOAD_FORECAST_SETTING.getKey()),
                is(equalTo(false))
            );
        }
    }

    public static class FakeLicenseWriteLoadForecasterPlugin extends WriteLoadForecasterPlugin {
        private final AtomicBoolean hasValidLicense = new AtomicBoolean(true);

        public FakeLicenseWriteLoadForecasterPlugin() {}

        void setHasValidLicense(boolean validLicense) {
            hasValidLicense.set(validLicense);
        }

        @Override
        protected boolean hasValidLicense() {
            return hasValidLicense.get();
        }
    }
}
