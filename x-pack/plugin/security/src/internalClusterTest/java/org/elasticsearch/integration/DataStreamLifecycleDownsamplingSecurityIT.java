/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.datastreams.lifecycle.action.PutDataStreamLifecycleAction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.downsample.Downsample;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * This test suite ensures that data stream lifecycle runtime tasks work correctly with security enabled, i.e., that the internal user for
 * data stream lifecycle has all requisite privileges to orchestrate the data stream lifecycle
 * This class focuses on the donwsampling execution.
 */
public class DataStreamLifecycleDownsamplingSecurityIT extends SecurityIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleDownsamplingSecurityIT.class);

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_DIMENSION_1 = "dimension_kw";
    public static final String FIELD_DIMENSION_2 = "dimension_long";
    public static final String FIELD_METRIC_COUNTER = "counter";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateSecurity.class,
            DataStreamsPlugin.class,
            SystemDataStreamTestPlugin.class,
            MapperExtrasPlugin.class,
            Wildcard.class,
            Downsample.class,
            AggregateMetricMapperPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        return settings.build();
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testDownsamplingAuthorized() throws Exception {
        String dataStreamName = "metrics-foo";

        DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
            .downsampling(
                new DataStreamLifecycle.Downsampling(
                    List.of(
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueMillis(0),
                            new DownsampleConfig(new DateHistogramInterval("1s"))
                        ),
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueSeconds(10),
                            new DownsampleConfig(new DateHistogramInterval("10s"))
                        )
                    )
                )
            )
            .build();

        setupDataStreamAndIngestDocs(client(), dataStreamName, lifecycle, 10_000);
        waitAndAssertDownsamplingCompleted(dataStreamName);
    }

    public void testConfiguringLifecycleWithDownsamplingForSystemDataStreamFails() {
        String dataStreamName = SystemDataStreamTestPlugin.SYSTEM_DATA_STREAM_NAME;
        indexDocuments(client(), dataStreamName, 100);
        DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
            .downsampling(
                new DataStreamLifecycle.Downsampling(
                    List.of(
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueMillis(0),
                            new DownsampleConfig(new DateHistogramInterval("1s"))
                        ),
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueSeconds(10),
                            new DownsampleConfig(new DateHistogramInterval("10s"))
                        )
                    )
                )
            )
            .build();
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(
                PutDataStreamLifecycleAction.INSTANCE,
                new PutDataStreamLifecycleAction.Request(new String[] { dataStreamName }, lifecycle)
            ).actionGet()
        );
        assertThat(
            illegalArgumentException.getMessage(),
            is(
                "System data streams do not support downsampling as part of their lifecycle "
                    + "configuration. Encountered ["
                    + dataStreamName
                    + "] in the request"
            )
        );
    }

    public void testExplicitSystemDataStreamConfigurationWithDownsamplingFails() {
        SystemDataStreamWithDownsamplingConfigurationPlugin pluginWithIllegalSystemDataStream =
            new SystemDataStreamWithDownsamplingConfigurationPlugin();
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> pluginWithIllegalSystemDataStream.getSystemDataStreamDescriptors()
        );
        assertThat(
            illegalArgumentException.getMessage(),
            is("System data streams do not support downsampling as part of their lifecycle configuration")
        );
    }

    private void waitAndAssertDownsamplingCompleted(String dataStreamName) throws Exception {
        List<Index> backingIndices = getDataStreamBackingIndices(dataStreamName);
        String firstGenerationBackingIndex = backingIndices.get(0).getName();
        String oneSecondDownsampleIndex = "downsample-1s-" + firstGenerationBackingIndex;
        String tenSecondsDownsampleIndex = "downsample-10s-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (event.indicesCreated().contains(oneSecondDownsampleIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(oneSecondDownsampleIndex))) {
                witnessedDownsamplingIndices.add(oneSecondDownsampleIndex);
            }
            if (event.indicesCreated().contains(tenSecondsDownsampleIndex)) {
                witnessedDownsamplingIndices.add(tenSecondsDownsampleIndex);
            }
        });

        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        assertBusy(() -> {
            assertNoAuthzErrors();
            // first downsampling round
            assertThat(witnessedDownsamplingIndices.contains(oneSecondDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            assertNoAuthzErrors();
            assertThat(witnessedDownsamplingIndices.size(), is(2));
            assertThat(witnessedDownsamplingIndices.contains(oneSecondDownsampleIndex), is(true));

            assertThat(witnessedDownsamplingIndices.contains(tenSecondsDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            assertNoAuthzErrors();
            List<Index> dsBackingIndices = getDataStreamBackingIndices(dataStreamName);

            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            // the last downsampling round must remain in the data stream
            assertThat(dsBackingIndices.get(0).getName(), is(tenSecondsDownsampleIndex));
            assertThat(indexExists(firstGenerationBackingIndex), is(false));
            assertThat(indexExists(oneSecondDownsampleIndex), is(false));
        }, 30, TimeUnit.SECONDS);
    }

    private Map<String, String> collectErrorsFromStoreAsMap() {
        Iterable<DataStreamLifecycleService> lifecycleServices = internalCluster().getInstances(DataStreamLifecycleService.class);
        Map<String, String> indicesAndErrors = new HashMap<>();
        for (DataStreamLifecycleService lifecycleService : lifecycleServices) {
            DataStreamLifecycleErrorStore errorStore = lifecycleService.getErrorStore();
            List<String> allIndices = errorStore.getAllIndices();
            for (var index : allIndices) {
                indicesAndErrors.put(index, errorStore.getError(index));
            }
        }
        return indicesAndErrors;
    }

    private List<Index> getDataStreamBackingIndices(String dataStreamName) {
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
        return getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
    }

    private void assertNoAuthzErrors() {
        var indicesAndErrors = collectErrorsFromStoreAsMap();
        for (var entry : indicesAndErrors.entrySet()) {
            assertThat(
                "unexpected authz error for index [" + entry.getKey() + "] with error message [" + entry.getValue() + "]",
                entry.getValue(),
                not(anyOf(containsString("security_exception"), containsString("unauthorized for user [_data_stream_lifecycle]")))
            );
        }
    }

    private void setupDataStreamAndIngestDocs(Client client, String dataStreamName, DataStreamLifecycle lifecycle, int docCount)
        throws IOException {
        putTSDBIndexTemplate(client, dataStreamName + "*", lifecycle);
        indexDocuments(client, dataStreamName, docCount);
    }

    private void putTSDBIndexTemplate(Client client, String pattern, DataStreamLifecycle lifecycle) throws IOException {
        Settings.Builder settings = indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1));
        CompressedXContent mapping = getTSDBMappings();
        putComposableIndexTemplate(client, "id1", mapping, List.of(pattern), settings.build(), null, lifecycle);
    }

    private static CompressedXContent getTSDBMappings() throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        mapping.startObject(FIELD_TIMESTAMP).field("type", "date").endObject();

        mapping.startObject(FIELD_DIMENSION_1).field("type", "keyword").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_DIMENSION_2).field("type", "long").field("time_series_dimension", true).endObject();

        mapping.startObject(FIELD_METRIC_COUNTER)
            .field("type", "double") /* numeric label indexed as a metric */
            .field("time_series_metric", "counter")
            .endObject();

        mapping.endObject().endObject().endObject();
        return CompressedXContent.fromJSON(Strings.toString(mapping));
    }

    private void putComposableIndexTemplate(
        Client client,
        String id,
        @Nullable CompressedXContent mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            new ComposableIndexTemplate(
                patterns,
                new Template(settings, mappings, null, lifecycle),
                null,
                null,
                null,
                metadata,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
            )
        );
        client.execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
    }

    private void indexDocuments(Client client, String dataStreamName, int docCount) {
        final Supplier<XContentBuilder> sourceSupplier = () -> {
            final String ts = randomDateForInterval(new DateHistogramInterval("1s"), System.currentTimeMillis());
            double counterValue = DATE_FORMATTER.parseMillis(ts);
            final List<String> dimensionValues = new ArrayList<>(5);
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                dimensionValues.add(randomAlphaOfLength(6));
            }
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field(FIELD_TIMESTAMP, ts)
                    .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                    .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                    .field(FIELD_METRIC_COUNTER, counterValue)
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(client, dataStreamName, sourceSupplier, docCount);
    }

    private String randomDateForInterval(final DateHistogramInterval interval, final long startTime) {
        long endTime = startTime + 10 * interval.estimateMillis();
        return randomDateForRange(startTime, endTime);
    }

    private String randomDateForRange(long start, long end) {
        return DATE_FORMATTER.formatMillis(randomLongBetween(start, end));
    }

    private void bulkIndex(Client client, String dataStreamName, Supplier<XContentBuilder> docSourceSupplier, int docCount) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
            XContentBuilder source = docSourceSupplier.get();
            indexRequest.source(source);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        int duplicates = 0;
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                if (response.getFailure().getCause() instanceof VersionConflictEngineException) {
                    // A duplicate event was created by random generator. We should not fail for this
                    // reason.
                    logger.debug("-> failed to insert a duplicate: [{}]", response.getFailureMessage());
                    duplicates++;
                } else {
                    throw new ElasticsearchException("Failed to index data: " + bulkResponse.buildFailureMessage());
                }
            }
        }
        int docsIndexed = docCount - duplicates;
        logger.info("-> Indexed [{}] documents. Dropped [{}] duplicates.", docsIndexed, duplicates);
    }

    public static class SystemDataStreamTestPlugin extends Plugin implements SystemIndexPlugin {

        static final String SYSTEM_DATA_STREAM_NAME = ".fleet-actions-results";

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            Settings.Builder settings = indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1));

            try {
                return List.of(
                    new SystemDataStreamDescriptor(
                        SYSTEM_DATA_STREAM_NAME,
                        "a system data stream for testing",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        new ComposableIndexTemplate(
                            List.of(SYSTEM_DATA_STREAM_NAME),
                            new Template(settings.build(), getTSDBMappings(), null, null),
                            null,
                            null,
                            null,
                            null,
                            new ComposableIndexTemplate.DataStreamTemplate()
                        ),
                        Map.of(),
                        Collections.singletonList("test"),
                        new ExecutorNames(
                            ThreadPool.Names.SYSTEM_CRITICAL_READ,
                            ThreadPool.Names.SYSTEM_READ,
                            ThreadPool.Names.SYSTEM_WRITE
                        )
                    )
                );
            } catch (IOException e) {
                throw new RuntimeException("Unable to create system data stream descriptor", e);
            }
        }

        @Override
        public String getFeatureName() {
            return SystemDataStreamTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A plugin for testing the data stream lifecycle runtime actions on system data streams";
        }
    }

    public static class SystemDataStreamWithDownsamplingConfigurationPlugin extends Plugin implements SystemIndexPlugin {

        static final String SYSTEM_DATA_STREAM_NAME = ".fleet-actions-results";

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
                .downsampling(
                    new DataStreamLifecycle.Downsampling(
                        List.of(
                            new DataStreamLifecycle.Downsampling.Round(
                                TimeValue.timeValueMillis(0),
                                new DownsampleConfig(new DateHistogramInterval("1s"))
                            ),
                            new DataStreamLifecycle.Downsampling.Round(
                                TimeValue.timeValueSeconds(10),
                                new DownsampleConfig(new DateHistogramInterval("10s"))
                            )
                        )
                    )
                )
                .build();

            Settings.Builder settings = indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1));

            try {
                return List.of(
                    new SystemDataStreamDescriptor(
                        SYSTEM_DATA_STREAM_NAME,
                        "a system data stream for testing",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        new ComposableIndexTemplate(
                            List.of(SYSTEM_DATA_STREAM_NAME),
                            new Template(settings.build(), getTSDBMappings(), null, lifecycle),
                            null,
                            null,
                            null,
                            null,
                            new ComposableIndexTemplate.DataStreamTemplate()
                        ),
                        Map.of(),
                        Collections.singletonList("test"),
                        new ExecutorNames(
                            ThreadPool.Names.SYSTEM_CRITICAL_READ,
                            ThreadPool.Names.SYSTEM_READ,
                            ThreadPool.Names.SYSTEM_WRITE
                        )
                    )
                );
            } catch (IOException e) {
                throw new RuntimeException("Unable to create system data stream descriptor", e);
            }
        }

        @Override
        public String getFeatureName() {
            return SystemDataStreamTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A plugin for testing the data stream lifecycle runtime actions on system data streams";
        }
    }
}
