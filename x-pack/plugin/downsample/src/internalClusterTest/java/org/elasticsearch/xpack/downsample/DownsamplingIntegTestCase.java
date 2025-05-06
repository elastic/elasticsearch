/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Base test case for downsampling integration tests. It provides helper methods to:
 * - set up templates and data streams
 * - index documents
 * - to assert the correctness of mapping, settings etc.
 */
public abstract class DownsamplingIntegTestCase extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DownsamplingIntegTestCase.class);
    static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    static final String FIELD_TIMESTAMP = "@timestamp";
    static final String FIELD_DIMENSION_KEYWORD = "dimension_kw";
    static final String FIELD_DIMENSION_LONG = "dimension_long";
    static final String FIELD_METRIC_COUNTER_DOUBLE = "counter";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, LocalStateCompositeXPackPlugin.class, Downsample.class, AggregateMetricMapperPlugin.class);
    }

    /**
     * Sets up a TSDB data stream and ingests the specified number of documents
     * @return the count of indexed documents
     */
    public int setupTSDBDataStreamAndIngestDocs(
        String dataStreamName,
        @Nullable String startTime,
        @Nullable String endTime,
        DataStreamLifecycle.Template lifecycle,
        int docCount,
        String firstDocTimestamp
    ) throws IOException {
        putTSDBIndexTemplate(dataStreamName + "*", startTime, endTime, lifecycle);
        return indexDocuments(dataStreamName, docCount, firstDocTimestamp);
    }

    /**
     * Creates an index template that will create TSDB composable templates
     */
    public void putTSDBIndexTemplate(
        String pattern,
        @Nullable String startTime,
        @Nullable String endTime,
        DataStreamLifecycle.Template lifecycle
    ) throws IOException {
        Settings.Builder settings = indexSettings(1, 0).putList(
            IndexMetadata.INDEX_ROUTING_PATH.getKey(),
            List.of(FIELD_DIMENSION_KEYWORD)
        );

        if (Strings.hasText(startTime)) {
            settings.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime);
        }

        if (Strings.hasText(endTime)) {
            settings.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime);
        }

        String mappingString = String.format(Locale.ROOT, """
            {
              "properties": {
                "@timestamp": {
                  "type": "date"
                },
                "%s": {
                  "type": "keyword",
                  "time_series_dimension": true
                },
                "%s": {
                  "type": "long",
                  "time_series_dimension": true
                },
                "%s": {
                  "type": "double",
                  "time_series_metric": "counter"
                }
              }
            }""", FIELD_DIMENSION_KEYWORD, FIELD_DIMENSION_LONG, FIELD_METRIC_COUNTER_DOUBLE);

        putTSDBIndexTemplate("id1", List.of(pattern), settings.build(), mappingString, lifecycle, null);
    }

    void putTSDBIndexTemplate(
        String id,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable String mappingString,
        @Nullable DataStreamLifecycle.Template lifecycle,
        @Nullable Map<String, Object> metadata
    ) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        if (settings != null) {
            settingsBuilder.put(settings);
        }
        // Ensure it will be a TSDB data stream
        settingsBuilder.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);
        CompressedXContent mappings = mappingString == null ? null : CompressedXContent.fromJSON(mappingString);
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(Template.builder().settings(settingsBuilder).mappings(mappings).lifecycle(lifecycle))
                .metadata(metadata)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
    }

    void updateDataLifecycle(String dataStreamName, DataStreamLifecycle lifecycle) throws IOException {
        assertAcked(
            client().execute(
                PutDataStreamLifecycleAction.INSTANCE,
                new PutDataStreamLifecycleAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    new String[] { dataStreamName },
                    lifecycle
                )
            )
        );
    }

    /**
     * Creates and indexes the specified number of documents using the docSource supplier.
     * @return the count of indexed documents
     */
    int bulkIndex(String dataStreamName, Supplier<XContentBuilder> docSourceSupplier, int docCount) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
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
        return docsIndexed;
    }

    int indexDocuments(String dataStreamName, int docCount, String firstDocTimestamp) {
        final Supplier<XContentBuilder> sourceSupplier = () -> {
            long startTime = LocalDateTime.parse(firstDocTimestamp).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            final String ts = randomDateForInterval(new DateHistogramInterval("1s"), startTime);
            double counterValue = DATE_FORMATTER.parseMillis(ts);
            final List<String> dimensionValues = new ArrayList<>(5);
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                dimensionValues.add(randomAlphaOfLength(6));
            }
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field(FIELD_TIMESTAMP, ts)
                    .field(FIELD_DIMENSION_KEYWORD, randomFrom(dimensionValues))
                    .field(FIELD_DIMENSION_LONG, randomIntBetween(1, 10))
                    .field(FIELD_METRIC_COUNTER_DOUBLE, counterValue)
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        return bulkIndex(dataStreamName, sourceSupplier, docCount);
    }

    String randomDateForInterval(final DateHistogramInterval interval, final long startTime) {
        long endTime = startTime + 10 * interval.estimateMillis();
        return randomDateForRange(startTime, endTime);
    }

    String randomDateForRange(long start, long end) {
        return DATE_FORMATTER.formatMillis(randomLongBetween(start, end));
    }
}
