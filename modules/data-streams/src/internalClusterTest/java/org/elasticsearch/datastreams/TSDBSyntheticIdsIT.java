/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageRequest;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageTestUtils;
import org.elasticsearch.action.admin.indices.diskusage.IndexDiskUsageStats;
import org.elasticsearch.action.admin.indices.diskusage.TransportAnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.time.FormatNames.STRICT_DATE_OPTIONAL_TIME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertCheckedResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test suite for time series indices that use synthetic ids for documents.
 * <p>
 * Synthetic _id fields are not indexed in Lucene, instead they are generated on demand by concatenating the values of two other fields of
 * the document (typically the {@code @timestamp} and {@code _tsid} fields).
 * </p>
 */
@LuceneTestCase.SuppressCodecs("*") // requires codecs used in production only
public class TSDBSyntheticIdsIT extends ESIntegTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern(STRICT_DATE_OPTIONAL_TIME.getName());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    public void testInvalidIndexMode() {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final var indexName = randomIdentifier();
        var randomNonTsdbIndexMode = randomValueOtherThan(IndexMode.TIME_SERIES, () -> randomFrom(IndexMode.values()));

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                indexName,
                indexSettings(1, 0).put(IndexSettings.MODE.getKey(), randomNonTsdbIndexMode)
                    .put(IndexSettings.USE_SYNTHETIC_ID.getKey(), true)
                    .build()
            )
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "The setting ["
                    + IndexSettings.USE_SYNTHETIC_ID.getKey()
                    + "] is only permitted when [index.mode] is set to [TIME_SERIES]. Current mode: ["
                    + randomNonTsdbIndexMode.getName().toUpperCase(Locale.ROOT)
                    + "]."
            )
        );
    }

    public void testSyntheticId() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(dataStreamName, randomIntBetween(1, 5));

        final var docs = new HashMap<String, String>();
        final var unit = randomFrom(ChronoUnit.SECONDS, ChronoUnit.MINUTES);
        final var timestamp = Instant.now();
        logger.info("timestamp is " + timestamp);

        // Index 10 docs in datastream
        //
        // For convenience, the metric value maps the index in the bulk response items
        var results = createDocuments(
            dataStreamName,
            // t + 0s
            document(timestamp, "vm-dev01", "cpu-load", 0),
            document(timestamp, "vm-dev02", "cpu-load", 1),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4),
            // t + 2s
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", 5),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", 6),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", 7),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", 8),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", 9)
        );

        // Verify that documents are created
        for (var result : results) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(result.getVersion(), equalTo(1L));
            docs.put(result.getId(), result.getIndex());
        }

        enum Operation {
            FLUSH,
            REFRESH,
            NONE
        }

        // Random flush or refresh or nothing, so that the next GETs are executed on flushed segments or in memory segments.
        switch (randomFrom(Operation.values())) {
            case FLUSH:
                flush(dataStreamName);
                break;
            case REFRESH:
                refresh(dataStreamName);
                break;
            case NONE:
            default:
                break;
        }

        // Get by synthetic _id
        var randomDocs = randomSubsetOf(randomIntBetween(0, results.length), results);
        for (var doc : randomDocs) {
            boolean fetchSource = randomBoolean();
            var getResponse = client().prepareGet(doc.getIndex(), doc.getId()).setFetchSource(fetchSource).get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getVersion(), equalTo(1L));

            if (fetchSource) {
                var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
                assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(doc.getItemId()));
            }
        }

        // Random flush or refresh or nothing, so that the next DELETEs are executed on flushed segments or in memory segments.
        switch (randomFrom(Operation.values())) {
            case FLUSH:
                flush(dataStreamName);
                break;
            case REFRESH:
                refresh(dataStreamName);
                break;
            case NONE:
            default:
                break;
        }

        // Delete by synthetic _id
        var deletedDocs = randomSubsetOf(randomIntBetween(1, docs.size()), docs.keySet());
        for (var docId : deletedDocs) {
            var deletedDocIndex = docs.get(docId);
            assertThat(deletedDocIndex, notNullValue());

            // Delete
            var deleteResponse = client().prepareDelete(deletedDocIndex, docId).get();
            assertThat(deleteResponse.getId(), equalTo(docId));
            assertThat(deleteResponse.getIndex(), equalTo(deletedDocIndex));
            assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
            assertThat(deleteResponse.getVersion(), equalTo(2L));
        }

        // Index more random docs
        if (randomBoolean()) {
            int nbDocs = randomIntBetween(1, 100);
            final var arrayOfDocs = new XContentBuilder[nbDocs];

            var t = timestamp.plus(4, unit); // t + 4s, no overlap with previous docs
            while (nbDocs > 0) {
                var hosts = randomSubsetOf(List.of("vm-dev01", "vm-dev02", "vm-dev03"));
                for (var host : hosts) {
                    if (--nbDocs < 0) {
                        break;
                    }
                    arrayOfDocs[nbDocs] = document(t, host, "cpu-load", randomInt(10));
                }
                // always use seconds, otherwise the doc might fell outside of the timestamps window of the datastream
                t = t.plus(1, ChronoUnit.SECONDS);
            }

            results = createDocuments(dataStreamName, arrayOfDocs);

            // Verify that documents are created
            for (var result : results) {
                assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
                assertThat(result.getVersion(), equalTo(1L));
                docs.put(result.getId(), result.getIndex());
            }
        }

        refresh(dataStreamName);

        assertCheckedResponse(client().prepareSearch(dataStreamName).setTrackTotalHits(true).setSize(100), searchResponse -> {
            assertHitCount(searchResponse, docs.size() - deletedDocs.size());

            // Verify that search response does not contain deleted docs
            for (var searchHit : searchResponse.getHits()) {
                assertThat(
                    "Document with id [" + searchHit.getId() + "] is deleted",
                    deletedDocs.contains(searchHit.getId()),
                    equalTo(false)
                );
            }
        });

        // Search by synthetic _id
        var otherDocs = randomSubsetOf(Sets.difference(docs.keySet(), Sets.newHashSet(deletedDocs)));
        for (var docId : otherDocs) {
            assertCheckedResponse(
                client().prepareSearch(docs.get(docId))
                    .setSource(new SearchSourceBuilder().query(new TermQueryBuilder(IdFieldMapper.NAME, docId))),
                searchResponse -> {
                    assertHitCount(searchResponse, 1L);
                    assertThat(searchResponse.getHits().getHits(), arrayWithSize(1));
                    assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo(docId));
                }
            );
        }

        flush(dataStreamName);

        // Check that synthetic _id field have no postings on disk
        var indices = new HashSet<>(docs.values());
        for (var index : indices) {
            var diskUsage = diskUsage(index);
            var diskUsageIdField = AnalyzeIndexDiskUsageTestUtils.getPerFieldDiskUsage(diskUsage, IdFieldMapper.NAME);
            assertThat("_id field should not have postings on disk", diskUsageIdField.getInvertedIndexBytes(), equalTo(0L));
        }
    }

    public void testGetFromTranslogBySyntheticId() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final var dataStreamName = randomIdentifier();
        putDataStreamTemplate(dataStreamName, 1);

        final var docs = new HashMap<String, String>();
        final var unit = randomFrom(ChronoUnit.SECONDS, ChronoUnit.MINUTES);
        final var timestamp = Instant.now();

        // Index 5 docs in datastream
        //
        // For convenience, the metric value maps the index in the bulk response items
        var results = createDocuments(
            dataStreamName,
            // t + 0s
            document(timestamp, "vm-dev01", "cpu-load", 0),
            document(timestamp, "vm-dev02", "cpu-load", 1),
            // t + 1s
            document(timestamp.plus(1, unit), "vm-dev01", "cpu-load", 2),
            document(timestamp.plus(1, unit), "vm-dev02", "cpu-load", 3),
            // t + 0s out-of-order doc
            document(timestamp, "vm-dev03", "cpu-load", 4)
        );

        // Verify that documents are created
        for (var result : results) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(result.getVersion(), equalTo(1L));
            docs.put(result.getId(), result.getIndex());
        }

        // Get by synthetic _id
        //
        // The documents are in memory buffers: the first GET will trigger the refresh of the internal reader
        // (see InternalEngine.REAL_TIME_GET_REFRESH_SOURCE) to have an up-to-date searcher to resolve documents ids and versions. It will
        // also enable the tracking of the locations of documents in the translog (see InternalEngine.trackTranslogLocation) so that next
        // GETs will be resolved using the translog.
        var randomDocs = randomSubsetOf(randomIntBetween(1, results.length), results);
        for (var doc : randomDocs) {
            var getResponse = client().prepareGet(doc.getIndex(), doc.getId()).setRealtime(true).setFetchSource(true).execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getVersion(), equalTo(1L));

            var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
            assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(doc.getItemId()));
        }

        int metricOffset = results.length;

        // Index 5 more docs
        results = createDocuments(
            dataStreamName,
            // t + 2s
            document(timestamp.plus(2, unit), "vm-dev01", "cpu-load", metricOffset),
            document(timestamp.plus(2, unit), "vm-dev02", "cpu-load", metricOffset + 1),
            // t - 1s out-of-order doc
            document(timestamp.minus(1, unit), "vm-dev01", "cpu-load", metricOffset + 2),
            // t + 3s
            document(timestamp.plus(3, unit), "vm-dev01", "cpu-load", metricOffset + 3),
            document(timestamp.plus(3, unit), "vm-dev02", "cpu-load", metricOffset + 4)
        );

        // Verify that documents are created
        for (var result : results) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(result.getVersion(), equalTo(1L));
            docs.put(result.getId(), result.getIndex());
        }

        // Get by synthetic _id
        //
        // Documents ids and versions are resolved using the translog. Here we exercise the get-from-translog (that uses the
        // TranslogDirectoryReader) and VersionsAndSeqNoResolver.loadDocIdAndVersionUncached paths.
        randomDocs = randomSubsetOf(randomIntBetween(1, results.length), results);
        for (var doc : randomDocs) {
            var getResponse = client().prepareGet(doc.getIndex(), doc.getId()).setRealtime(true).setFetchSource(true).execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getVersion(), equalTo(1L));

            var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
            assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(metricOffset + doc.getItemId()));
        }

        flushAndRefresh(dataStreamName);

        // Get by synthetic _id
        //
        // Here we exercise the get-from-searcher and VersionsAndSeqNoResolver.timeSeriesLoadDocIdAndVersion paths.
        randomDocs = randomSubsetOf(randomIntBetween(1, results.length), results);
        for (var doc : randomDocs) {
            var getResponse = client().prepareGet(doc.getIndex(), doc.getId())
                .setRealtime(randomBoolean())
                .setFetchSource(true)
                .execute()
                .actionGet();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getVersion(), equalTo(1L));

            var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
            assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(metricOffset + doc.getItemId()));
        }

        // Check that synthetic _id field have no postings on disk
        var indices = new HashSet<>(docs.values());
        for (var index : indices) {
            var diskUsage = diskUsage(index);
            var diskUsageIdField = AnalyzeIndexDiskUsageTestUtils.getPerFieldDiskUsage(diskUsage, IdFieldMapper.NAME);
            assertThat("_id field should not have postings on disk", diskUsageIdField.getInvertedIndexBytes(), equalTo(0L));
        }

        assertHitCount(client().prepareSearch(dataStreamName).setSize(0), 10L);
    }

    private static XContentBuilder document(Instant timestamp, String hostName, String metricField, Integer metricValue)
        throws IOException {
        var source = XContentFactory.jsonBuilder();
        source.startObject();
        {
            source.field("@timestamp", DATE_FORMATTER.format(timestamp));
            source.field("hostname", hostName);
            source.startObject("metric");
            {
                source.field("field", metricField);
                source.field("value", metricValue);

            }
            source.endObject();
        }
        source.endObject();
        return source;
    }

    private static BulkItemResponse[] createDocuments(String indexName, XContentBuilder... docs) {
        assertThat(docs, notNullValue());
        final var client = client();
        var bulkRequest = client.prepareBulk();
        for (var doc : docs) {
            bulkRequest.add(client.prepareIndex(indexName).setOpType(DocWriteRequest.OpType.CREATE).setSource(doc));
        }
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);
        return bulkResponse.getItems();
    }

    private static void putDataStreamTemplate(String indexPattern, int shards) throws IOException {
        final var settings = indexSettings(shards, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            .put(IndexSettings.USE_SYNTHETIC_ID.getKey(), true);

        final var mappings = """
            {
                "_doc": {
                    "properties": {
                        "@timestamp": {
                            "type": "date"
                        },
                        "hostname": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "metric": {
                            "properties": {
                                "field": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                },
                                "value": {
                                    "type": "integer",
                                    "time_series_metric": "counter"
                                }
                            }
                        }
                    }
                }
            }""";

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request(getTestClass().getName().toLowerCase(Locale.ROOT))
            .indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of(indexPattern))
                    .template(new Template(settings.build(), new CompressedXContent(mappings), null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                    .build()
            );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());
    }

    private static IndexDiskUsageStats diskUsage(String indexName) {
        var diskUsageResponse = client().execute(
            TransportAnalyzeIndexDiskUsageAction.TYPE,
            new AnalyzeIndexDiskUsageRequest(new String[] { indexName }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, false)
        ).actionGet();

        var indexDiskUsageStats = AnalyzeIndexDiskUsageTestUtils.getIndexStats(diskUsageResponse, indexName);
        assertNotNull(indexDiskUsageStats);
        return indexDiskUsageStats;
    }
}
