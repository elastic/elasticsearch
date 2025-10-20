/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageRequest;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageTestUtils;
import org.elasticsearch.action.admin.indices.diskusage.IndexDiskUsageStats;
import org.elasticsearch.action.admin.indices.diskusage.TransportAnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.time.FormatNames.STRICT_DATE_OPTIONAL_TIME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * Test suite for time series indices that use synthetic ids for documents.
 * <p>
 * Synthetic _id fields are not indexed in Lucene, instead they are generated on demand by concatenating the values of two other fields of
 * the document (typically the {@code @timestamp} and {@code _tsid} fields).
 * </p>
 */
public class TSDBSyntheticIdsIT extends ESIntegTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern(STRICT_DATE_OPTIONAL_TIME.getName());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
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

    @TestLogging(reason = "debug", value = "org.elasticsearch.index.engine.Engine:TRACE")
    public void testSyntheticId() throws Exception {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        final var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                    .put(IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.getKey(), false)
                    .put(InternalSettingsPlugin.USE_COMPOUND_FILE.getKey(), false)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
                    .put(IndexSettings.USE_SYNTHETIC_ID.getKey(), true)
                    .build()
            )
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "hostname",
                    "type=keyword,time_series_dimension=true",
                    "metric.field",
                    "type=keyword",
                    "metric.value",
                    "type=integer"
                )
        );
        ensureGreen(indexName);

        final var timestamp = Instant.ofEpochMilli(1760957415027L);

        // Index 5 docs + 1 update
        var results = indexDocuments(
            indexName,
            document(timestamp, "vm-dev01", "cpu-load", 0),
            document(timestamp.plusSeconds(2), "vm-dev01", "cpu-load", 1),
            document(timestamp.plusSeconds(2), "vm-dev01", "cpu-load", 2), // update
            document(timestamp, "vm-dev02", "cpu-load", 3),
            document(timestamp.plusSeconds(2), "vm-dev03", "cpu-load", 4),
            document(timestamp.plusSeconds(3), "vm-dev03", "cpu-load", 5)
        );

        // Verify documents
        assertThat(results[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(results[0].getVersion(), equalTo(1L));

        assertThat(results[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(results[1].getVersion(), equalTo(1L));

        assertThat(results[2].getId(), equalTo(results[1].getId()));
        assertThat(results[2].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED)); // update
        assertThat(results[2].getVersion(), equalTo(2L));

        assertThat(results[3].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(results[3].getVersion(), equalTo(1L));

        assertThat(results[4].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(results[4].getVersion(), equalTo(1L));

        assertThat(results[5].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(results[5].getVersion(), equalTo(1L));

        // Not refreshed yet
        assertHitCount(client().prepareSearch(indexName).setSize(0), 0L);

        switch (randomInt(2)) {
            case 0:
                flush(indexName);
                break;
            case 1:
                refresh(indexName);
                break;
            case 2:
            default:
                break;
        }

        // Get by synthetic _id
        // Note: before synthetic _id this would have required postings on disks
        final var docId = results[1].getId();
        var getResponse = client().prepareGet(indexName, docId).setFetchSource(true).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2L));
        assertThat(getResponse.getId(), equalTo(docId));
        var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
        assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(2));

        flushAndRefresh(indexName);

        // Check that synthetic _id field has no postings on disk
        var diskUsage = diskUsage(indexName);
        var diskUsageIdField = AnalyzeIndexDiskUsageTestUtils.getPerFieldDiskUsage(diskUsage, IdFieldMapper.NAME);
        assertThat("_id field should not have postings on disk", diskUsageIdField.getInvertedIndexBytes(), equalTo(0L));
    }

    @FunctionalInterface
    private interface DocumentSource extends CheckedSupplier<XContentBuilder, IOException> {}

    private static DocumentSource document(Instant timestamp, String hostName, String metricField, Integer metricValue) {
        return () -> {
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
        };
    }

    private static BulkItemResponse[] indexDocuments(String indexName, DocumentSource... docs) throws IOException {
        assertThat(docs, notNullValue());
        final var client = client();
        var bulkRequest = client.prepareBulk();
        for (var doc : docs) {
            try (var source = doc.get()) {
                bulkRequest.add(client.prepareIndex(indexName).setSource(source));
            }
        }
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);
        return bulkResponse.getItems();
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

    private static IndexDiskUsageStats.PerFieldDiskUsage diskUsageForField(IndexDiskUsageStats indexDiskUsageStats, String fieldName) {
        var fieldDiskUsage = AnalyzeIndexDiskUsageTestUtils.getPerFieldDiskUsage(indexDiskUsageStats, fieldName);
        assertNotNull(fieldDiskUsage);
        return fieldDiskUsage;
    }
}
