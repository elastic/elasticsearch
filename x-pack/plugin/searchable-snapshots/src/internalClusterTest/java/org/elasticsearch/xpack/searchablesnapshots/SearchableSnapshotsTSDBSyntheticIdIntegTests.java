/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageRequest;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageResponse;
import org.elasticsearch.action.admin.indices.diskusage.TransportAnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * The test set up is a time-series index that uses synthetic ids with some random documents added and half of them deleted.
 * A snapshot is created of the index, the index is deleted and the snapshot is mounted.
 * All tests run against the mounted snapshot.
 */
public class SearchableSnapshotsTSDBSyntheticIdIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {
    private static final String REPOSITORY = "synthetic-id-repository";
    private final String INDEX = "synthetic-id-index";
    private final String SNAPSHOT = "synthetic-id-snapshot";
    private final String MOUNTED_INDEX = "mounted-" + INDEX;
    private final String[] HOSTNAMES = new String[] { "host01", "host02", "host03", "host04" };
    private int numberOfDocuments;
    private Set<String> docIds;
    private Collection<String> deletedDocIds;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        createRepository(REPOSITORY, FsRepository.TYPE);
        assertAcked(syntheticIdIndex(INDEX));
        int initialNumberOfDocuments = scaledRandomIntBetween(20, 2_000);
        docIds = indexRandomDocuments(INDEX, initialNumberOfDocuments);
        deletedDocIds = deleteRandomDocuments(INDEX, docIds);
        numberOfDocuments = docIds.size();
        createSnapshot(REPOSITORY, SNAPSHOT, List.of(INDEX));
        assertAcked(indicesAdmin().prepareDelete(INDEX));
        mountSnapshot(
            REPOSITORY,
            SNAPSHOT,
            INDEX,
            MOUNTED_INDEX,
            Settings.EMPTY,
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        );
        ensureGreen(MOUNTED_INDEX);
    }

    private CreateIndexRequestBuilder syntheticIdIndex(String indexName) {
        Settings settings = Settings.builder()
            .put(IndexSettings.SYNTHETIC_ID.getKey(), true)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.DEFAULT_CODEC)
            .build();
        final var mapping = """
            {
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
            """;
        return prepareCreate(indexName).setSettings(settings).setMapping(mapping);
    }

    private Set<String> indexRandomDocuments(String index, int numdocs) {
        logger.debug("--> indexing [{}] documents into [{}]", numdocs, index);
        Set<String> docIds = new HashSet<>(numdocs);
        Instant now = Instant.now();
        for (int i = 0; i < numdocs; i++) {
            String source = String.format(Locale.ROOT, """
                {"@timestamp": "%s", "hostname": "%s", "metric": {"field": "cpu-load", "value": %d}}
                """, now.plus(i, ChronoUnit.SECONDS), randomFrom(HOSTNAMES), randomByte());
            DocWriteResponse response = prepareIndex(index).setSource(source, XContentType.JSON).get(TEST_REQUEST_TIMEOUT);
            docIds.add(response.getId());
        }
        flushAndRefresh(index);
        assertDocCount(index, numdocs);
        return docIds;
    }

    private Collection<String> deleteRandomDocuments(String index, Set<String> docIds) {
        var deletedDocIds = randomSubsetOf(docIds);
        for (String docId : deletedDocIds) {
            DeleteResponse response = client().prepareDelete(index, docId).get(TEST_REQUEST_TIMEOUT);
            assertThat(response.status(), Matchers.equalTo(RestStatus.OK));
            docIds.remove(docId);
        }
        return deletedDocIds;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        assertAcked(indicesAdmin().prepareDelete("mounted-*"));
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, SNAPSHOT).get());
        assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY));
        super.tearDown();
    }

    /**
     * Ideally, the test cases would be in different tests, but the setup is a bit expensive and
     * having them all together in one test cuts execution a lot.
     */
    public void testSearchableSnapshot() throws IOException {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);

        // Index exists
        assertTrue("Expected index [" + MOUNTED_INDEX + "] to exist, but did not", indexExists(MOUNTED_INDEX));

        // Setting persisted
        GetIndexResponse settingResponse = new GetIndexRequestBuilder(client(), TEST_REQUEST_TIMEOUT, MOUNTED_INDEX).get(
            TEST_REQUEST_TIMEOUT
        );
        Map<String, Settings> indexSettingsAsMap = settingResponse.getSettings();
        assertThat(indexSettingsAsMap.get(MOUNTED_INDEX).get(IndexSettings.SYNTHETIC_ID.getKey()), Matchers.equalTo("true"));

        // Correct doc count
        assertDocCount(MOUNTED_INDEX, numberOfDocuments);

        // No inverted index
        assertThat(invertedIndexSize(MOUNTED_INDEX), Matchers.equalTo(0));

        // Search by docId
        var searchIds = randomSubsetOf(Math.min(10, docIds.size()), docIds);
        for (String docId : searchIds) {
            SearchResponse searchResponse = prepareSearch(MOUNTED_INDEX).setQuery(QueryBuilders.idsQuery().addIds(docId)).get();
            try {
                assertThat(searchResponse.getHits().getTotalHits().value(), Matchers.equalTo(1L));
            } finally {
                searchResponse.decRef();
            }
        }

        // Get by docId
        for (String docId : searchIds) {
            GetResponse getResponse = client().prepareGet().setIndex(MOUNTED_INDEX).setId(docId).get();
            try {
                assertTrue(getResponse.isExists());
            } finally {
                getResponse.decRef();
            }
        }

        if (deletedDocIds.isEmpty() == false) {
            // Search by deleted doc id
            String deletedDocId = randomFrom(deletedDocIds);
            SearchResponse searchResponse = prepareSearch(MOUNTED_INDEX).setQuery(QueryBuilders.idsQuery().addIds(deletedDocId)).get();
            try {
                assertThat(searchResponse.getHits().getTotalHits().value(), Matchers.equalTo(0L));
            } finally {
                searchResponse.decRef();
            }

            // Get by deleted doc id
            GetResponse response = client().prepareGet().setIndex(MOUNTED_INDEX).setId(deletedDocId).get();
            try {
                assertFalse(response.isExists());
            } finally {
                response.decRef();
            }
        }
    }

    private int invertedIndexSize(String indexName) throws IOException {
        AnalyzeIndexDiskUsageRequest request = new AnalyzeIndexDiskUsageRequest(new String[] { indexName }, IndicesOptions.DEFAULT, false);
        ActionFuture<AnalyzeIndexDiskUsageResponse> future = client().execute(TransportAnalyzeIndexDiskUsageAction.TYPE, request);
        AnalyzeIndexDiskUsageResponse response = future.actionGet(TEST_REQUEST_TIMEOUT);

        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        ObjectPath objectPath = ObjectPath.createFromXContent(XContentType.JSON.xContent(), BytesReference.bytes(builder));
        return objectPath.evaluate(indexName + ".all_fields.inverted_index.total_in_bytes");
    }
}
