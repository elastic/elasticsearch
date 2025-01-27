/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.seqno;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RetentionLeaseRestIT extends ESRestTestCase {
    private static final String ADD_RETENTION_LEASE_ENDPOINT = "/%s/seq_no/add_retention_lease";
    private static final String BULK_INDEX_ENDPOINT = "/%s/_bulk";
    private static final String[] DOCUMENT_NAMES = { "alpha", "beta", "gamma", "delta" };

    @Before
    public void assumeSnapshotBuild() {
        assumeTrue("/{index}/seq_no/add_retention_lease endpoint only available in snapshot builds", Build.current().isSnapshot());
    }

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testAddRetentionLeaseSuccessfully() throws IOException {
        final String indexName = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        assertTrue(indexExists(indexName));

        assertOK(bulkIndex(indexName, randomIntBetween(10, 20)));

        final Request retentionLeaseRequest = new Request("PUT", String.format(Locale.ROOT, ADD_RETENTION_LEASE_ENDPOINT, indexName));
        final String retentionLeaseId = randomAlphaOfLength(6);
        final String retentionLeaseSource = randomAlphaOfLength(8);
        retentionLeaseRequest.addParameter("id", retentionLeaseId);
        retentionLeaseRequest.addParameter("source", retentionLeaseSource);

        final Response response = client().performRequest(retentionLeaseRequest);
        assertOK(response);

        assertRetentionLeaseResponseContent(response, indexName, indexName, retentionLeaseId, retentionLeaseSource);
        assertRetentionLeaseExists(indexName, retentionLeaseId, retentionLeaseSource);
    }

    public void testAddRetentionLeaseWithoutIdAndSource() throws IOException {
        final String indexName = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        assertTrue(indexExists(indexName));

        assertOK(bulkIndex(indexName, randomIntBetween(10, 20)));

        final Request retentionLeaseRequest = new Request("PUT", String.format(Locale.ROOT, ADD_RETENTION_LEASE_ENDPOINT, indexName));

        final Response response = client().performRequest(retentionLeaseRequest);
        assertOK(response);

        assertRetentionLeaseResponseContent(response, indexName, indexName, null, null);
    }

    public void testAddRetentionLeaseToDataStream() throws IOException {
        final String templateName = randomAlphanumericOfLength(8).toLowerCase(Locale.ROOT);
        assertOK(createIndexTemplate(templateName, """
            {
              "index_patterns": [ "test-*-*" ],
              "data_stream": {},
              "priority": 100,
              "template": {
                "settings": {
                  "number_of_shards": 1,
                  "number_of_replicas": 0
                },
                "mappings": {
                  "properties": {
                    "@timestamp": {
                      "type": "date"
                    },
                    "name": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }
            """));

        final String dataStreamName = "test-"
            + randomAlphanumericOfLength(5).toLowerCase(Locale.ROOT)
            + "-"
            + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        assertOK(createDataStream(dataStreamName));
        assertOK(bulkIndex(dataStreamName, randomIntBetween(10, 20)));

        final Request retentionLeaseRequest = new Request("PUT", String.format(Locale.ROOT, ADD_RETENTION_LEASE_ENDPOINT, dataStreamName));
        final String retentionLeaseId = randomAlphaOfLength(6);
        final String retentionLeaseSource = randomAlphaOfLength(8);
        retentionLeaseRequest.addParameter("id", retentionLeaseId);
        retentionLeaseRequest.addParameter("source", retentionLeaseSource);

        final Response response = client().performRequest(retentionLeaseRequest);
        assertOK(response);

        final String dataStreamBackingIndex = getFirstBackingIndex(dataStreamName);
        assertRetentionLeaseResponseContent(response, dataStreamName, dataStreamBackingIndex, retentionLeaseId, retentionLeaseSource);
        assertRetentionLeaseExists(dataStreamBackingIndex, retentionLeaseId, retentionLeaseSource);
    }

    public void testAddRetentionLeaseUsingAlias() throws IOException {
        final String indexName = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        assertTrue(indexExists(indexName));

        final String aliasName = randomAlphanumericOfLength(8).toLowerCase(Locale.ROOT);
        final Request putAliasRequest = new Request("PUT", "/" + indexName + "/_alias/" + aliasName);
        assertOK(client().performRequest(putAliasRequest));

        assertOK(bulkIndex(aliasName, randomIntBetween(10, 20)));

        final Request retentionLeaseRequest = new Request("PUT", String.format(Locale.ROOT, ADD_RETENTION_LEASE_ENDPOINT, aliasName));
        final String retentionLeaseId = randomAlphaOfLength(6);
        final String retentionLeaseSource = randomAlphaOfLength(8);
        retentionLeaseRequest.addParameter("id", retentionLeaseId);
        retentionLeaseRequest.addParameter("source", retentionLeaseSource);

        final Response response = client().performRequest(retentionLeaseRequest);
        assertOK(response);

        assertRetentionLeaseResponseContent(response, aliasName, indexName, retentionLeaseId, retentionLeaseSource);
        assertRetentionLeaseExists(indexName, retentionLeaseId, retentionLeaseSource);
    }

    public void testAddRetentionLeaseMissingIndex() throws IOException {
        final String missingIndexName = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        assertFalse(indexExists(missingIndexName));

        final Request retentionLeaseRequest = new Request(
            "PUT",
            String.format(Locale.ROOT, ADD_RETENTION_LEASE_ENDPOINT, missingIndexName)
        );
        final ResponseException exception = assertThrows(ResponseException.class, () -> client().performRequest(retentionLeaseRequest));
        assertResponseException(exception, RestStatus.BAD_REQUEST, "Error adding retention lease for [" + missingIndexName + "]");
    }

    public void testAddRetentionLeaseInvalidParameters() throws IOException {
        final String indexName = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        assertTrue(indexExists(indexName));
        assertOK(bulkIndex(indexName, randomIntBetween(10, 20)));

        final Request retentionLeaseRequest = new Request("PUT", String.format(Locale.ROOT, ADD_RETENTION_LEASE_ENDPOINT, indexName));
        retentionLeaseRequest.addParameter("id", null);
        retentionLeaseRequest.addParameter("source", randomBoolean() ? UUIDs.randomBase64UUID() : "test-source");

        final ResponseException exception = assertThrows(ResponseException.class, () -> client().performRequest(retentionLeaseRequest));
        assertResponseException(exception, RestStatus.BAD_REQUEST, "retention lease ID can not be empty");
    }

    public void testAddMultipleRetentionLeasesForSameShard() throws IOException {
        final String indexName = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        assertTrue(indexExists(indexName));
        assertOK(bulkIndex(indexName, randomIntBetween(10, 20)));

        int numberOfLeases = randomIntBetween(2, 5);
        for (int i = 0; i < numberOfLeases; i++) {
            final Request retentionLeaseRequest = new Request("PUT", String.format(Locale.ROOT, ADD_RETENTION_LEASE_ENDPOINT, indexName));
            retentionLeaseRequest.addParameter("id", "lease-" + i);
            retentionLeaseRequest.addParameter("source", "test-source-" + i);

            final Response response = client().performRequest(retentionLeaseRequest);
            assertOK(response);

            assertRetentionLeaseResponseContent(response, indexName, indexName, "lease-" + i, "test-source-" + i);
        }

        for (int i = 0; i < numberOfLeases; i++) {
            assertRetentionLeaseExists(indexName, "lease-" + i, "test-source-" + i);
        }
    }

    private static Response bulkIndex(final String indexName, int numberOfDocuments) throws IOException {
        final StringBuilder sb = new StringBuilder();
        long timestamp = System.currentTimeMillis();

        for (int i = 0; i < numberOfDocuments; i++) {
            sb.append(
                String.format(
                    Locale.ROOT,
                    "{ \"index\": {} }\n{ \"@timestamp\": \"%s\", \"name\": \"%s\" }\n",
                    Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                    randomFrom(DOCUMENT_NAMES)
                )
            );
            timestamp += 1000;
        }

        final Request request = new Request("POST", String.format(Locale.ROOT, BULK_INDEX_ENDPOINT, indexName));
        request.setJsonEntity(sb.toString());
        request.addParameter("refresh", "true");
        return client().performRequest(request);
    }

    private void assertResponseException(final ResponseException exception, final RestStatus expectedStatus, final String expectedMessage) {
        assertEquals(expectedStatus.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        assertTrue(exception.getMessage().contains(expectedMessage));
    }

    private Map<String, Object> getRetentionLeases(final String indexName) throws IOException {
        final Request statsRequest = new Request("GET", "/" + indexName + "/_stats");
        statsRequest.addParameter("level", "shards");

        final Response response = client().performRequest(statsRequest);
        assertOK(response);

        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            EntityUtils.toString(response.getEntity()),
            false
        );

        @SuppressWarnings("unchecked")
        final Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        if (indices == null || indices.containsKey(indexName) == false) {
            throw new IllegalArgumentException("No shard stats found for: " + indexName);
        }

        @SuppressWarnings("unchecked")
        final Map<String, Object> shards = (Map<String, Object>) ((Map<String, Object>) indices.get(indexName)).get("shards");

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> shardList = (List<Map<String, Object>>) shards.get("0");

        return getRetentionLeases(indexName, shardList);
    }

    private static Map<String, Object> getRetentionLeases(final String indexName, final List<Map<String, Object>> shardList) {
        final Map<String, Object> shardStats = shardList.getFirst();

        @SuppressWarnings("unchecked")
        final Map<String, Object> retentionLeases = (Map<String, Object>) shardStats.get("retention_leases");
        if (retentionLeases == null) {
            throw new IllegalArgumentException("No retention leases found for shard 0 of index: " + indexName);
        }
        return retentionLeases;
    }

    private void assertRetentionLeaseExists(
        final String indexAbstractionName,
        final String expectedRetentionLeaseId,
        final String expectedRetentionLeaseSource
    ) throws IOException {
        final Map<String, Object> retentionLeases = getRetentionLeases(indexAbstractionName);

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> leases = (List<Map<String, Object>>) retentionLeases.get("leases");

        boolean retentionLeaseExists = leases.stream().anyMatch(lease -> {
            final String id = (String) lease.get("id");
            final String source = (String) lease.get("source");
            return expectedRetentionLeaseId.equals(id) && expectedRetentionLeaseSource.equals(source);
        });

        assertTrue(
            "Retention lease with ID [" + expectedRetentionLeaseId + "] and source [" + expectedRetentionLeaseSource + "] does not exist.",
            retentionLeaseExists
        );
    }

    private Response createDataStream(final String dataStreamName) throws IOException {
        return client().performRequest(new Request("PUT", "/_data_stream/" + dataStreamName));
    }

    private String getFirstBackingIndex(final String dataStreamName) throws IOException {
        final Response response = client().performRequest(new Request("GET", "/_data_stream/" + dataStreamName));

        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            EntityUtils.toString(response.getEntity()),
            false
        );

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> dataStreams = (List<Map<String, Object>>) responseMap.get("data_streams");

        if (dataStreams == null || dataStreams.isEmpty()) {
            throw new IllegalArgumentException("No data stream found for name: " + dataStreamName);
        }

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> backingIndices = (List<Map<String, Object>>) dataStreams.get(0).get("indices");

        if (backingIndices == null || backingIndices.isEmpty()) {
            throw new IllegalArgumentException("No backing indices found for data stream: " + dataStreamName);
        }

        return (String) backingIndices.getFirst().get("index_name");
    }

    private static Response createIndexTemplate(final String templateName, final String mappings) throws IOException {
        final Request request = new Request("PUT", "/_index_template/" + templateName);
        request.setJsonEntity(mappings);
        return client().performRequest(request);
    }

    private void assertRetentionLeaseResponseContent(
        final Response response,
        final String expectedIndexAbstraction,
        final String expectedConcreteIndex,
        final String expectedLeaseId,
        final String expectedLeaseSource
    ) throws IOException {
        final Map<String, Object> responseBody = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            EntityUtils.toString(response.getEntity()),
            false
        );

        assertEquals("Unexpected index abstraction in response", expectedIndexAbstraction, responseBody.get("index_abstraction"));
        assertEquals("Unexpected concrete index in response", expectedConcreteIndex, responseBody.get("index"));
        assertNotNull("Shard ID missing in response", responseBody.get("shard_id"));

        if (expectedLeaseId != null) {
            assertEquals("Unexpected lease ID in response", expectedLeaseId, responseBody.get("id"));
        }
        if (expectedLeaseSource != null) {
            assertEquals("Unexpected lease source in response", expectedLeaseSource, responseBody.get("source"));
        }
    }
}
