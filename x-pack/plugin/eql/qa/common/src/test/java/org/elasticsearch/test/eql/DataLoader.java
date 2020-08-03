/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test.eql;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class DataLoader {

    private static final String TEST_DATA = "/test_data.json";
    private static final String MAPPING = "/mapping-default.json";
    static final String indexPrefix = "endgame";
    public static final String testIndexName = indexPrefix + "-1.4.0";

    public static void main(String[] args) throws IOException {
        try (RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build()) {
            loadDatasetIntoEs(new RestHighLevelClient(
                client,
                ignore -> {
                },
                List.of()) {
            }, (t, u) -> createParser(t, u));
        }
    }

    public static void loadDatasetIntoEs(RestHighLevelClient client,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p) throws IOException {

        createTestIndex(client);
        loadData(client, p);
    }

    private static void createTestIndex(RestHighLevelClient client) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(testIndexName)
            .mapping(Streams.readFully(DataLoader.class.getResourceAsStream(MAPPING)), XContentType.JSON);

        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @SuppressWarnings("unchecked")
    private static void loadData(RestHighLevelClient client, CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p)
        throws IOException {
        BulkRequest bulk = new BulkRequest();
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        try (XContentParser parser = p.apply(JsonXContent.jsonXContent, DataLoader.class.getResourceAsStream(TEST_DATA))) {
            List<Object> list = parser.list();
            for (Object item : list) {
                assertThat(item, instanceOf(Map.class));
                Map<String, Object> entry = (Map<String, Object>) item;
                transformDataset(entry);
                bulk.add(new IndexRequest(testIndexName).source(entry, XContentType.JSON));
            }
        }

        if (bulk.numberOfActions() > 0) {
            BulkResponse bulkResponse = client.bulk(bulk, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                LogManager.getLogger(DataLoader.class).info("Data FAILED loading");
            } else {
                LogManager.getLogger(DataLoader.class).info("Data loaded");
            }
        }
    }

    private static void transformDataset(Map<String, Object> entry) {
        Object object = entry.get("timestamp");
        assertThat(object, instanceOf(Long.class));
        Long ts = (Long) object;
        // currently this is windows filetime
        entry.put("@timestamp", winFileTimeToUnix(ts));
    }


    private static final long FILETIME_EPOCH_DIFF = 11644473600000L;
    private static final long FILETIME_ONE_MILLISECOND = 10 * 1000;

    public static long winFileTimeToUnix(final long filetime) {
        long ts = (filetime / FILETIME_ONE_MILLISECOND);
        return ts - FILETIME_EPOCH_DIFF;
    }

    private static XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        NamedXContentRegistry contentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        return xContent.createParser(contentRegistry, LoggingDeprecationHandler.INSTANCE, data);
    }
}
