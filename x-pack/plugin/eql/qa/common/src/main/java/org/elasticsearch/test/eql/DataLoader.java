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
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DataLoader {

    private static final String TEST_DATA = "/test_data.json";
    private static final String MAPPING = "/mapping-default.json";
    static final String indexPrefix = "endgame";
    static final String testIndexName = indexPrefix + "-1.4.0";

    public static void main(String[] args) throws IOException {
        try (RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build()) {
            loadDatasetIntoEs(new RestHighLevelClient(
                client,
                ignore -> {
                },
                Collections.emptyList()) {
            }, (t, u) -> createParser(t, u));
        }
    }

    @SuppressWarnings("unchecked")
    protected static void loadDatasetIntoEs(RestHighLevelClient client,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p) throws IOException {

        CreateIndexRequest request = new CreateIndexRequest(testIndexName)
            .mapping(Streams.readFully(DataLoader.class.getResourceAsStream(MAPPING)), XContentType.JSON);

        client.indices().create(request, RequestOptions.DEFAULT);

        BulkRequest bulk = new BulkRequest();
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        try (XContentParser parser = p.apply(JsonXContent.jsonXContent, DataLoader.class.getResourceAsStream(TEST_DATA))) {
            List<Object> list = parser.list();
            for (Object item : list) {
                bulk.add(new IndexRequest(testIndexName).source((Map<String, Object>) item, XContentType.JSON));
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

    private static XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        NamedXContentRegistry contentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        return xContent.createParser(contentRegistry, LoggingDeprecationHandler.INSTANCE, data);
    }
}
