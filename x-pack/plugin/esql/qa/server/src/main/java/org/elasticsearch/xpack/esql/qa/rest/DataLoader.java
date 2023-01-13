/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import org.apache.http.HttpEntity;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ql.TestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Loads ESQL dataset into ES.
 *
 * While the loader could be made generic, the queries are bound to each index and generalizing that would make things way too complicated.
 */
public class DataLoader {
    public static final String TEST_INDEX_SIMPLE = "simple";

    private static final Map<String, String[]> replacementPatterns = Collections.unmodifiableMap(getReplacementPatterns());

    private static Map<String, String[]> getReplacementPatterns() {
        final Map<String, String[]> map = Maps.newMapWithExpectedSize(1);
        map.put("[runtime_random_keyword_type]", new String[] { "keyword", "wildcard" });
        return map;
    }

    public static void loadDatasetIntoEs(RestClient client, CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p)
        throws IOException {
        load(client, TEST_INDEX_SIMPLE, null, null, p);
    }

    private static void load(
        RestClient client,
        String indexNames,
        String dataName,
        Consumer<Map<String, Object>> datasetTransform,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p
    ) throws IOException {
        String[] splitNames = indexNames.split(",");
        for (String indexName : splitNames) {
            String name = "/data/" + indexName + ".mapping";
            URL mapping = DataLoader.class.getResource(name);
            if (mapping == null) {
                throw new IllegalArgumentException("Cannot find resource " + name);
            }
            name = "/data/" + (dataName != null ? dataName : indexName) + ".data";
            URL data = DataLoader.class.getResource(name);
            if (data == null) {
                throw new IllegalArgumentException("Cannot find resource " + name);
            }
            createTestIndex(client, indexName, readMapping(mapping));
            loadData(client, indexName, datasetTransform, data, p);
        }
    }

    private static void createTestIndex(RestClient client, String indexName, String mapping) throws IOException {
        ESRestTestCase.createIndex(client, indexName, null, mapping, null);
    }

    /**
     * Reads the mapping file, ignoring comments and replacing placeholders for random types.
     */
    private static String readMapping(URL resource) throws IOException {
        try (BufferedReader reader = TestUtils.reader(resource)) {
            StringBuilder b = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#") == false) {
                    for (Entry<String, String[]> entry : replacementPatterns.entrySet()) {
                        line = line.replace(entry.getKey(), ESRestTestCase.randomFrom(entry.getValue()));
                    }
                    b.append(line);
                }
            }
            return b.toString();
        }
    }

    @SuppressWarnings("unchecked")
    private static void loadData(
        RestClient client,
        String indexName,
        Consumer<Map<String, Object>> datasetTransform,
        URL resource,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p
    ) throws IOException {
        Request request = new Request("POST", "/_bulk");
        StringBuilder builder = new StringBuilder();

        try (XContentParser parser = p.apply(JsonXContent.jsonXContent, TestUtils.inputStream(resource))) {
            List<Object> list = parser.list();
            for (Object item : list) {
                assertThat(item, instanceOf(Map.class));
                Map<String, Object> entry = (Map<String, Object>) item;
                if (datasetTransform != null) {
                    datasetTransform.accept(entry);
                }
                builder.append("{\"index\": {\"_index\":\"" + indexName + "\"}}\n");
                builder.append(toJson(entry));
                builder.append("\n");
            }
        }
        request.setJsonEntity(builder.toString());
        request.addParameter("refresh", "wait_for");
        Response response = client.performRequest(request);
        if (response.getStatusLine().getStatusCode() == 200) {
            HttpEntity entity = response.getEntity();
            try (InputStream content = entity.getContent()) {
                XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
                Map<String, Object> result = XContentHelper.convertToMap(xContentType.xContent(), content, false);
                Object errors = result.get("errors");
                if (Boolean.FALSE.equals(errors)) {
                    LogManager.getLogger(DataLoader.class).info("Data loading OK");
                } else {
                    LogManager.getLogger(DataLoader.class).info("Data loading FAILED");
                }
            }
        } else {
            LogManager.getLogger(DataLoader.class).info("Error loading data: " + response.getStatusLine());
        }

    }

    private static String toJson(Map<String, Object> body) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent()).map(body)) {
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
