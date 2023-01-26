/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ql.TestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
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

    /**
     * <p>
     * Loads spec data on a local ES server.
     * </p>
     * <p>
     * Accepts an URL as first argument, eg. http://localhost:9200 or http://user:pass@localhost:9200
     *</p>
     * <p>
     * If no arguments are specified, the default URL is http://localhost:9200 without authentication
     * </p>
     * <p>
     * It also supports HTTPS
     * </p>
     * @param args the URL to connect
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String protocol = "http";
        String host = "localhost";
        int port = 9200;
        String username = null;
        String password = null;
        if (args.length > 0) {
            URL url = URI.create(args[0]).toURL();
            protocol = url.getProtocol();
            host = url.getHost();
            port = url.getPort();
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("Please specify a valid port [0 - 65535], found [" + port + "]");
            }
            String userInfo = url.getUserInfo();
            if (userInfo != null) {
                if (userInfo.contains(":") == false || userInfo.split(":").length != 2) {
                    throw new IllegalArgumentException("Invalid user credentials [username:password], found [" + userInfo + "]");
                }
                String[] userPw = userInfo.split(":");
                username = userPw[0];
                password = userPw[1];
            }
        }
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, protocol));
        if (username != null) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder = builder.setHttpClientConfigCallback(
                httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            );
        }

        try (RestClient client = builder.build()) {
            loadDatasetIntoEs(client, DataLoader::createParser);
        }
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
     * Reads the mapping file, ignoring comments
     */
    private static String readMapping(URL resource) throws IOException {
        try (BufferedReader reader = TestUtils.reader(resource)) {
            StringBuilder b = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#") == false) {
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

    private static XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        NamedXContentRegistry contentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(contentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        return xContent.createParser(config, data);
    }
}
