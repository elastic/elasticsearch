/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.ql.TestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class CsvTestsDataLoader {
    public static final String TEST_INDEX_SIMPLE = "test";

    private static final Logger LOGGER = LogManager.getLogger(CsvTestsDataLoader.class);

    public static void main(String[] args) throws IOException {
        String protocol = "http";
        String host = "localhost";
        int port = 9200;

        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, protocol));
        try (RestClient client = builder.build()) {
            loadDatasetIntoEs(client, CsvTestsDataLoader::createParser);
        }
    }

    public static void loadDatasetIntoEs(RestClient client, CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p)
        throws IOException {
        load(client, TEST_INDEX_SIMPLE, "/mapping-default.json", "/employees.csv", p);
    }

    private static void load(
        RestClient client,
        String indexName,
        String mappingName,
        String dataName,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p
    ) throws IOException {
        URL mapping = CsvTestsDataLoader.class.getResource(mappingName);
        if (mapping == null) {
            throw new IllegalArgumentException("Cannot find resource mapping-default.json");
        }
        URL data = CsvTestsDataLoader.class.getResource(dataName);
        if (data == null) {
            throw new IllegalArgumentException("Cannot find resource employees.csv");
        }
        createTestIndex(client, indexName, readMapping(mapping));
        loadData(client, indexName, data, p);
    }

    private static void createTestIndex(RestClient client, String indexName, String mapping) throws IOException {
        ESRestTestCase.createIndex(client, indexName, null, mapping, null);
    }

    private static String readMapping(URL resource) throws IOException {
        try (BufferedReader reader = TestUtils.reader(resource)) {
            StringBuilder b = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                b.append(line);
            }
            return b.toString();
        }
    }

    @SuppressWarnings("unchecked")
    private static void loadData(
        RestClient client,
        String indexName,
        URL resource,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p
    ) throws IOException {
        Request request = new Request("POST", "/_bulk");
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = org.elasticsearch.xpack.ql.TestUtils.reader(resource)) {
            String line;
            int lineNumber = 1;
            String[] columns = null; // list of column names. If one column name contains dot, it is a subfield and its value will be null
            List<Integer> subFieldsIndices = new ArrayList<>(); // list containing the index of a subfield in "columns" String[]

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // ignore comments
                if (line.isEmpty() == false && line.startsWith("//") == false) {
                    var entries = Strings.delimitedListToStringArray(line, ",");
                    for (int i = 0; i < entries.length; i++) {
                        entries[i] = entries[i].trim();
                    }
                    // the schema row
                    if (columns == null) {
                        columns = new String[entries.length];
                        for (int i = 0; i < entries.length; i++) {
                            int split = entries[i].indexOf(":");
                            String name, typeName;

                            if (split < 0) {
                                throw new IllegalArgumentException(
                                    "A type is always expected in the schema definition; found " + entries[i]
                                );
                            } else {
                                name = entries[i].substring(0, split).trim();
                                if (name.indexOf(".") < 0) {
                                    typeName = entries[i].substring(split + 1).trim();
                                    if (typeName.length() == 0) {
                                        throw new IllegalArgumentException(
                                            "A type is always expected in the schema definition; found " + entries[i]
                                        );
                                    }
                                } else {// if it's a subfield, ignore it in the _bulk request
                                    name = null;
                                    subFieldsIndices.add(i);
                                }
                            }
                            columns[i] = name;
                        }
                    }
                    // data rows
                    else {
                        if (entries.length != columns.length) {
                            throw new IllegalArgumentException(
                                format(
                                    null,
                                    "Error line [{}]: Incorrect number of entries; expected [{}] but found [{}]",
                                    lineNumber,
                                    columns.length,
                                    entries.length
                                )
                            );
                        }
                        StringBuilder row = new StringBuilder();
                        for (int i = 0; i < entries.length; i++) {
                            // ignore values that belong to subfields and don't add them to the bulk request
                            if (subFieldsIndices.contains(i) == false) {
                                boolean isValueNull = "".equals(entries[i]);
                                try {
                                    if (isValueNull == false) {
                                        row.append("\"" + columns[i] + "\":\"" + entries[i] + "\"");
                                    }
                                } catch (Exception e) {
                                    throw new IllegalArgumentException(
                                        format(
                                            null,
                                            "Error line [{}]: Cannot parse entry [{}] with value [{}]",
                                            lineNumber,
                                            i + 1,
                                            entries[i]
                                        ),
                                        e
                                    );
                                }
                                if (i < entries.length - 1 && isValueNull == false) {
                                    row.append(",");
                                }
                            }
                        }
                        builder.append("{\"index\": {\"_index\":\"" + indexName + "\"}}\n");
                        builder.append("{" + row + "}\n");
                    }
                }
                lineNumber++;
            }
            builder.append("\n");
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
                    LOGGER.info("Data loading OK");
                    request = new Request("POST", "/" + TEST_INDEX_SIMPLE + "/_forcemerge?max_num_segments=1");
                    response = client.performRequest(request);
                    if (response.getStatusLine().getStatusCode() != 200) {
                        LOGGER.info("Force-merge to 1 segment failed: " + response.getStatusLine());
                    }
                } else {
                    LOGGER.info("Data loading FAILED");
                }
            }
        } else {
            LOGGER.info("Error loading data: " + response.getStatusLine());
        }
    }

    private static XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        NamedXContentRegistry contentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(contentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        return xContent.createParser(config, data);
    }
}
