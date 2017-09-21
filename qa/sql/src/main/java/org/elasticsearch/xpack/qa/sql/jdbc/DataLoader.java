/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.jdbc;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class DataLoader {

    public static void main(String[] args) throws Exception {
        try (RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build()) {
            loadDatasetIntoEs(client);
            Loggers.getLogger(DataLoader.class).info("Data loaded");
        }
    }

    protected static void loadDatasetIntoEs(RestClient client) throws Exception {
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("settings");
        {
            createIndex.field("number_of_shards", 1);
        }
        createIndex.endObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("emp");
            {
                createIndex.startObject("properties");
                {
                    createIndex.startObject("emp_no").field("type", "integer").endObject();
                    createIndex.startObject("birth_date").field("type", "date").endObject();
                    createIndex.startObject("first_name").field("type", "text").endObject();
                    createIndex.startObject("last_name").field("type", "text").endObject();
                    createIndex.startObject("gender").field("type", "keyword").endObject();
                    createIndex.startObject("hire_date").field("type", "date").endObject();
                }
                createIndex.endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject().endObject();
        client.performRequest("PUT", "/test_emp", emptyMap(), new StringEntity(createIndex.string(), ContentType.APPLICATION_JSON));

        StringBuilder bulk = new StringBuilder();
        csvToLines("employees", (titles, fields) -> {
            bulk.append("{\"index\":{}}\n");
            bulk.append('{');
            for (int f = 0; f < fields.size(); f++) {
                if (f != 0) {
                    bulk.append(',');
                }
                bulk.append('"').append(titles.get(f)).append("\":\"").append(fields.get(f)).append('"');
            }
            bulk.append("}\n");
        });
        client.performRequest("POST", "/test_emp/emp/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
    }

    private static void csvToLines(String name, CheckedBiConsumer<List<String>, List<String>, Exception> consumeLine) throws Exception {
        String location = "/" + name + ".csv";
        URL dataSet = SqlSpecTestCase.class.getResource(location);
        if (dataSet == null) {
            throw new IllegalArgumentException("Can't find [" + location + "]");
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(readFromJarUrl(dataSet), StandardCharsets.UTF_8))) {
            String titlesString = reader.readLine();
            if (titlesString == null) {
                throw new IllegalArgumentException("[" + location + "] must contain at least a title row");
            }
            List<String> titles = Arrays.asList(titlesString.split(","));    

            String line;
            while ((line = reader.readLine()) != null) {
                consumeLine.accept(titles, Arrays.asList(line.split(",")));
            }
        }
    }

    @SuppressForbidden(reason = "test reads from jar")
    public static InputStream readFromJarUrl(URL source) throws IOException {
        return source.openStream();
    }
}
