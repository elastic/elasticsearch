/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.sql.jdbc.h2.SqlSpecIntegrationTest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public abstract class TestUtils {

    static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    public static RestClient restClient(String host, int port) {
        return RestClient.builder(new HttpHost(host, port)).build();

    }

    public static RestClient restClient(InetAddress address) {
        return RestClient.builder(new HttpHost(address)).build();

    }

    public static Client client() {
        return new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), 9300));
    }

    public static void index(RestClient client, String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        body.accept(builder);
        builder.endObject();
        HttpEntity doc = new StringEntity(builder.string(), ContentType.APPLICATION_JSON);
        client.performRequest("PUT", "/" + index + "/doc/1", singletonMap("refresh", "true"), doc);
    }

    public static void loadDatasetInEs(RestClient client) throws Exception {
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("settings"); {
            createIndex.field("number_of_shards", 1);
        }
        createIndex.endObject();
        createIndex.startObject("mappings"); {
            createIndex.startObject("emp");
            {
                createIndex.startObject("properties"); {
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
        client.performRequest("POST", "/test_emp/emp/_bulk", singletonMap("refresh", "true"), new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
    }
    
    private static void csvToLines(String name, CheckedBiConsumer<List<String>, List<String>, Exception> consumeLine) throws Exception {
        String location = "/" + name + ".csv";
        URL dataSet = SqlSpecIntegrationTest.class.getResource(location);
        if (dataSet == null) {
            throw new IllegalArgumentException("Can't find [" + location + "]");
        }
        List<String> lines = Files.readAllLines(PathUtils.get(dataSet.toURI()));
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("[" + location + "] must contain at least a title row");
        }
        List<String> titles = Arrays.asList(lines.get(0).split(","));
        for (int l = 1; l < lines.size(); l++) {
            consumeLine.accept(titles, Arrays.asList(lines.get(l).split(",")));
        }
    }
    

    

    Throwable reworkException(Throwable th, Class<?> testSuite, String testName, Path source, int lineNumber) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(testSuite.getName(), testName, source.getFileName().toString(), lineNumber);

        th.setStackTrace(redone);
        return th;
    }

}