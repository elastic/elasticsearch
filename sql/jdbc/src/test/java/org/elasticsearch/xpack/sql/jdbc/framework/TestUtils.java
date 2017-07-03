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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public abstract class TestUtils {

    static final DateTimeFormatter UTC_FORMATTER = DateTimeFormatter.ISO_DATE_TIME
            .withLocale(Locale.ROOT)
            .withZone(ZoneId.of("UTC"));
    
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
            createIndex.startObject("emp"); {
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
    
    /**
     * Fill the H2 database. Note that we have to parse the CSV ourselves
     * because H2 interprets the CSV using the default locale which is
     * randomized by the testing framework. Because some locales (th-TH,
     * for example) parse dates in very different ways we parse using the
     * root locale.
     */
    public static void loadDatesetInH2(Connection h2) throws Exception {
        csvToLines("employees", (titles, fields) -> {
            StringBuilder insert = new StringBuilder("INSERT INTO \"emp.emp\" (");
            for (int t = 0; t < titles.size(); t++) {
                if (t != 0) {
                    insert.append(',');
                }
                insert.append('"').append(titles.get(t)).append('"');
            }
            insert.append(") VALUES (");
            for (int t = 0; t < titles.size(); t++) {
                if (t != 0) {
                    insert.append(',');
                }
                insert.append('?');
            }
            insert.append(')');

            PreparedStatement s = h2.prepareStatement(insert.toString());
            for (int t = 0; t < titles.size(); t++) {
                String field = fields.get(t);
                if (titles.get(t).endsWith("date")) {
                    /* Dates need special handling because H2 uses the default local for
                     * parsing which doesn't work because Elasticsearch always uses
                     * the "root" locale. This mismatch would cause the test to fail
                     * all the time in places like Thailand. Luckily Elasticsearch's
                     * randomized testing sometimes randomly pretends you are in
                     * Thailand and caught this.... */
                    s.setTimestamp(t + 1, new Timestamp(Instant.from(UTC_FORMATTER.parse(field)).toEpochMilli()));
                } else {
                    s.setString(t + 1, field);
                }
            }
            assertEquals(1, s.executeUpdate());
        });
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