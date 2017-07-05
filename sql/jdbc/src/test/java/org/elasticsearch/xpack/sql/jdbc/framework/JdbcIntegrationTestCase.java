/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.jdbc.SqlSpecIT;
import org.junit.ClassRule;
import org.relique.io.TableReader;
import org.relique.jdbc.csv.CsvConnection;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.sql.jdbc.framework.JdbcAssert.assertResultSets;

public abstract class JdbcIntegrationTestCase extends ESRestTestCase {
    /**
     * Should the HTTP server that serves SQL be embedded in the test
     * process (true) or should the JDBC driver connect to Elasticsearch
     * running at {@code tests.rest.cluster}. Note that to use embedded
     * HTTP you have to have Elasticsearch's transport protocol open on
     * port 9300 but the Elasticsearch running there does not need to have
     * the SQL plugin installed. Note also that embedded HTTP is faster
     * but is not canonical because it runs against a different HTTP server
     * then JDBC will use in production. Gradle always uses non-embedded.
     */
    private static final boolean EMBED_SQL = Booleans.parseBoolean(System.getProperty("tests.embed.sql", "false"));

    /**
     * Properties used when settings up a CSV-based jdbc connection.
     */
    private static final Properties CSV_PROPERTIES = new Properties();
    static {
        CSV_PROPERTIES.setProperty("charset", "UTF-8");
        // trigger auto-detection
        CSV_PROPERTIES.setProperty("columnTypes", "");
        CSV_PROPERTIES.setProperty("separator", "|");
        CSV_PROPERTIES.setProperty("trimValues", "true");
    }

    @ClassRule
    public static final AbstractJdbcConnectionSource ES = EMBED_SQL ? new EmbeddedJdbcServer() : new AbstractJdbcConnectionSource() {
        @Override
        public Connection get() throws SQLException {
            return DriverManager.getConnection("jdbc:es://" + System.getProperty("tests.rest.cluster"));
        }
    };

    public Connection esJdbc() throws SQLException {
        return ES.get();
    }

    public static void index(String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        body.accept(builder);
        builder.endObject();
        HttpEntity doc = new StringEntity(builder.string(), ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + index + "/doc/1", singletonMap("refresh", "true"), doc);
    }

    public void assertMatchesCsv(String query, String csvTableName, String expectedResults) throws SQLException {
        Reader reader = new StringReader(expectedResults); 
        TableReader tableReader = new TableReader() {
            @Override
            public Reader getReader(Statement statement, String tableName) throws SQLException {
                return reader;
            }

            @Override
            public List<String> getTableNames(Connection connection) throws SQLException {
                throw new UnsupportedOperationException();
            }
        };
        try (Connection csv = new CsvConnection(tableReader, CSV_PROPERTIES, "") {};
             Connection es = esJdbc()) {
            // pass the testName as table for debugging purposes (in case the underlying reader is missing)
            ResultSet expected = csv.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
                    .executeQuery("SELECT * FROM " + csvTableName);
            // trigger data loading for type inference
            expected.beforeFirst();
            ResultSet actual = es.createStatement().executeQuery(query);
            assertResultSets(expected, actual);
        }
    }

    protected String clusterName() {
        try {
            String response = EntityUtils.toString(client().performRequest("GET", "/").getEntity());
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false).get("cluster_name").toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void loadDatasetIntoEs() throws Exception {
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
        client().performRequest("PUT", "/test_emp", emptyMap(), new StringEntity(createIndex.string(), ContentType.APPLICATION_JSON));

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
        client().performRequest("POST", "/test_emp/emp/_bulk", singletonMap("refresh", "true"), new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
    }
    
    private static void csvToLines(String name, CheckedBiConsumer<List<String>, List<String>, Exception> consumeLine) throws Exception {
        String location = "/" + name + ".csv";
        URL dataSet = SqlSpecIT.class.getResource(location);
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
}
