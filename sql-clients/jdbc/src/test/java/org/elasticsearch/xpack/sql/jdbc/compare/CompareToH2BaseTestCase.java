/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.compare;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.jdbc.JdbcIntegrationTestCase;
import org.elasticsearch.xpack.sql.jdbc.integration.util.EsDataLoader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.sql.jdbc.compare.JdbcAssert.assertResultSets;

/**
 * Compares Elasticsearch's JDBC driver to H2.
 */
public abstract class CompareToH2BaseTestCase extends JdbcIntegrationTestCase {
    public final String queryName;
    public final String query;
    public final Integer lineNumber;
    public final Path source;

    public CompareToH2BaseTestCase(String queryName, String query, Integer lineNumber, Path source) {
        this.queryName = queryName;
        this.query = query;
        this.lineNumber = lineNumber;
        this.source = source;
    }

    protected static List<Object[]> readScriptSpec(String spec) throws Exception {
        String url = "/" + spec + ".spec";
        URL resource = CompareToH2BaseTestCase.class.getResource(url);
        if (resource == null) {
            throw new IllegalArgumentException("Couldn't find [" + url + "]");
        }
        Path source = PathUtils.get(resource.toURI());
        List<String> lines = Files.readAllLines(source);

        Map<String, Integer> testNames = new LinkedHashMap<>();
        List<Object[]> ctorArgs = new ArrayList<>();

        String name = null;
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            // ignore comments
            if (!line.isEmpty() && !line.startsWith("//")) {
                if (name == null) {
                    if (testNames.keySet().contains(line)) {
                        throw new IllegalStateException("Duplicate test name [" + line 
                                + "] at line [" + i + "] (previously seen at line [" + testNames.get(line) + "])");
                    } else {
                        name = line;
                        testNames.put(name, Integer.valueOf(i));
                    }
                } else {
                    if (line.endsWith(";")) {
                        query.append(line.substring(0, line.length() - 1));
                    }
                    ctorArgs.add(new Object[] { name, query.toString(), Integer.valueOf(i), source });
                    name = null;
                    query.setLength(0);
                }
            }
        }
        assertNull("Cannot find query for test " + name, name);

        return ctorArgs;
    }

    public void testQuery() throws Throwable {
        /*
         * The syntax on the connection string is fairly particular:
         *      mem:; creates an anonymous database in memory. The `;` is
         *              technically the separator that comes after the name.
         *      DATABASE_TO_UPPER=false turns *off* H2's Oracle-like habit
         *              of upper-casing everything that isn't quoted.
         *      ALIAS_COLUMN_NAME=true turn *on* returning alias names in
         *              result set metadata which is what most DBs do except
         *              for MySQL and, by default, H2. Our jdbc driver does it.
         *      RUNSCRIPT FROM 'classpath:/h2-setup.sql' initializes the
         *              database with test data.
         */
        try (Connection h2 = DriverManager.getConnection(
                "jdbc:h2:mem:;DATABASE_TO_UPPER=false;ALIAS_COLUMN_NAME=true;INIT=RUNSCRIPT FROM 'classpath:/h2-setup.sql'")) {
            try (PreparedStatement h2Query = h2.prepareStatement(query);
                    ResultSet expected = h2Query.executeQuery()) {
                setupElasticsearchIndex();
                j.query(query, actual -> {
                    assertResultSets(expected, actual);
                    return null;
                });
            };
        }
    }

    private void setupElasticsearchIndex() throws IOException, URISyntaxException {
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
        client().performRequest("PUT", "/emp", emptyMap(), new StringEntity(createIndex.string(), ContentType.APPLICATION_JSON));

        URL dataSet = EsDataLoader.class.getResource("/employees.csv");
        if (dataSet == null) {
            throw new IllegalArgumentException("Can't find employees.csv");
        }
        StringBuilder bulk = new StringBuilder();
        List<String> lines = Files.readAllLines(PathUtils.get(dataSet.toURI()));
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("employees.csv must contain at least a title row");
        }
        String[] titles = lines.get(0).split(",");
        for (int t = 0; t < titles.length; t++) {
            titles[t] = titles[t].replaceAll("\"", "");
        }
        for (int l = 1; l < lines.size(); l++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append('{');
            String[] columns = lines.get(l).split(",");
            for (int c = 0; c < columns.length; c++) {
                if (c != 0) {
                    bulk.append(',');
                }
                bulk.append('"').append(titles[c]).append("\":\"").append(columns[c]).append('"');
            }
            bulk.append("}\n");
        }
        client().performRequest("POST", "/emp/emp/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
    }
}