/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.compare;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.jdbc.JdbcIntegrationTestCase;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.sql.jdbc.compare.JdbcAssert.assertResultSets;

/**
 * Compares Elasticsearch's JDBC driver to H2.
 */
public abstract class CompareToH2BaseTestCase extends JdbcIntegrationTestCase {
    static final DateTimeFormatter UTC_FORMATTER = DateTimeFormatter.ISO_DATE_TIME
            .withLocale(Locale.ROOT)
            .withZone(ZoneId.of("UTC"));

    public final String queryName;
    public final String query;
    public final Integer lineNumber;
    public final Path source;

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

    public CompareToH2BaseTestCase(String queryName, String query, Integer lineNumber, Path source) {
        this.queryName = queryName;
        this.query = query;
        this.lineNumber = lineNumber;
        this.source = source;
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
            fillH2(h2);
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

    private void setupElasticsearchIndex() throws Exception {
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
        client().performRequest("POST", "/emp/emp/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
    }

    /**
     * Fill the h2 database. Note that we have to parse the CSV ourselves
     * because h2 interprets the CSV using the default locale which is
     * randomized by the testing framework. Because some locales (th-TH,
     * for example) parse dates in very different ways we parse using the
     * root locale.
     */
    private void fillH2(Connection h2) throws Exception {
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

    private void csvToLines(String name,
            CheckedBiConsumer<List<String>, List<String>, Exception> consumeLine) throws Exception {
        String location = "/" + name + ".csv";
        URL dataSet = CompareToH2BaseTestCase.class.getResource(location);
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