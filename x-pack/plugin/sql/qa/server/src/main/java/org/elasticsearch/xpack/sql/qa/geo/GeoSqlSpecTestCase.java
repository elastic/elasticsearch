/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.geo;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.client.Request;
import org.elasticsearch.xpack.sql.qa.jdbc.LocalH2;
import org.elasticsearch.xpack.sql.qa.jdbc.SpecBaseIntegrationTestCase;
import org.elasticsearch.xpack.sql.jdbc.JdbcConfiguration;
import org.h2gis.functions.factory.H2GISFunctions;
import org.junit.Before;
import org.junit.ClassRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Tests comparing geo sql queries executed against our jdbc client
 * with those executed against H2GIS's jdbc client.
 */
public abstract class GeoSqlSpecTestCase extends SpecBaseIntegrationTestCase {
    private String query;

    @ClassRule
    public static LocalH2 H2 = new LocalH2((c) -> {
        // Load GIS extensions
        H2GISFunctions.load(c);
        c.createStatement().execute("RUNSCRIPT FROM 'classpath:/ogc/sqltsch.sql'");
        c.createStatement().execute("RUNSCRIPT FROM 'classpath:/geo/setup_test_geo.sql'");
    });

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        Parser parser = new SqlSpecParser();
        List<Object[]> tests = new ArrayList<>();
        tests.addAll(readScriptSpec("/ogc/ogc.sql-spec", parser));
        tests.addAll(readScriptSpec("/geo/geosql.sql-spec", parser));
        return tests;
    }

    @Before
    public void setupTestGeoDataIfNeeded() throws Exception {
        assumeTrue(
            "Cannot support locales that don't use Hindu-Arabic numerals and non-ascii - sign due to H2",
            "-42".equals(NumberFormat.getInstance(Locale.getDefault()).format(-42))
        );
        assumeTrue(
            "JTS inside H2 is using default local for toUpperCase() in string comparison making it fail to parse WKT on certain"
                + " locales",
            "point".toUpperCase(Locale.getDefault()).equals("POINT")
        );
        if (client().performRequest(new Request("HEAD", "/ogc")).getStatusLine().getStatusCode() == 404) {
            GeoDataLoader.loadOGCDatasetIntoEs(client(), "ogc");
        }
        if (client().performRequest(new Request("HEAD", "/geo")).getStatusLine().getStatusCode() == 404) {
            GeoDataLoader.loadGeoDatasetIntoEs(client(), "geo");
        }
    }

    private static class SqlSpecParser implements Parser {
        @Override
        public Object parse(String line) {
            return line.endsWith(";") ? line.substring(0, line.length() - 1) : line;
        }
    }

    public GeoSqlSpecTestCase(String fileName, String groupName, String testName, Integer lineNumber, String query) {
        super(fileName, groupName, testName, lineNumber);
        this.query = query;
    }

    @Override
    protected final void doTest() throws Throwable {
        try (Connection h2 = H2.get(); Connection es = esJdbc()) {

            ResultSet expected, elasticResults;
            expected = executeJdbcQuery(h2, query);
            elasticResults = executeJdbcQuery(es, query);

            assertResults(expected, elasticResults);
        }
    }

    // TODO: use UTC for now until deciding on a strategy for handling date extraction
    @Override
    protected Properties connectionProperties() {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(JdbcConfiguration.TIME_ZONE, "UTC");
        return connectionProperties;
    }
}
