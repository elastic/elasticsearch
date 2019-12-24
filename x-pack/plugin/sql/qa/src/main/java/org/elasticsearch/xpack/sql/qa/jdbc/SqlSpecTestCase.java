/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.junit.Assume;
import org.junit.ClassRule;

import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Tests comparing sql queries executed against our jdbc client
 * with those executed against H2's jdbc client.
 */
public abstract class SqlSpecTestCase extends SpecBaseIntegrationTestCase {
    private String query;

    @ClassRule
    public static LocalH2 H2 = new LocalH2((c) -> {
        c.createStatement().execute("RUNSCRIPT FROM 'classpath:/setup_test_emp.sql'");
    });

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = JdbcTestUtils.classpathResources("/*.sql-spec");
        assertTrue("Not enough specs found " + urls.toString(), urls.size() > 9);
        return readScriptSpec(urls, specParser());
    }

    private static class SqlSpecParser implements Parser {
        private final StringBuilder query = new StringBuilder();

        @Override
        public Object parse(String line) {
            // not initialized
            String q = null;
            if (line.endsWith(";")) {
                query.append(line.substring(0, line.length() - 1));
                q = query.toString();
                query.setLength(0);
            } else {
                query.append(line);
                query.append("\r\n");
            }

            return q;
        }
    }

    static SqlSpecParser specParser() {
        return new SqlSpecParser();
    }

    public SqlSpecTestCase(String fileName, String groupName, String testName, Integer lineNumber, String query) {
        super(fileName, groupName, testName, lineNumber);
        this.query = query;
    }

    @Override
    protected int fetchSize() {
        // using a smaller fetchSize for nested documents' tests to uncover bugs
        // similar to https://github.com/elastic/elasticsearch/issues/42581
        return randomIntBetween(1, 20);
    }

    @Override
    protected final void doTest() throws Throwable {
        // we skip the tests in case of these locales because ES-SQL is Locale-insensitive for now
        // while H2 does take the Locale into consideration
        String[] h2IncompatibleLocales = new String[] {"tr", "az", "tr-TR", "tr-CY", "az-Latn", "az-Cyrl", "az-Latn-AZ", "az-Cyrl-AZ"};
        boolean goodLocale = !Arrays.stream(h2IncompatibleLocales)
                .anyMatch((l) -> Locale.getDefault().equals(new Locale.Builder().setLanguageTag(l).build()));
        if (fileName.startsWith("case-functions")) {
            Assume.assumeTrue(goodLocale);
        }
        
        try (Connection h2 = H2.get();
             Connection es = esJdbc()) {

            ResultSet expected, elasticResults;
            expected = executeJdbcQuery(h2, query);
            elasticResults = executeJdbcQuery(es, query);

            assertResults(expected, elasticResults);
        }
    }
}
