/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.multi_cluster_with_security;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.qa.jdbc.CsvSpecTestCase;
import org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.CsvTestCase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;
import static org.elasticsearch.xpack.ql.TestUtils.classpathResources;
import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.specParser;

public class JdbcCsvSpecIT extends CsvSpecTestCase {

    public static final String REMOTE_CLUSTER_NAME = "my_remote_cluster"; // gradle defined
    public static final String EXTRACT_FN_NAME = "EXTRACT";

    private static final Pattern DESCRIBE_OR_SHOW = Pattern.compile("(?i)\\s*(DESCRIBE|SHOW).*");
    private static final Pattern FROM_QUALIFIED = Pattern.compile(".*(?i)FROM\\s+[^\\s:]+:[^\\s:]+\\s.*");

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        List<Object[]> list = new ArrayList<>();
        list.addAll(CsvSpecTestCase.readScriptSpec());
        list.addAll(readScriptSpec(classpathResources("/multi-cluster-with-security/*.csv-spec"), specParser()));
        return list;
    }

    public JdbcCsvSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(
            fileName,
            groupName,
            testName,
            lineNumber,
            randomBoolean() && isFromQualified(testCase.query) == false ? qualifyFromClause(testCase) : testCase
        );
    }

    // qualify the query FROM clause with the cluster name, but (crudely) skip `EXTRACT(a FROM b)` calls.
    private static CsvTestCase qualifyFromClause(CsvTestCase testCase) {
        String query = testCase.query;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < query.length();) {
            int j = query.substring(i).toUpperCase(Locale.ROOT).indexOf(EXTRACT_FN_NAME);
            j = j >= 0 ? i + j : query.length();
            sb.append(
                query.substring(i, j)
                    .replaceAll("(?i)(FROM)(\\s+)(\\w+|\"[^\"]+\")", "$1$2" + buildRemoteIndexName(REMOTE_CLUSTER_NAME, "$3"))
            );
            boolean inString = false, escaping = false;
            char stringDelim = 0, crrChar;
            for (int k = query.indexOf('(', j + EXTRACT_FN_NAME.length()); k >= 0 && k < query.length(); k++) {
                switch (crrChar = query.charAt(k)) {
                    case ')':
                        if (inString == false) {
                            sb.append(query, j, k + 1);
                            j = k + 1;
                            k = query.length();
                        }
                        break;
                    case '"':
                    case '\'':
                        if (escaping == false) {
                            if (inString) {
                                inString = stringDelim != crrChar;
                            } else {
                                stringDelim = crrChar;
                                inString = true;
                            }
                        }
                        break;
                    case '\\':
                        if (inString) {
                            escaping = escaping ? false : true; // !escaping
                        }
                        break;
                    default:
                        escaping = escaping ? false : escaping;
                }
            }
            i = j;
        }

        testCase.query = sb.toString();
        return testCase;
    }

    @Override
    public Connection esJdbc() throws SQLException {
        Connection connection = esJdbc(connectionProperties());
        // Only set the default catalog if the query index isn't yet qualified with the catalog, which can happen if query has been written
        // qualified from the start (for the documentation) or edited in qualifyFromClause() above.
        if (isFromQualified(csvTestCase().query) == false) {
            connection.setCatalog(REMOTE_CLUSTER_NAME);
        }
        return connection;
    }

    @Override
    public boolean isEnabled() {
        return super.isEnabled() &&
        // skip single-cluster tests that'd need a CLUSTER clause to work in multi-cluster mode.
            (DESCRIBE_OR_SHOW.matcher(csvTestCase().query).matches() == false || fileName.startsWith("multi-cluster"));
    }

    @Override
    protected int fetchSize() {
        // using a smaller fetchSize for nested documents' tests to uncover bugs
        // similar to https://github.com/elastic/elasticsearch/issues/35176 quicker
        return fileName.startsWith("nested") && randomBoolean() ? randomIntBetween(1, 5) : super.fetchSize();
    }

    // Simple check if the FROM clause of a query contains a cluster-qualified index (pattern).
    // Note: it won't work reliably with multiple clauses (subselects) or queries embedded in strings.
    private static boolean isFromQualified(String query) {
        return FROM_QUALIFIED.matcher(query).matches();
    }
}
