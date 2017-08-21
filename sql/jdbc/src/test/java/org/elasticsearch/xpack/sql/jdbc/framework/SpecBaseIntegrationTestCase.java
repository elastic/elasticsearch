/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.lang.String.format;

/**
 * Tests that compare the Elasticsearch JDBC client to some other JDBC client
 * after loading a specific set of test data.
 */
public abstract class SpecBaseIntegrationTestCase extends JdbcIntegrationTestCase {
    protected static final String PARAM_FORMATTING = "%0$s.test%2$s";

    protected final String groupName;
    protected final String testName;
    protected final Integer lineNumber;
    protected final Path source;

    public SpecBaseIntegrationTestCase(String groupName, String testName, Integer lineNumber, Path source) {
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
        this.source = source;
    }

    @Before
    public void setupTestDataIfNeeded() throws Exception {
        if (client().performRequest("HEAD", "/test_emp").getStatusLine().getStatusCode() == 404) {
            DataLoader.loadDatasetIntoEs(client());
        }
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @AfterClass
    public static void wipeTestData() throws IOException {
        if (false == EMBED_SQL) {
            try {
                adminClient().performRequest("DELETE", "/*");
            } catch (ResponseException e) {
                // 404 here just means we had no indexes
                if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                    throw e;
                }
            }
        }
    }

    public final void test() throws Throwable {
        try {
            doTest();
        } catch (AssertionError ae) {
            throw reworkException(ae);
        }
    }

    /**
     * Implementations should pay attention on using {@link #executeJdbcQuery(Connection, String)} (typically for ES connections)
     * and {@link #assertResults(ResultSet, ResultSet)} which takes into account logging/debugging results (through {@link #logEsResultSet()}.
     * 
     * @throws Throwable
     */
    protected abstract void doTest() throws Throwable;

    protected ResultSet executeJdbcQuery(Connection con, String query) throws SQLException {
        Statement statement = con.createStatement();
        //statement.setFetchSize(randomInt(10));
        // NOCOMMIT: hook up pagination
        statement.setFetchSize(1000);
        return statement.executeQuery(query);
    }

    protected boolean logEsResultSet() {
        return false;
    }

    protected void assertResults(ResultSet expected, ResultSet elastic) throws SQLException {
        Logger log = logEsResultSet() ? logger : null;
        JdbcAssert.assertResultSets(expected, elastic, log);
    }

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), groupName + ".test" + testName,
                source.getFileName().toString(), lineNumber);

        th.setStackTrace(redone);
        return th;
    }

    //
    // spec reader
    //

    // returns groupName, testName, its line location, its source and the custom object (based on each test parser)
    protected static List<Object[]> readScriptSpec(String url, Parser parser) throws Exception {
        Path source = PathUtils.get(SpecBaseIntegrationTestCase.class.getResource(url).toURI());
        String fileName = source.getFileName().toString();
        int dot = fileName.indexOf(".");
        String groupName = dot > 0 ? fileName.substring(0, dot) : fileName;

        List<String> lines = Files.readAllLines(source);

        Map<String, Integer> testNames = new LinkedHashMap<>();
        List<Object[]> pairs = new ArrayList<>();

        String testName = null;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            // ignore comments
            if (!line.isEmpty() && !line.startsWith("//")) {
                // parse test name
                if (testName == null) {
                    if (testNames.keySet().contains(line)) {
                        throw new IllegalStateException(format(Locale.ROOT,
                                "Duplicate test name '%s' at line %d (previously seen at line %d)", line, i, testNames.get(line)));
                    }
                    else {
                        testName = Strings.capitalize(line);
                        testNames.put(testName, Integer.valueOf(i));
                    }
                }
                else {
                    Object result = parser.parse(line);
                    // only if the parser is ready, add the object - otherwise keep on serving it lines
                    if (result != null) {
                        pairs.add(new Object[] { groupName, testName, Integer.valueOf(i), source, result });
                        testName = null;
                    }
                }
            }
        }
        assertNull("Cannot find spec for test " + testName, testName);

        return pairs;
    }

    public interface Parser {
        Object parse(String line);
    }
}