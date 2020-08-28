/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.junit.Assume;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class QueryFolderOkTests extends AbstractQueryFolderTestCase {

    private final String name;
    private final String query;
    private final Object expect;

    public QueryFolderOkTests(String name, String query, Object expect) {
        this.name = name;
        this.query = query;
        this.expect = expect;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() throws Exception {
        return QueriesUtils.readSpec("/queryfolder_tests.txt");
    }

    public static Iterable<Object[]> readSpec(String url) throws Exception {
        ArrayList<Object[]> arr = new ArrayList<>();
        Map<String, Integer> testNames = new LinkedHashMap<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                QueryFolderOkTests.class.getResourceAsStream(url), StandardCharsets.UTF_8))) {
            int lineNumber = 0;
            String line;
            String name = null;
            String query = null;
            ArrayList<Object> expectations = new ArrayList<>(8);

            StringBuilder sb = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();

                if (line.isEmpty() || line.startsWith("//")) {
                    continue;
                }

                if (name == null) {
                    name = line;
                    Integer previousName = testNames.put(name, lineNumber);
                    if (previousName != null) {
                        throw new IllegalArgumentException("Duplicate test name '" + line + "' at line " + lineNumber
                                + " (previously seen at line " + previousName + ")");
                    }
                } else if (query == null) {
                    sb.append(line);
                    if (line.endsWith(";")) {
                        sb.setLength(sb.length() - 1);
                        query = sb.toString();
                        sb.setLength(0);
                    }
                } else {
                    boolean done = false;
                    if (line.endsWith(";")) {
                        line = line.substring(0, line.length() - 1);
                        done = true;
                    }
                    // no expectation
                    if (line.equals("null") == false) {
                        expectations.add(line);
                    }
                    if (done) {
                        // Add and zero out for the next spec
                        addSpec(arr, name, query, expectations.isEmpty() ? null : expectations.toArray());
                        name = null;
                        query = null;
                        expectations.clear();
                    }
                }
            }

            if (name != null) {
                throw new IllegalStateException("Read a test [" + name + "] without a body at the end of [" + url + "]");
            }
        }
        return arr;
    }

    private static void addSpec(ArrayList<Object[]> arr, String name, String query, Object[] expectations) {
        if ((Strings.isNullOrEmpty(name) == false) && (Strings.isNullOrEmpty(query) == false)) {
            arr.add(new Object[]{name, query, expectations});
        }
    }

    public void test() {
        String testName = name.toLowerCase(Locale.ROOT);
        // skip tests that do not make sense from case sensitivity point of view
        boolean isCaseSensitiveValidTest = testName.endsWith("sensitive") == false
            || testName.endsWith("-casesensitive") && configuration.isCaseSensitive()
            || testName.endsWith("-caseinsensitive") && configuration.isCaseSensitive() == false;
        Assume.assumeTrue(isCaseSensitiveValidTest);

        PhysicalPlan p = plan(query);
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertEquals(1, eqe.output().size());

        final String query = eqe.queryContainer().toString().replaceAll("\\s+", "");

        // test query term
        if (expect != null) {
            if (expect instanceof Object[]) {
                for (Object item : (Object[]) expect) {
                    assertThat(query, containsString((String) item));
                }
            } else {
                assertThat(query, containsString((String) expect));
            }
        }

        // test common term
        assertThat(query, containsString("\"term\":{\"event.category\":{\"value\":\"process\""));

        // test field source extraction
        assertThat(query, not(containsString("\"_source\":{\"includes\":[],\"excludes\":[]")));
    }
}
