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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.containsString;

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
        ArrayList<Object[]> arr = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                QueryFolderOkTests.class.getResourceAsStream("/queryfolder_tests.txt"), StandardCharsets.UTF_8))) {
            String line;
            String name = null;
            String query = null;
            ArrayList<Object> expectations = null;
            int newLineCount = 0;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("//")) {
                    continue;
                }

                line = line.trim();
                if (Strings.isEmpty(line)) {
                    if (name != null) {
                        newLineCount++;
                    }
                    if (newLineCount >= 2) {
                        // Add and zero out for the next spec
                        addSpec(arr, name, query, expectations == null ? null : expectations.toArray());
                        name = null;
                        query = null;
                        expectations = null;
                        newLineCount = 0;
                    }
                    continue;
                }

                if (name == null) {
                    name = line;
                    continue;
                }

                if (query == null) {
                    query = line;
                    continue;
                }

                if (line.equals("null") == false) {  // special case for no expectations
                    if (expectations == null) {
                        expectations = new ArrayList<>();
                    }
                    expectations.add(line);
                }
            }
            addSpec(arr, name, query, expectations.toArray());
        }
        return arr;
    }

    private static void addSpec(ArrayList<Object[]> arr, String name, String query, Object[] expectations) {
        if ((Strings.isNullOrEmpty(name) == false) && (Strings.isNullOrEmpty(query) == false)) {
            arr.add(new Object[]{name, query, expectations});
        }
    }

    public void test() {
        PhysicalPlan p = plan(query);
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertEquals(25, eqe.output().size());
        assertEquals(KEYWORD, eqe.output().get(0).dataType());

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
        assertThat(query, containsString("\"_source\":{\"includes\":[],\"excludes\":[]"));
    }
}
