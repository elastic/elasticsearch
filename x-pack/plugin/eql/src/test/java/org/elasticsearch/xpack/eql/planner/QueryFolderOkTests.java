/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.planner;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;

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

    public void test() {
        PhysicalPlan p = plan(query);
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertEquals(0, eqe.output().size());

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
