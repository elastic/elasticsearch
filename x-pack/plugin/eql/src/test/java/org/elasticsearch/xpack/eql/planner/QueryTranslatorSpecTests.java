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
import org.elasticsearch.xpack.ql.TestUtils;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class QueryTranslatorSpecTests extends AbstractQueryTranslatorTestCase {

    private final String filename;
    private final String name;
    private final String query;
    private final List<Matcher<String>> matchers;

    public QueryTranslatorSpecTests(String filename, String name, String query, List<Matcher<String>> matchers) {
        this.filename = filename;
        this.name = name;
        this.query = query;
        this.matchers = matchers;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = "%2$s")
    public static Iterable<Object[]> parameters() throws Exception {
        return TestUtils.readSpec(QueryTranslatorSpecTests.class, "/querytranslator_tests.txt");
    }

    public void test() {
        PhysicalPlan p = plan(query);
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertEquals(0, eqe.output().size());

        final String queryWithoutWhitespace = eqe.queryContainer().toString().replaceAll("\\s+", "");

        // test query term
        matchers.forEach(m -> assertThat(queryWithoutWhitespace, m));

        // test common term
        assertThat(queryWithoutWhitespace, containsString("\"term\":{\"event.category\":{\"value\":\"process\""));

        // test field source extraction
        assertThat(queryWithoutWhitespace, not(containsString("\"_source\":{\"includes\":[],\"excludes\":[]")));
    }
}
