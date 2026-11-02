/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/** The aggregation of a query is pushed to a satellite of version 1 that declares it, and only then. */
public class PushAggregateToSatelliteTests extends ESTestCase {

    private static final String PATH = "flight://localhost:1/employees";

    private static Map<String, Object> capsV1() {
        Map<String, Object> caps = new HashMap<>(PushFragmentToSatelliteTests.CAPS);
        caps.put("specVersions", List.of(0, 1));
        caps.put("plan", List.of("scan", "filter", "project", "eval", "topn", "limit", "aggregate"));
        caps.put("aggregates", List.of("count", "sum", "min", "max"));
        caps.put("totalRecords", 1000L);
        return caps;
    }

    private static PhysicalPlan plan(String query, Map<String, Object> caps) throws IOException {
        assumeTrue("EXTERNAL command", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        return PushFragmentToSatelliteTests.localPlan("EXTERNAL \"" + PATH + "\" | " + query, caps, QueryPragmas.EMPTY, Map.of());
    }

    private static String fragmentOf(PhysicalPlan plan) throws IOException {
        List<ExternalSourceExec> leaves = plan.collect(ExternalSourceExec.class);
        assertEquals(1, leaves.size());
        return leaves.get(0).pushedFilter() instanceof SatelliteFragment f ? PushFragmentToSatelliteTests.json(f.request()) : "no fragment";
    }

    private static boolean aggregateIsPushed(PhysicalPlan plan) {
        return plan.collect(AggregateExec.class).stream().noneMatch(a -> a.getMode() == AggregatorMode.INITIAL);
    }

    public void testGroupedAggregatesAreOneAggregateNodeAndTheInitialAggregationIsReplacedByAnAdapter() throws IOException {
        PhysicalPlan plan = plan(
            "STATS c = COUNT(*), s = SUM(salary), mn = MIN(emp_no), mx = MAX(first_name) BY languages | LIMIT 5",
            capsV1()
        );
        assertTrue(aggregateIsPushed(plan));
        String f = fragmentOf(plan);
        assertThat(f, containsString("\"specVersion\":1"));
        assertThat(
            f,
            containsString(
                "\"plan\":{\"op\":\"aggregate\",\"keys\":[{\"name\":\"languages\",\"from\":\"languages\"}],\"aggregates\":["
                    + "{\"name\":\"$$agg$0\",\"fn\":\"count\"},{\"name\":\"$$agg$1\",\"fn\":\"sum\",\"from\":\"salary\"},"
                    + "{\"name\":\"$$agg$2\",\"fn\":\"min\",\"from\":\"emp_no\"},"
                    + "{\"name\":\"$$agg$3\",\"fn\":\"max\",\"from\":\"first_name\"}]"
            )
        );
        assertThat(f, containsString("\"input\":{\"op\":\"scan\""));
        // the satellite's plain values reach the final aggregation as the partial state it expects: a project over an eval
        assertFalse(plan.collect(ProjectExec.class).isEmpty());
        assertFalse(plan.collect(EvalExec.class).isEmpty());
    }

    public void testUngroupedCountScansTheCheapestColumn() throws IOException {
        String f = fragmentOf(plan("STATS c = COUNT(*) | LIMIT 5", capsV1()));
        assertThat(f, containsString("\"keys\":[],\"aggregates\":[{\"name\":\"$$agg$0\",\"fn\":\"count\"}]"));
        assertThat(f, containsString("\"op\":\"scan\",\"target\":\"employees\",\"columns\":[{\"name\":\"emp_no\",\"type\":\"integer\"}]"));
    }

    public void testFilterAndEvalBelowTheAggregateAreInTheSameFragment() throws IOException {
        String f = fragmentOf(
            plan("WHERE salary > 50000 | EVAL d = salary * 2 | STATS s = SUM(d), n = COUNT(salary) BY languages | LIMIT 5", capsV1())
        );
        assertThat(f, containsString("\"op\":\"aggregate\""));
        assertThat(f, containsString("\"fn\":\"sum\",\"from\":\"d\""));
        assertThat(f, containsString("\"op\":\"eval\""));
        assertThat(f, containsString("\"op\":\"filter\""));
    }

    /** The aggregators keep a datetime in a long channel, a boolean and a keyword in their own: each is adapted from the plain value. */
    public void testMinAndMaxOfEveryTypeAreAdapted() throws IOException {
        for (String query : List.of(
            "STATS m = MAX(hire_date), n = MIN(hire_date) | LIMIT 5",
            "STATS m = MIN(still_hired), n = MAX(still_hired) | LIMIT 5",
            "STATS m = MIN(first_name), n = MAX(first_name) BY languages | LIMIT 5",
            "STATS m = MIN(emp_no), n = MAX(emp_no) | LIMIT 5"
        )) {
            PhysicalPlan plan = plan(query, capsV1());
            assertTrue(query, aggregateIsPushed(plan));
            assertThat(query, fragmentOf(plan), containsString("\"op\":\"aggregate\""));
        }
    }

    public void testAvgOfAnIntegerIsPushedAsASumAndACount() throws IOException {
        PhysicalPlan plan = plan("STATS a = AVG(salary) | LIMIT 5", capsV1());
        assertTrue(aggregateIsPushed(plan));
        String f = fragmentOf(plan);
        assertThat(f, containsString("\"fn\":\"sum\",\"from\":\"salary\""));
        assertThat(f, containsString("\"fn\":\"count\",\"from\":\"salary\""));
    }

    public void testSumIsPushedOnlyWhenTheRecordCountProvesItCannotOverflow() throws IOException {
        for (Object records : new Object[] { -1L, 0L, 4_294_967_296L }) {
            Map<String, Object> caps = capsV1();
            caps.put("totalRecords", records);
            PhysicalPlan plan = plan("STATS s = SUM(salary) | LIMIT 5", caps);
            assertFalse("records " + records, aggregateIsPushed(plan));
            assertThat(fragmentOf(plan), not(containsString("\"op\":\"aggregate\"")));
        }
        Map<String, Object> caps = capsV1();
        caps.put("totalRecords", 4_294_967_295L);
        assertTrue(aggregateIsPushed(plan("STATS s = SUM(salary) | LIMIT 5", caps)));
        // the others do not depend on it
        Map<String, Object> unknown = capsV1();
        unknown.put("totalRecords", -1L);
        assertTrue(aggregateIsPushed(plan("STATS c = COUNT(*), mn = MIN(salary) BY languages | LIMIT 5", unknown)));
    }

    public void testWhatVersionOneDoesNotPushStaysInEs() throws IOException {
        for (String query : List.of(
            "STATS c = COUNT(1) | LIMIT 5",
            "STATS m = MIN(height) | LIMIT 5",
            "STATS c = COUNT(*) BY height | LIMIT 5",
            "STATS c = COUNT_DISTINCT(languages) | LIMIT 5",
            "STATS a = COUNT(*) WHERE salary > 3, b = COUNT(*) WHERE salary > 9 | LIMIT 5",
            "LIMIT 100 | STATS c = COUNT(*) | LIMIT 5"
        )) {
            PhysicalPlan plan = plan(query, capsV1());
            assertFalse(
                query,
                plan.collect(ExternalSourceExec.class)
                    .stream()
                    .anyMatch(
                        e -> e.pushedFilter() instanceof SatelliteFragment f
                            && f.request().get("plan") instanceof Map<?, ?> p
                            && "aggregate".equals(p.get("op"))
                    )
            );
        }
    }

    public void testAnUndeclaredFunctionOrTheOperatorIsNotPushed() throws IOException {
        Map<String, Object> noMax = capsV1();
        noMax.put("aggregates", List.of("count", "sum", "min"));
        assertFalse(aggregateIsPushed(plan("STATS m = MAX(salary) | LIMIT 5", noMax)));
        Map<String, Object> noOperator = capsV1();
        noOperator.put("plan", List.of("scan", "filter", "project", "eval", "topn", "limit"));
        assertFalse(aggregateIsPushed(plan("STATS c = COUNT(*) | LIMIT 5", noOperator)));
    }

    public void testASatelliteOfVersionZeroGetsNoAggregate() throws IOException {
        PhysicalPlan plan = plan("STATS c = COUNT(*), s = SUM(salary) BY languages | LIMIT 5", PushFragmentToSatelliteTests.CAPS);
        assertFalse(aggregateIsPushed(plan));
        assertThat(fragmentOf(plan), not(containsString("aggregate")));
        assertThat(fragmentOf(plan), containsString("\"specVersion\":0"));
    }

    public void testPushdownOffSendsNoAggregate() throws IOException {
        assumeTrue("EXTERNAL command", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        QueryPragmas off = new QueryPragmas(org.elasticsearch.common.settings.Settings.builder().put("satellite_pushdown", false).build());
        PhysicalPlan plan = PushFragmentToSatelliteTests.localPlan(
            "EXTERNAL \"" + PATH + "\" | STATS c = COUNT(*) | LIMIT 5",
            capsV1(),
            off,
            Map.of()
        );
        assertFalse(aggregateIsPushed(plan));
        assertThat(plan.collect(ExternalSourceExec.class).getFirst().pushedFilter(), org.hamcrest.Matchers.nullValue());
    }

    public void testTheAdaptedSourceIsTheOnlySatelliteLeaf() throws IOException {
        PhysicalPlan plan = plan("STATS c = COUNT(*) BY languages | LIMIT 5", capsV1());
        assertThat(plan.collect(ExternalSourceExec.class).getFirst().pushedFilter(), instanceOf(SatelliteFragment.class));
    }
}
