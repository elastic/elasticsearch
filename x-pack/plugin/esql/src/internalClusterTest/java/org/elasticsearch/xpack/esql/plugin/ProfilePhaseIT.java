/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * A query that runs in several phases lists the drivers and plans of all of them in one PROFILE response. They are labelled with
 * their phase, so a client does not have to guess where one phase ends and the next one begins.
 */
public class ProfilePhaseIT extends AbstractEsqlIntegTestCase {

    public void testQueryWithOnePhaseHasNoPhases() {
        createIndex();
        EsqlQueryResponse.Profile profile = profile("FROM emps | SORT id | LIMIT 3");
        assertThat(profile.drivers(), not(empty()));
        for (DriverProfile driver : profile.drivers()) {
            assertThat(driver.phase(), nullValue());
            assertThat(driver.phaseKind(), nullValue());
        }
        for (PlanProfile plan : profile.plans()) {
            assertThat(plan.phase(), nullValue());
        }
    }

    /** An {@code INLINE STATS} and an {@code IN} subquery each run before the query that uses them. */
    public void testPhasesAreLabelledInTheOrderTheyRan() {
        createIndex();
        EsqlQueryResponse.Profile profile = profile("""
            FROM emps
            | INLINE STATS c = COUNT(*) BY lang
            | WHERE id IN (FROM emps | WHERE id < 5 | KEEP id)
            | INLINE STATS total = COUNT(*)
            | KEEP id, c, total
            """);
        List<String> phases = new ArrayList<>();
        for (DriverProfile driver : profile.drivers()) {
            assertThat("driver [" + driver.description() + "] has a phase", driver.phase(), not(nullValue()));
            assertThat("driver [" + driver.description() + "] says why its phase ran", driver.phaseKind(), not(nullValue()));
            if (phases.contains(driver.phase()) == false) {
                phases.add(driver.phase());
            }
        }
        for (PlanProfile plan : profile.plans()) {
            assertThat("plan [" + plan.description() + "] has a phase", plan.phase(), not(nullValue()));
        }
        // The drivers of a phase finish before the next one starts
        assertThat(phases, contains("subplan-0", "subplan-1", "subplan-2", "main"));
        assertThat(
            profile.drivers().stream().filter(d -> d.phase().equals("subplan-1")).map(DriverProfile::phaseKind).distinct().toList(),
            equalTo(List.of("semi_join"))
        );
        assertThat(
            profile.drivers().stream().filter(d -> d.phase().equals("main")).map(DriverProfile::phaseKind).distinct().toList(),
            equalTo(List.of("main"))
        );
        assertThat(profile.drivers().stream().map(DriverProfile::phaseKind).toList(), hasItem("inline_join"));
    }

    private void createIndex() {
        assertAcked(client().admin().indices().prepareCreate("emps").setMapping("id", "type=integer", "lang", "type=integer"));
        for (int i = 1; i <= 10; i++) {
            prepareIndex("emps").setSource(Map.of("id", i, "lang", i % 3)).get();
        }
        client().admin().indices().prepareRefresh("emps").get();
    }

    private EsqlQueryResponse.Profile profile(String query) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(query);
        request.profile(true);
        try (EsqlQueryResponse response = run(request)) {
            return response.profile();
        }
    }
}
