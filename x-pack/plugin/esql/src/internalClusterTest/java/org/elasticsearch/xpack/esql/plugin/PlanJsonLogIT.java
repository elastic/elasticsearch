/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.plan.PlanJsonLog;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Checks the stage labels of the JSON plans logged on {@link PlanJsonLog#LOGGER_NAME}.
 * A query with {@code IN} subqueries runs in several phases, and each phase's physical plans must say
 * which phase they belong to, and where its subquery was written, so a log visualizer can tell them apart.
 */
@TestLogging(value = PlanJsonLog.LOGGER_NAME + ":DEBUG", reason = "asserts on the JSON plan debug logging")
public class PlanJsonLogIT extends AbstractEsqlIntegTestCase {

    public void testSinglePhaseQueryHasNoPhaseLabels() {
        createIndices();
        try (var mockLog = MockLog.capture(PlanJsonLog.LOGGER_NAME)) {
            mockLog.addExpectation(seen("physical", "JSON plan [physical]:*"));
            mockLog.addExpectation(seen("optimized_physical", "JSON plan [optimized_physical]:*"));
            mockLog.addExpectation(unseen("phase line", "JSON plan phase *"));
            mockLog.addExpectation(unseen("labelled physical", "JSON plan [physical:*"));
            run(syncEsqlQueryRequest("FROM emps | KEEP id")).close();
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testSubqueryPhasesAreLabelled() {
        createIndices();
        try (var view = createView("emps_in_view", "FROM emps | WHERE lang IN (FROM langs | WHERE name == \"English\" | KEEP code)")) {
            try (var mockLog = MockLog.capture(PlanJsonLog.LOGGER_NAME)) {
                // Subplans run innermost first, so the subquery written inside the view is the first phase
                mockLog.addExpectation(
                    seen(
                        "view subplan phase",
                        "JSON plan phase [subplan-0]: kind [semi_join], view [emps_in_view], location [1:*], source [lang IN (FROM langs *]"
                    )
                );
                mockLog.addExpectation(seen("view subplan logical", "JSON plan [logical:subplan-0]:*"));
                mockLog.addExpectation(seen("view subplan physical", "JSON plan [physical:subplan-0]:*"));
                mockLog.addExpectation(seen("view subplan optimized physical", "JSON plan [optimized_physical:subplan-0]:*"));
                mockLog.addExpectation(
                    seen(
                        "IN subplan phase",
                        "JSON plan phase [subplan-*]: kind [semi_join], view [], location [1:*], "
                            + "source [id IN (FROM emps_in_view | KEEP id)]"
                    )
                );
                mockLog.addExpectation(
                    seen(
                        "NOT IN subplan phase",
                        "JSON plan phase [subplan-*]: kind [anti_join], view [], location [1:*], source [id NOT IN (FROM emps *]"
                    )
                );
                mockLog.addExpectation(seen("main phase", "JSON plan phase [main]: kind [main], view [], location [], source []"));
                mockLog.addExpectation(seen("main logical", "JSON plan [logical:main]:*"));
                mockLog.addExpectation(seen("main physical", "JSON plan [physical:main]:*"));
                mockLog.addExpectation(seen("main optimized physical", "JSON plan [optimized_physical:main]:*"));
                // The view body is logged once it has been resolved
                mockLog.addExpectation(seen("resolved view", "JSON plan [resolved:emps_in_view]:*"));
                // The local execution plan, whose operators name the physical plan node they were built from
                mockLog.addExpectation(seen("data compute plan", "JSON plan [compute_plan:data]:*"));
                mockLog.addExpectation(seen("final compute plan", "JSON plan [compute_plan:final]:*"));
                mockLog.addExpectation(seen("data drivers", "JSON plan [drivers:data]:*\"plan_node_id\"*"));
                mockLog.addExpectation(seen("final drivers", "JSON plan [drivers:final]:*\"plan_node_id\"*"));
                mockLog.addExpectation(unseen("unlabelled physical", "JSON plan [physical]:*"));
                mockLog.addExpectation(unseen("unlabelled optimized physical", "JSON plan [optimized_physical]:*"));
                run(
                    syncEsqlQueryRequest(
                        "FROM emps | WHERE id IN (FROM emps_in_view | KEEP id) AND id NOT IN (FROM emps | WHERE id > 2 | KEEP id) | KEEP id"
                    )
                ).close();
                mockLog.assertAllExpectationsMatched();
            }
        }
    }

    private void createIndices() {
        assertAcked(client().admin().indices().prepareCreate("emps").setMapping("id", "type=integer", "lang", "type=integer"));
        assertAcked(client().admin().indices().prepareCreate("langs").setMapping("code", "type=integer", "name", "type=keyword"));
        indexRandom(
            true,
            prepareIndex("emps").setSource(Map.of("id", 1, "lang", 1)),
            prepareIndex("emps").setSource(Map.of("id", 2, "lang", 2)),
            prepareIndex("emps").setSource(Map.of("id", 3, "lang", 1)),
            prepareIndex("langs").setSource(Map.of("code", 1, "name", "English")),
            prepareIndex("langs").setSource(Map.of("code", 2, "name", "French"))
        );
    }

    private static MockLog.SeenEventExpectation seen(String name, String message) {
        return new MockLog.SeenEventExpectation(name, PlanJsonLog.LOGGER_NAME, Level.DEBUG, message);
    }

    private static MockLog.UnseenEventExpectation unseen(String name, String message) {
        return new MockLog.UnseenEventExpectation(name, PlanJsonLog.LOGGER_NAME, Level.DEBUG, message);
    }

    private Releasable createView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query, null, false))
            )
        );
        return () -> assertAcked(
            client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name }, false)
            )
        );
    }
}
