/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.matchesPattern;

/**
 * The result of a sub-plan ({@code IN} subquery, {@code INLINE STATS}) becomes a local relation of the main plan, capped by
 * {@code esql.intermediate_local_relation_max_size}. An oversized result must fail as soon as the pages collected on the
 * coordinator pass the cap, not after the sub-plan has read its whole input.
 */
public class SubPlanResultLimitIT extends AbstractPausableIntegTestCase {
    /**
     * Only the collected size varies: it is the size of the first page, which depends on the random page size and data.
     */
    private static final String TOO_LARGE_AT_1B = "sub-plan execution results too large \\[at least [\\d.]+[kmg]?b\\] > 1b, "
        + "the limit set by \\[esql\\.intermediate_local_relation_max_size\\]";

    @Before
    public void resetPausePermits() {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
    }

    @After
    public void resetLimit() {
        updateClusterSettings(Settings.builder().putNull(PlannerSettings.INTERMEDIATE_LOCAL_RELATION_MAX_SIZE.getKey()));
    }

    /**
     * The subquery reads the pausable {@code pause_me} field, so every document needs a permit. The test releases only enough
     * permits for the first page, with a cap smaller than one page. The query must then fail and cancel while the rest of the
     * index is still unread. Without the early check it would block until every document had been read.
     */
    public void testOversizedInSubqueryFailsBeforeReadingAllInput() throws Exception {
        setLimitBelowOnePage();
        String query = """
            FROM test
            | WHERE foo IN (FROM test | WHERE pause_me IS NOT NULL | KEEP foo)
            """;
        var pragmas = new QueryPragmas(Settings.builder().put("data_partitioning", "shard").put("page_size", pageSize()).build());
        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query).pragmas(pragmas));
        try {
            scriptPermits.release(pageSize());
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks();
                assertTrue("expected the query task to be cancelled on failure: " + tasks, tasks.stream().anyMatch(TaskInfo::cancelled));
            }, 30, TimeUnit.SECONDS);
            assertThat(scriptWaits.availablePermits(), lessThan(numberOfDocs()));
        } finally {
            scriptPermits.release(numberOfDocs() * 10);
        }
        Throwable cause = ExceptionsHelper.unwrapCause(expectThrows(Exception.class, () -> future.actionGet(60, TimeUnit.SECONDS)));
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertThat(cause.getMessage(), matchesPattern(TOO_LARGE_AT_1B));
    }

    /**
     * {@code INLINE STATS} ends in an aggregation, which reads its whole input before it emits a page, so the early check
     * only saves collecting the rest of the result. The message tells it apart from the check on the complete result.
     */
    public void testOversizedInlineStatsFailsWhileCollecting() {
        setLimitBelowOnePage();
        Throwable cause = ExceptionsHelper.unwrapCause(
            expectThrows(Exception.class, () -> run("FROM test | INLINE STATS c = COUNT(*) BY foo"))
        );
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertThat(cause.getMessage(), matchesPattern(TOO_LARGE_AT_1B));
    }

    public void testLimitDoesNotApplyToMainPlan() {
        setLimitBelowOnePage();
        try (EsqlQueryResponse response = run("FROM test | KEEP foo | LIMIT 10000")) {
            assertThat(getValuesList(response), hasSize(numberOfDocs()));
        }
    }

    private void setLimitBelowOnePage() {
        updateClusterSettings(Settings.builder().put(PlannerSettings.INTERMEDIATE_LOCAL_RELATION_MAX_SIZE.getKey(), "1b"));
    }
}
