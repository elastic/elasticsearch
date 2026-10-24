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

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/**
 * The right side of a {@code JOIN (subquery)} is materialized on the coordinator and capped by
 * {@code esql.intermediate_local_relation_max_size}. An oversized right side must fail as soon as the collected pages
 * pass the cap, not after the subquery has read its whole input.
 * <p>
 * The subquery reads the pausable {@code pause_me} field, so every document needs a permit. The test releases only
 * enough permits for the first page, with a cap smaller than one page. The query must then fail while the rest of the
 * index is still unread. Without the early check the query would block until every document had been read.
 */
public class SubqueryJoinResultLimitIT extends AbstractPausableIntegTestCase {

    @Before
    public void requireJoinOnSubquery() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
    }

    @Before
    public void resetPausePermits() {
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
    }

    @After
    public void resetLimit() {
        updateClusterSettings(Settings.builder().putNull(PlannerSettings.INTERMEDIATE_LOCAL_RELATION_MAX_SIZE.getKey()));
    }

    public void testOversizedSubqueryFailsBeforeReadingAllInput() throws Exception {
        // Any non-empty page is larger than this, so the first page from the subquery crosses the cap.
        updateClusterSettings(Settings.builder().put(PlannerSettings.INTERMEDIATE_LOCAL_RELATION_MAX_SIZE.getKey(), "1b"));

        // pause_me is only filtered on, not kept, so each document evaluates it, and takes a permit, exactly once.
        String query = """
            FROM test
            | KEEP foo
            | LEFT OUTER JOIN (
                FROM test
                | WHERE pause_me IS NOT NULL
                | EVAL bar = foo
                | KEEP foo, bar
              ) ON foo
            """;
        var pragmas = new QueryPragmas(Settings.builder().put("data_partitioning", "shard").put("page_size", pageSize()).build());
        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query).pragmas(pragmas));
        try {
            // Let exactly one page of the subquery through.
            scriptPermits.release(pageSize());

            // The coordinator fails the query on that first page and cancels it, while the reader is still blocked on the
            // next document. Without the early check nothing is cancelled until all numberOfDocs() documents are read.
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks();
                assertTrue("expected the query task to be cancelled on failure: " + tasks, tasks.stream().anyMatch(TaskInfo::cancelled));
            }, 30, TimeUnit.SECONDS);
            assertThat(scriptWaits.availablePermits(), lessThan(numberOfDocs()));
        } finally {
            // Unblock the remaining script waits so the cancelled drivers can finish.
            scriptPermits.release(numberOfDocs() * 10);
        }

        Exception e = expectThrows(Exception.class, () -> future.actionGet(60, TimeUnit.SECONDS));
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertThat(cause.getMessage(), containsString("sub-plan execution results too large"));
        assertThat(cause.getMessage(), containsString(PlannerSettings.INTERMEDIATE_LOCAL_RELATION_MAX_SIZE.getKey()));
    }
}
