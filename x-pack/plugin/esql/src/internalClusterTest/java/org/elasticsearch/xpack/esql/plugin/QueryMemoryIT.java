/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.AllocatingQueryPlugin;
import org.elasticsearch.search.AllocatingQueryPlugin.AllocatingQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The memory an ES|QL query allocates on a node is its query's there.
 */
public class QueryMemoryIT extends AbstractEsqlIntegTestCase {

    private static final long BYTES = ByteSizeValue.ofKb(64).getBytes();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), AllocatingQueryPlugin.class);
    }

    /**
     * The search context of a data-node request charges the query the known allocation of its filter.
     */
    public void testDataNodeSearchContextChargesTheQuery() {
        assertAcked(prepareCreate("test").setSettings(indexSettings(1, 0)).setMapping("f", "type=long"));
        prepareIndex("test").setSource("f", 1).get();
        indicesAdmin().prepareRefresh("test").get();

        EsqlQueryRequest request = syncEsqlQueryRequest("FROM test | STATS c = COUNT(*)");
        request.filter(new AllocatingQueryBuilder(BYTES));
        long bytes;
        ActionFuture<EsqlQueryResponse> response;
        try (var pause = AllocatingQueryPlugin.pause()) {
            response = client().execute(EsqlQueryAction.INSTANCE, request);
            pause.awaitReached();
            bytes = memoryBytes(queryId());
        }
        try (EsqlQueryResponse result = response.actionGet()) {
            assertThat(getValuesList(result), equalTo(List.of(List.of(1L))));
        }
        assertThat(bytes, greaterThanOrEqualTo(BYTES));
    }

    /**
     * The id of the running query, its task on the coordinator.
     */
    private static TaskId queryId() {
        List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks();
        assertThat(tasks, hasSize(1));
        return tasks.get(0).taskId();
    }

    private static long memoryBytes(TaskId queryId) {
        long bytes = 0;
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            bytes += searchService.queryScheduler().memoryBytes(queryId);
        }
        return bytes;
    }
}
