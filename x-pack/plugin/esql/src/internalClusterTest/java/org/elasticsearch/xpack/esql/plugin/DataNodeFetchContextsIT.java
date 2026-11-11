/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * A data node request that asks for fetch contexts opens a registered single-session reader context per shard, so the
 * fetch phase can find it again after the response. Nothing on the coordinator asks for them yet, so the test adds the
 * keep-alive to the requests the coordinator sends. Their plans send no {@code _doc} off the node, so no row reads a
 * context after the cut, and the node frees every one of them once it answered.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataNodeFetchContextsIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "fetch-contexts";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testOpensAContextPerShardAndFreesThemWhenNoRowLeavesTheNode() throws Exception {
        String dataNode = internalCluster().startNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        int shards = between(1, 5);
        assertAcked(prepareCreate(INDEX).setSettings(indexSettings(shards, 0)).setMapping("v", "type=long"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        int docs = between(20, 100);
        for (int i = 0; i < docs; i++) {
            bulk.add(prepareIndex(INDEX).setSource("v", i));
        }
        assertNoFailures(bulk.get());

        List<ReaderContext> registered = new CopyOnWriteArrayList<>();
        MockSearchService searchService = (MockSearchService) internalCluster().getInstance(SearchService.class, dataNode);
        searchService.setOnPutContext(registered::add);
        AtomicInteger asked = askForFetchContexts(coordinator, TimeValue.timeValueMinutes(between(1, 5)));

        try (
            EsqlQueryResponse response = client(coordinator).execute(
                EsqlQueryAction.INSTANCE,
                syncEsqlQueryRequest("FROM " + INDEX + " | SORT v DESC | LIMIT 3 | KEEP v")
            ).actionGet()
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(docs - 1L), List.of(docs - 2L), List.of(docs - 3L))));
        } finally {
            searchService.setOnPutContext(context -> {});
        }

        assertThat("the coordinator sent its data node requests with fetch contexts", asked.get(), equalTo(1));
        assertThat(registered, hasSize(shards));
        assertThat(registered.stream().map(ReaderContext::singleSession).toList(), everyItem(equalTo(true)));
        assertBusy(() -> assertThat(searchService.getActiveContexts(), equalTo(0)));
    }

    /**
     * Opening the search context of a shard can fail with an {@link IOException}. It fails only that shard, and the contexts
     * the batch opened before it close, with or without fetch contexts.
     */
    public void testAShardWhoseContextFailsToOpenWithAnIOExceptionLeaksNothing() throws Exception {
        String dataNode = internalCluster().startNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        int shards = between(2, 5);
        assertAcked(prepareCreate(INDEX).setSettings(indexSettings(shards, 0)).setMapping("v", "type=long"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 100; i++) {
            bulk.add(prepareIndex(INDEX).setSource("v", i));
        }
        assertNoFailures(bulk.get());
        if (randomBoolean()) {
            askForFetchContexts(coordinator, TimeValue.timeValueMinutes(1));
        }

        MockSearchService searchService = (MockSearchService) internalCluster().getInstance(SearchService.class, dataNode);
        AtomicInteger created = new AtomicInteger();
        // the last shard of the batch fails, after the others opened their contexts
        searchService.setOnCreateSearchContext(context -> {
            if (created.incrementAndGet() == shards) {
                throwUnchecked(new IOException("simulated failure to open a search context"));
            }
        });
        try (
            EsqlQueryResponse response = client(coordinator).execute(
                EsqlQueryAction.INSTANCE,
                syncEsqlQueryRequest("FROM " + INDEX + " | STATS c = COUNT(*)").allowPartialResults(true)
            ).actionGet()
        ) {
            assertTrue("one shard failed", response.isPartial());
        } finally {
            searchService.setOnCreateSearchContext(context -> {});
        }

        assertBusy(() -> assertThat(searchService.getActiveContexts(), equalTo(0)));
    }

    /**
     * Throws {@code e} from code that can't declare it, like the hook of {@link MockSearchService}. The methods that call
     * the hook declare an {@link IOException}, so it reaches their callers as one.
     */
    @SuppressWarnings("unchecked")
    private static <E extends Exception> void throwUnchecked(Exception e) throws E {
        throw (E) e;
    }

    /**
     * Adds {@code keepAlive} to every data node request {@code coordinator} sends.
     *
     * @return counts the requests it changed
     */
    private static AtomicInteger askForFetchContexts(String coordinator, TimeValue keepAlive) {
        AtomicInteger asked = new AtomicInteger();
        MockTransportService transport = MockTransportService.getInstance(coordinator);
        transport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(ComputeService.DATA_ACTION_NAME) && request instanceof DataNodeRequest dataNodeRequest) {
                asked.incrementAndGet();
                request = withFetchContexts(dataNodeRequest, keepAlive);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        return asked;
    }

    private static DataNodeRequest withFetchContexts(DataNodeRequest in, TimeValue keepAlive) {
        DataNodeRequest request = new DataNodeRequest(
            in.sessionId(),
            in.queryId(),
            in.configuration(),
            in.clusterAlias(),
            in.shards(),
            in.aliasFilters(),
            in.plan(),
            in.indices(),
            in.indicesOptions(),
            in.runNodeLevelReduction(),
            in.reductionLateMaterialization(),
            in.singleNodeOptimizations(),
            in.externalSplits(),
            in.requiredClusterStateVersion(),
            keepAlive
        );
        request.setParentTask(in.getParentTask());
        return request;
    }
}
