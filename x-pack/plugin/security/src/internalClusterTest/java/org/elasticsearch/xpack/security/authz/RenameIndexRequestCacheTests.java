/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.junit.After;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

/**
 * The request cache of a shard serves the result of one search to another only if the keys of the two requests agree, and for a user
 * with document or field level security the key has to say so. A request that still carries the name an index had before a rename must
 * get the same key as one that carries the name it has now.
 */
public class RenameIndexRequestCacheTests extends SecurityIntegTestCase {

    @After
    public void cleanupSecurityIndex() {
        super.deleteSecurityIndex();
    }

    @Override
    protected String configUsers() {
        final String hashed = SecuritySettingsSource.TEST_PASSWORD_HASHED;
        return super.configUsers() + "rc_plain_user:" + hashed + "\n" + "rc_dls_user:" + hashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "rc_plain_role:rc_plain_user\n" + "rc_dls_role:rc_dls_user\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + """

            rc_plain_role:
              indices:
                - names: 'rc*'
                  privileges: [ read ]
            rc_dls_role:
              indices:
                - names: 'rc*'
                  privileges: [ read ]
                  query: '{"term": {"visible": true}}'
            """;
    }

    public void testRequestCacheDoesNotServeAnotherUsersResultForARequestUnderTheOldName() throws Exception {
        createIndex("rc-old", 1, 0, false);
        indexDoc("rc-old", "1", "visible", true);
        indexDoc("rc-old", "2", "visible", false);
        indexDoc("rc-old", "3", "visible", false);
        refresh("rc-old");
        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("rc-old", "rc-alias"));
        final Index original = clusterService().state().metadata().getProject().index("rc-old").getIndex();
        assertTrue(
            client().execute(TransportIndicesAliasesAction.TYPE, renameIndexRequest("rc-old", "rc-new")).actionGet().isAcknowledged()
        );

        // what a coordinator that has not yet applied the rename sends for a search through an alias: the name of the index before
        // it in the shard id, and the alias, which the data node resolves to the name the index has now
        final ShardId staleShardId = new ShardId(original, 0);

        assertThat("an unrestricted user sees every document", totalHitsOfShardSearch("rc_plain_user", staleShardId), equalTo(3L));
        assertThat(
            "the result cached for them must not be served to a user whose role restricts the documents",
            totalHitsOfShardSearch("rc_dls_user", staleShardId),
            equalTo(1L)
        );
    }

    private long totalHitsOfShardSearch(String user, ShardId shardId) throws Exception {
        final String nodeId = clusterService().state().routingTable().index("rc-new").shard(0).primaryShard().currentNodeId();
        final String nodeName = clusterService().state().nodes().get(nodeId).getName();
        final TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
        final SearchTransportService searchTransportService = internalCluster().getInstance(SearchTransportService.class, nodeName);

        final SearchRequest searchRequest = new SearchRequest("rc-alias").requestCache(true)
            .allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().size(0).trackTotalHits(true).query(QueryBuilders.matchAllQuery()));
        final ShardSearchRequest request = new ShardSearchRequest(
            new OriginalIndices(new String[] { "rc-alias" }, searchRequest.indicesOptions()),
            searchRequest,
            shardId,
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            System.currentTimeMillis(),
            null
        );
        final SearchTask task = (SearchTask) transportService.getTaskManager()
            .register("transport", TransportSearchAction.TYPE.name(), searchRequest);
        final ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        final PlainActionFuture<Long> future = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
            // a node only sends a message on behalf of an authenticated user, which this records in the context
            final PlainActionFuture<Authentication> authenticated = new PlainActionFuture<>();
            internalCluster().getInstance(AuthenticationService.class, nodeName)
                .authenticate(request.getClass().getName(), request, false, authenticated);
            authenticated.actionGet().writeToContext(threadContext);
            searchTransportService.sendExecuteQuery(
                transportService.getConnection(transportService.getLocalNode()),
                request,
                task,
                // the connection is to this node, so the result is the server's own object, which it releases once the listener returns
                ActionListener.wrap(result -> future.onResponse(result.queryResult().getTotalHits().value()), future::onFailure),
                bytes -> {},
                bytes -> {}
            );
        }
        try {
            return future.actionGet();
        } finally {
            transportService.getTaskManager().unregister(task);
        }
    }
}
