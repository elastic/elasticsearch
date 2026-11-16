/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Integration tests documenting the current behaviour of {@code _search/template} and
 * {@code _msearch/template} against a shard being restored from a snapshot.
 *
 * <p>Both actions delegate to {@code TransportSearchAction} after rendering the Mustache template.
 * When the target shard is INITIALIZING, each search parks in {@code SearchReadyGate} —
 * identical to a plain {@code _search} request. The request hangs indefinitely until the
 * repository is unblocked and the shard becomes search-ready.
 *
 * <p>Setup: a single-shard, no-replica index is deleted before restore so blob reads are required,
 * which is what {@code blockAllDataNodes} actually blocks.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class RestoringShardSearchTemplateIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, MustachePlugin.class);
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndexWithContent(INDEX);
        ensureGreen(INDEX);
        createRepository(REPO, "mock");
        createFullSnapshot(REPO, SNAPSHOT);
        assertAcked(indicesAdmin().prepareDelete(INDEX));
    }

    // -------------------------------------------------------------------------
    // _search/template — parks like _search (delegates to TransportSearchAction)
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_search/template} renders the Mustache template and then delegates to
     * {@code TransportSearchAction}. The rendered search goes through
     * {@code SearchService.rewriteAndFetchShardRequest}, which parks in {@code SearchReadyGate}
     * when the shard is INITIALIZING. The request hangs rather than returning 503 immediately.
     */
    public void testSearchTemplateWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(INDEX));
        request.setScriptType(ScriptType.INLINE);
        request.setScript("{\"query\": {\"match_all\": {}}}");
        request.setScriptParams(Map.of());
        var future = client().execute(MustachePlugin.SEARCH_TEMPLATE_ACTION, request);
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // _msearch/template — parks like _msearch (delegates to TransportSearchAction per sub-request)
    // -------------------------------------------------------------------------

    /**
     * {@code POST /_msearch/template} fans out each rendered template search to
     * {@code TransportSearchAction}. Each sub-search parks in {@code SearchReadyGate} when the
     * target shard is INITIALIZING. The outer msearch-template future also hangs.
     */
    public void testMsearchTemplateWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        SearchTemplateRequest templateRequest = new SearchTemplateRequest();
        templateRequest.setRequest(new SearchRequest(INDEX));
        templateRequest.setScriptType(ScriptType.INLINE);
        templateRequest.setScript("{\"query\": {\"match_all\": {}}}");
        templateRequest.setScriptParams(Map.of());
        MultiSearchTemplateRequest msearchRequest = new MultiSearchTemplateRequest().add(templateRequest);
        var future = client().execute(MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION, msearchRequest);
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }
}
