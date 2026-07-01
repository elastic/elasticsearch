/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.ScriptService;
import org.mockito.Mockito;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests reindex with a script modifying the documents.
 */
public class ReindexScriptTests extends AbstractAsyncBulkByPaginatedSearchActionScriptTestCase<
    ReindexRequest,
    BulkByPaginatedSearchResponse> {

    public void testSetIndex() throws Exception {
        Object dest = randomFrom(new Object[] { 234, 234L, "pancake" });
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_index", dest));
        assertEquals(dest.toString(), index.index());
    }

    public void testSettingIndexToNullIsError() throws Exception {
        try {
            applyScript((Map<String, Object> ctx) -> ctx.put("_index", null));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("_index cannot be null"));
        }
    }

    public void testSetId() throws Exception {
        Object id = randomFrom(new Object[] { null, 234, 234L, "pancake" });
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_id", id));
        if (id == null) {
            assertNull(index.id());
        } else {
            assertEquals(id.toString(), index.id());
        }
    }

    public void testSetVersion() throws Exception {
        Number version = randomFrom(new Number[] { null, 234, 234L });
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_version", version));
        if (version == null) {
            assertEquals(Versions.MATCH_ANY, index.version());
        } else {
            assertEquals(version.longValue(), index.version());
        }
    }

    public void testSettingVersionToJunkIsAnError() throws Exception {
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> applyScript((Map<String, Object> ctx) -> ctx.put("_version", "junk"))
        );
        assertEquals(err.getMessage(), "_version [junk] is wrong type, expected assignable to [java.lang.Number], not [java.lang.String]");

        err = expectThrows(IllegalArgumentException.class, () -> applyScript((Map<String, Object> ctx) -> ctx.put("_version", Math.PI)));
        assertEquals(
            err.getMessage(),
            "_version may only be set to an int or a long but was [3.141592653589793] with type [java.lang.Double]"
        );
    }

    public void testSetRouting() throws Exception {
        String routing = randomRealisticUnicodeOfLengthBetween(5, 20);
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_routing", routing));
        assertEquals(routing, index.routing());
        assertFalse(index.isRoutingFromSlice());
    }

    public void testSetRoutingKeepsSliceAliasReadableButNotProvenance() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String routing = randomRealisticUnicodeOfLengthBetween(5, 20);
        IndexRequest index = applyScript((Map<String, Object> ctx) -> {
            ctx.put("_routing", routing);
            assertEquals(routing, ctx.get(SliceIndexing.PARAM_NAME));
            assertTrue(ctx.containsKey(SliceIndexing.PARAM_NAME));
        });
        assertEquals(routing, index.routing());
        assertFalse(index.isRoutingFromSlice());
    }

    public void testSetRoutingSameValueClearsSliceProvenance() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String routing = randomRealisticUnicodeOfLengthBetween(5, 20);
        ReindexRequest request = request();
        IndexRequest index = new IndexRequest("index").id("1").source(Map.of("foo", "bar")).routing(routing).setRoutingFromSlice(true);
        PaginatedHitSource.Hit doc = new PaginatedHitSource.BasicHit("test", "id", 0).setRouting(routing);

        IndexRequest result = applyScriptToRequest((Map<String, Object> ctx) -> ctx.put("_routing", routing), request, index, doc);
        assertEquals(routing, result.routing());
        assertFalse(result.isRoutingFromSlice());
    }

    public void testSetRoutingFromSourceCanOverridePrepopulatedRequestRouting() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String sourceRouting = randomRealisticUnicodeOfLengthBetween(5, 20);
        String prepopulatedRouting = randomValueOtherThan(sourceRouting, () -> randomRealisticUnicodeOfLengthBetween(5, 20));
        ReindexRequest request = request();
        IndexRequest index = new IndexRequest("index").id("1")
            .source(Map.of("foo", "bar"))
            .routing(prepopulatedRouting)
            .setRoutingFromSlice(true);
        PaginatedHitSource.Hit doc = new PaginatedHitSource.BasicHit("test", "id", 0).setRouting(sourceRouting);

        IndexRequest result = applyScriptToRequest((Map<String, Object> ctx) -> ctx.put("_routing", sourceRouting), request, index, doc);
        assertEquals(sourceRouting, result.routing());
        assertFalse(result.isRoutingFromSlice());
    }

    public void testSetSlice() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String slice = randomRealisticUnicodeOfLengthBetween(5, 20);
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put(SliceIndexing.PARAM_NAME, slice));
        assertEquals(slice, index.routing());
        assertTrue(index.isRoutingFromSlice());
    }

    public void testSetSliceRejectedWhenFeatureFlagDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyScript((Map<String, Object> ctx) -> ctx.put(SliceIndexing.PARAM_NAME, "slice1"))
        );
        assertThat(e.getMessage(), containsString(SliceIndexing.PARAM_NAME + " cannot be updated"));
    }

    @Override
    protected ReindexRequest request() {
        ReindexRequest request = new ReindexRequest();
        request.getDestination().index("test");
        return request;
    }

    @Override
    protected Reindexer.AsyncIndexBySearchAction action(ScriptService scriptService, ReindexRequest request) {
        ReindexSslConfig sslConfig = Mockito.mock(ReindexSslConfig.class);
        return new Reindexer.AsyncIndexBySearchAction(
            task,
            logger,
            null,
            null,
            threadPool,
            scriptService,
            ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID),
            sslConfig,
            request,
            listener(),
            randomBoolean() ? null : Version.CURRENT,
            randomPositiveTimeValue(),
            null,
            new ReindexSettings(),
            new NoopCircuitBreaker("test")
        );
    }

    @SuppressWarnings("unchecked")
    private <T extends ActionRequest> T applyScriptToRequest(
        java.util.function.Consumer<Map<String, Object>> scriptBody,
        ReindexRequest request,
        IndexRequest index,
        PaginatedHitSource.Hit doc
    ) {
        Mockito.when(
            scriptService.compile(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq(org.elasticsearch.script.UpdateScript.CONTEXT)
            )
        ).thenReturn((params, ctx) -> new org.elasticsearch.script.UpdateScript(java.util.Collections.emptyMap(), ctx) {
            @Override
            public void execute() {
                scriptBody.accept(ctx);
            }
        });
        Mockito.when(
            scriptService.compile(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq(org.elasticsearch.script.UpdateByQueryScript.CONTEXT)
            )
        ).thenReturn((params, ctx) -> new org.elasticsearch.script.UpdateByQueryScript(java.util.Collections.emptyMap(), ctx) {
            @Override
            public void execute() {
                scriptBody.accept(getCtx());
            }
        });
        Mockito.when(
            scriptService.compile(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq(org.elasticsearch.script.ReindexScript.CONTEXT)
            )
        ).thenReturn((params, ctx) -> new org.elasticsearch.script.ReindexScript(java.util.Collections.emptyMap(), ctx) {
            @Override
            public void execute() {
                scriptBody.accept(getCtx());
            }
        });

        AbstractAsyncBulkByPaginatedSearchAction<ReindexRequest, ?> action = action(scriptService, request.setScript(mockScript("")));
        AbstractAsyncBulkByPaginatedSearchAction.RequestWrapper<?> wrapped = action.buildScriptApplier()
            .apply(AbstractAsyncBulkByPaginatedSearchAction.wrap(index), doc);
        return (wrapped != null) ? (T) wrapped.self() : null;
    }
}
