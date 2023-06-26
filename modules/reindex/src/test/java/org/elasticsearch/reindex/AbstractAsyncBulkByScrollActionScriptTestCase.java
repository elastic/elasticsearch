/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.reindex.AbstractAsyncBulkByScrollActionTestCase;
import org.elasticsearch.index.reindex.AbstractBulkIndexByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.reindex.AbstractAsyncBulkByScrollAction.RequestWrapper;
import org.elasticsearch.script.ReindexScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.UpdateByQueryScript;
import org.elasticsearch.script.UpdateScript;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractAsyncBulkByScrollActionScriptTestCase<
    Request extends AbstractBulkIndexByScrollRequest<Request>,
    Response extends BulkByScrollResponse> extends AbstractAsyncBulkByScrollActionTestCase<Request, Response> {

    protected ScriptService scriptService;

    @Before
    public void setupScriptService() {
        scriptService = mock(ScriptService.class);
    }

    @SuppressWarnings("unchecked")
    protected <T extends ActionRequest> T applyScript(Consumer<Map<String, Object>> scriptBody) {
        IndexRequest index = new IndexRequest("index").id("1").source(singletonMap("foo", "bar"));
        ScrollableHitSource.Hit doc = new ScrollableHitSource.BasicHit("test", "id", 0);
        when(scriptService.compile(any(), eq(UpdateScript.CONTEXT))).thenReturn(
            (params, ctx) -> new UpdateScript(Collections.emptyMap(), ctx) {
                @Override
                public void execute() {
                    scriptBody.accept(ctx);
                }
            }
        );
        when(scriptService.compile(any(), eq(UpdateByQueryScript.CONTEXT))).thenReturn(
            (params, ctx) -> new UpdateByQueryScript(Collections.emptyMap(), ctx) {
                @Override
                public void execute() {
                    scriptBody.accept(getCtx());
                }
            }
        );
        when(scriptService.compile(any(), eq(ReindexScript.CONTEXT))).thenReturn(
            (params, ctx) -> new ReindexScript(Collections.emptyMap(), ctx) {
                @Override
                public void execute() {
                    scriptBody.accept(getCtx());
                }
            }
        );
        AbstractAsyncBulkByScrollAction<Request, ?> action = action(scriptService, request().setScript(mockScript("")));
        RequestWrapper<?> result = action.buildScriptApplier().apply(AbstractAsyncBulkByScrollAction.wrap(index), doc);
        return (result != null) ? (T) result.self() : null;
    }

    public void testScriptAddingJunkToCtxIsError() {
        try {
            applyScript((Map<String, Object> ctx) -> ctx.put("junk", "junk"));
            fail("Expected error");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Cannot put key [junk] with value [junk] into ctx"));
        }
    }

    public void testChangeSource() {
        IndexRequest index = applyScript((Map<String, Object> ctx) -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
            source.put("bar", "cat");
        });
        assertEquals("cat", index.sourceAsMap().get("bar"));
    }

    public void testSetOpTypeDelete() throws Exception {
        DeleteRequest delete = applyScript((Map<String, Object> ctx) -> ctx.put("op", "delete"));
        assertThat(delete.index(), equalTo("index"));
        assertThat(delete.id(), equalTo("1"));
    }

    public void testSetOpTypeUnknown() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyScript((Map<String, Object> ctx) -> ctx.put("op", "unknown"))
        );
        assertThat(e.getMessage(), equalTo("[op] must be one of delete, index, noop, not [unknown]"));
    }

    protected abstract AbstractAsyncBulkByScrollAction<Request, ?> action(ScriptService scriptService, Request request);
}
