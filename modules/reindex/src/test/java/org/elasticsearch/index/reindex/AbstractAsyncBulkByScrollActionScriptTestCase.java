/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.reindex.AbstractAsyncBulkByScrollAction.OpType;
import org.elasticsearch.index.reindex.AbstractAsyncBulkByScrollAction.RequestWrapper;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.UpdateScript;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractAsyncBulkByScrollActionScriptTestCase<
                Request extends AbstractBulkIndexByScrollRequest<Request>,
                Response extends BulkByScrollResponse>
        extends AbstractAsyncBulkByScrollActionTestCase<Request, Response> {

    protected ScriptService scriptService;

    @Before
    public void setupScriptService() {
        scriptService = mock(ScriptService.class);
    }

    @SuppressWarnings("unchecked")
    protected <T extends ActionRequest> T applyScript(Consumer<Map<String, Object>> scriptBody) {
        IndexRequest index = new IndexRequest("index").id("1").source(singletonMap("foo", "bar"));
        ScrollableHitSource.Hit doc = new ScrollableHitSource.BasicHit("test", "id", 0);
        UpdateScript.Factory factory = (params, ctx) -> new UpdateScript(Collections.emptyMap(), ctx) {
            @Override
            public void execute() {
                scriptBody.accept(getCtx());
            }
        };
        when(scriptService.compile(any(), eq(UpdateScript.CONTEXT))).thenReturn(factory);
        AbstractAsyncBulkByScrollAction<Request, ?> action = action(scriptService, request().setScript(mockScript("")));
        RequestWrapper<?> result = action.buildScriptApplier().apply(AbstractAsyncBulkByScrollAction.wrap(index), doc);
        return (result != null) ? (T) result.self() : null;
    }

    public void testScriptAddingJunkToCtxIsError() {
        try {
            applyScript((Map<String, Object> ctx) -> ctx.put("junk", "junk"));
            fail("Expected error");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Invalid fields added to context [junk]"));
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
        DeleteRequest delete = applyScript((Map<String, Object> ctx) -> ctx.put("op", OpType.DELETE.toString()));
        assertThat(delete.index(), equalTo("index"));
        assertThat(delete.id(), equalTo("1"));
    }

    public void testSetOpTypeUnknown() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> applyScript((Map<String, Object> ctx) -> ctx.put("op", "unknown")));
        assertThat(e.getMessage(), equalTo("Operation type [unknown] not allowed, only [noop, index, delete] are allowed"));
    }

    protected abstract AbstractAsyncBulkByScrollAction<Request, ?> action(ScriptService scriptService, Request request);
}
