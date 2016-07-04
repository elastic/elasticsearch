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
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.reindex.AbstractAsyncBulkIndexByScrollAction.OpType;
import org.elasticsearch.index.reindex.AbstractAsyncBulkIndexByScrollAction.RequestWrapper;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.junit.Before;
import org.mockito.Matchers;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractAsyncBulkIndexByScrollActionScriptTestCase<
                Request extends AbstractBulkIndexByScrollRequest<Request>,
                Response extends BulkIndexByScrollResponse>
        extends AbstractAsyncBulkIndexByScrollActionTestCase<Request, Response> {

    private static final Script EMPTY_SCRIPT = new Script("");

    protected ScriptService scriptService;

    @Before
    public void setupScriptService() {
        scriptService = mock(ScriptService.class);
    }

    @SuppressWarnings("unchecked")
    protected <T extends ActionRequest<?>> T applyScript(Consumer<Map<String, Object>> scriptBody) {
        IndexRequest index = new IndexRequest("index", "type", "1").source(singletonMap("foo", "bar"));
        Map<String, SearchHitField> fields = new HashMap<>();
        InternalSearchHit doc = new InternalSearchHit(0, "id", new Text("type"), fields);
        doc.shardTarget(new SearchShardTarget("nodeid", new Index("index", "uuid"), 1));
        ExecutableScript executableScript = new SimpleExecutableScript(scriptBody);

        when(scriptService.executable(any(CompiledScript.class), Matchers.<Map<String, Object>>any()))
                .thenReturn(executableScript);
        AbstractAsyncBulkIndexByScrollAction<Request> action = action(scriptService, request().setScript(EMPTY_SCRIPT));
        RequestWrapper<?> result = action.buildScriptApplier().apply(AbstractAsyncBulkIndexByScrollAction.wrap(index), doc);
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

    public void testSetOpTypeNoop() throws Exception {
        assertThat(task.getStatus().getNoops(), equalTo(0L));
        assertNull(applyScript((Map<String, Object> ctx) -> ctx.put("op", OpType.NOOP.toString())));
        assertThat(task.getStatus().getNoops(), equalTo(1L));
    }

    public void testSetOpTypeDelete() throws Exception {
        DeleteRequest delete = applyScript((Map<String, Object> ctx) -> ctx.put("op", OpType.DELETE.toString()));
        assertThat(delete.index(), equalTo("index"));
        assertThat(delete.type(), equalTo("type"));
        assertThat(delete.id(), equalTo("1"));
    }

    public void testSetOpTypeUnknown() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> applyScript((Map<String, Object> ctx) -> ctx.put("op", "unknown")));
        assertThat(e.getMessage(), equalTo("Operation type [unknown] not allowed, only [noop, index, delete] are allowed"));
    }

    protected abstract AbstractAsyncBulkIndexByScrollAction<Request> action(ScriptService scriptService, Request request);
}
