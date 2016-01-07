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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;

public class AsyncBulkIndexByScrollActionTest extends ESTestCase {
    private ThreadPool threadPool;
    private DummyBulkIndexByScrollRequest mainRequest;
    private PlainActionFuture<BulkIndexByScrollResponse> listener;

    @Before
    public void setupForTest() {
        threadPool = new ThreadPool(getTestName());
        mainRequest = new DummyBulkIndexByScrollRequest();
        listener = new PlainActionFuture<>();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testScriptAddingJunkToCtxIsError() {
        IndexRequest index = new IndexRequest("index", "type", "1").source(singletonMap("foo", "bar"));
        Map<String, SearchHitField> fields = new HashMap<>();
        InternalSearchHit doc = new InternalSearchHit(0, "id", new StringText("type"), fields);
        doc.shardTarget(new SearchShardTarget("nodeid", "index", 1));
        ExecutableScript script = new SimpleExecuteableScript((Map<String, Object> ctx) -> ctx.put("junk", "junk"));
        try {
            new DummyAsyncBulkIndexByScrollAction().applyScript(index, doc, script, new HashMap<>());
            fail("Expected error");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Invalid fields added to ctx [junk]"));
        }
    }

    private class DummyAsyncBulkIndexByScrollAction
            extends AbstractAsyncBulkIndexByScrollAction<DummyBulkIndexByScrollRequest, BulkIndexByScrollResponse> {

        public DummyAsyncBulkIndexByScrollAction() {
            super(logger, null, null, threadPool, AsyncBulkIndexByScrollActionTest.this.mainRequest, null, listener);
        }

        @Override
        protected IndexRequest buildIndexRequest(SearchHit doc) {
            return null;
        }

        @Override
        protected void scriptChangedIndex(IndexRequest index, Object to) {
        }

        @Override
        protected void scriptChangedType(IndexRequest index, Object to) {
        }

        @Override
        protected void scriptChangedId(IndexRequest index, Object to) {
        }

        @Override
        protected void scriptChangedVersion(IndexRequest index, Object to) {
        }

        @Override
        protected void scriptChangedRouting(IndexRequest index, Object to) {
        }

        @Override
        protected void scriptChangedParent(IndexRequest index, Object to) {
        }

        @Override
        protected void scriptChangedTimestamp(IndexRequest index, Object to) {
        }

        @Override
        protected void scriptChangedTTL(IndexRequest index, Object to) {
        }

        @Override
        protected BulkIndexByScrollResponse buildResponse(long took) {
            return null;
        }
    }

    private static class DummyBulkIndexByScrollRequest extends AbstractBulkIndexByScrollRequest<DummyBulkIndexByScrollRequest> {
        @Override
        protected DummyBulkIndexByScrollRequest self() {
            return null;
        }
    }

    private static class SimpleExecuteableScript implements ExecutableScript {
        private final Consumer<Map<String, Object>> script;
        private Map<String, Object> ctx;

        public SimpleExecuteableScript(Consumer<Map<String, Object>> script) {
            this.script = script;
        }

        @Override
        public Object run() {
            script.accept(ctx);
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void setNextVar(String name, Object value) {
            if ("ctx".equals(name)) {
                ctx = (Map<String, Object>) value;
            } else {
                throw new IllegalArgumentException("Unsupported var [" + name + "]");
            }
        }

        @Override
        public Object unwrap(Object value) {
            return value;
        }
    }
}
