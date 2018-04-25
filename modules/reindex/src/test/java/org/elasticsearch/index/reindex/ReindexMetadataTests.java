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

import org.elasticsearch.index.reindex.ScrollableHitSource.Hit;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;

/**
 * Index-by-search test for ttl, timestamp, and routing.
 */
public class ReindexMetadataTests extends AbstractAsyncBulkByScrollActionMetadataTestCase<ReindexRequest, BulkByScrollResponse> {
    public void testRoutingCopiedByDefault() throws Exception {
        IndexRequest index = new IndexRequest();
        action().copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals("foo", index.routing());
    }

    public void testRoutingCopiedIfRequested() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("keep");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals("foo", index.routing());
    }

    public void testRoutingDiscardedIfRequested() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("discard");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals(null, index.routing());
    }

    public void testRoutingSetIfRequested() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("=cat");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals("cat", index.routing());
    }

    public void testRoutingSetIfWithDegenerateValue() throws Exception {
        TestAction action = action();
        action.mainRequest().getDestination().routing("==]");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkByScrollAction.wrap(index), doc().setRouting("foo"));
        assertEquals("=]", index.routing());
    }

    @Override
    protected TestAction action() {
        return new TestAction();
    }

    @Override
    protected ReindexRequest request() {
        return new ReindexRequest(new SearchRequest(), new IndexRequest());
    }

    private class TestAction extends TransportReindexAction.AsyncIndexBySearchAction {
        TestAction() {
            super(ReindexMetadataTests.this.task, ReindexMetadataTests.this.logger, null, ReindexMetadataTests.this.threadPool, request(),
                    null, null, listener(), Settings.EMPTY);
        }

        public ReindexRequest mainRequest() {
            return this.mainRequest;
        }

        @Override
        public AbstractAsyncBulkByScrollAction.RequestWrapper<?> copyMetadata(AbstractAsyncBulkByScrollAction.RequestWrapper<?> request,
                Hit doc) {
            return super.copyMetadata(request, doc);
        }
    }
}
