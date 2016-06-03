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

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;

/**
 * Index-by-search test for ttl, timestamp, and routing.
 */
public class ReindexMetadataTests extends AbstractAsyncBulkIndexbyScrollActionMetadataTestCase<ReindexRequest, BulkIndexByScrollResponse> {
    public void testRoutingCopiedByDefault() throws Exception {
        IndexRequest index = new IndexRequest();
        action().copyMetadata(AbstractAsyncBulkIndexByScrollAction.wrap(index), doc(RoutingFieldMapper.NAME, "foo"));
        assertEquals("foo", index.routing());
    }

    public void testRoutingCopiedIfRequested() throws Exception {
        TransportReindexAction.AsyncIndexBySearchAction action = action();
        action.mainRequest.getDestination().routing("keep");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkIndexByScrollAction.wrap(index), doc(RoutingFieldMapper.NAME, "foo"));
        assertEquals("foo", index.routing());
    }

    public void testRoutingDiscardedIfRequested() throws Exception {
        TransportReindexAction.AsyncIndexBySearchAction action = action();
        action.mainRequest.getDestination().routing("discard");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkIndexByScrollAction.wrap(index), doc(RoutingFieldMapper.NAME, "foo"));
        assertEquals(null, index.routing());
    }

    public void testRoutingSetIfRequested() throws Exception {
        TransportReindexAction.AsyncIndexBySearchAction action = action();
        action.mainRequest.getDestination().routing("=cat");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkIndexByScrollAction.wrap(index), doc(RoutingFieldMapper.NAME, "foo"));
        assertEquals("cat", index.routing());
    }

    public void testRoutingSetIfWithDegenerateValue() throws Exception {
        TransportReindexAction.AsyncIndexBySearchAction action = action();
        action.mainRequest.getDestination().routing("==]");
        IndexRequest index = new IndexRequest();
        action.copyMetadata(AbstractAsyncBulkIndexByScrollAction.wrap(index), doc(RoutingFieldMapper.NAME, "foo"));
        assertEquals("=]", index.routing());
    }

    @Override
    protected TransportReindexAction.AsyncIndexBySearchAction action() {
        return new TransportReindexAction.AsyncIndexBySearchAction(task, logger, null, threadPool, request(), listener(), null, null);
    }

    @Override
    protected ReindexRequest request() {
        return new ReindexRequest(new SearchRequest(), new IndexRequest());
    }
}
