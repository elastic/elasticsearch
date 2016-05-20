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

public class UpdateByQueryMetadataTests
        extends AbstractAsyncBulkIndexbyScrollActionMetadataTestCase<UpdateByQueryRequest, BulkIndexByScrollResponse> {
    public void testRoutingIsCopied() throws Exception {
        IndexRequest index = new IndexRequest();
        action().copyMetadata(AbstractAsyncBulkIndexByScrollAction.wrap(index), doc(RoutingFieldMapper.NAME, "foo"));
        assertEquals("foo", index.routing());
    }

    @Override
    protected TransportUpdateByQueryAction.AsyncIndexBySearchAction action() {
        return new TransportUpdateByQueryAction.AsyncIndexBySearchAction(task, logger, null, threadPool, request(), listener(), null, null);
    }

    @Override
    protected UpdateByQueryRequest request() {
        return new UpdateByQueryRequest(new SearchRequest());
    }
}
