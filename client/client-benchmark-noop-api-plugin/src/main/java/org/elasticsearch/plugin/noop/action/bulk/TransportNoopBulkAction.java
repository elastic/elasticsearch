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
package org.elasticsearch.plugin.noop.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportNoopBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {
    private static final BulkItemResponse ITEM_RESPONSE = new BulkItemResponse(1, DocWriteRequest.OpType.UPDATE,
        new UpdateResponse(new ShardId("mock", "", 1), "mock_type", "1", 1L, DocWriteResponse.Result.CREATED));

    @Inject
    public TransportNoopBulkAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                      ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, NoopBulkAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, BulkRequest::new);
    }

    @Override
    protected void doExecute(BulkRequest request, ActionListener<BulkResponse> listener) {
        final int itemCount = request.requests().size();
        // simulate at least a realistic amount of data that gets serialized
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[itemCount];
        for (int idx = 0; idx < itemCount; idx++) {
            bulkItemResponses[idx] = ITEM_RESPONSE;
        }
        listener.onResponse(new BulkResponse(bulkItemResponses, 0));
    }
}
