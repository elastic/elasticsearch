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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;

import java.io.IOException;

public abstract class AbstractBaseReindexRestHandler<
                Request extends AbstractBulkByScrollRequest<Request>,
                Response extends BulkIndexByScrollResponse,
                TA extends TransportAction<Request, Response>
            > extends BaseRestHandler {
    protected final IndicesQueriesRegistry indicesQueriesRegistry;
    protected final AggregatorParsers aggParsers;
    protected final Suggesters suggesters;
    private final ClusterService clusterService;
    private final RestController controller;
    private final TA action;

    protected AbstractBaseReindexRestHandler(Settings settings, RestController controller, Client client, ClusterService clusterService,
            IndicesQueriesRegistry indicesQueriesRegistry, AggregatorParsers aggParsers, Suggesters suggesters, TA action) {
        super(settings, controller, client);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        this.aggParsers = aggParsers;
        this.suggesters = suggesters;
        this.clusterService = clusterService;
        this.controller = controller;
        this.action = action;
    }

    protected void execute(RestRequest request, Request internalRequest, RestChannel channel) throws IOException {
        /*
         * Copy the HeadersAndContext from the request to the internalRequest so things that rely on context don't complain. Normally
         * requests don't have to do this because they receive a wrapped client implementation that does it transparently. We can't do that
         * because we need a task listener which is not exposed by the client interface.
         */
        for (String usefulHeader : controller.relevantHeaders()) {
            String headerValue = request.header(usefulHeader);
            if (headerValue != null) {
                internalRequest.putHeader(usefulHeader, headerValue);
            }
        }
        internalRequest.copyContextFrom(request);
        internalRequest.setRequestsPerSecond(request.paramAsFloat("requests_per_second", internalRequest.getRequestsPerSecond()));
        if (request.paramAsBoolean("wait_for_completion", true)) {
            action.execute(internalRequest, new BulkIndexByScrollResponseContentListener<Response>(channel));
            return;
        }
        /*
         * Lets try and validate before forking so the user gets some error. The
         * task can't totally validate until it starts but this is better than
         * nothing.
         */
        ActionRequestValidationException validationException = internalRequest.validate();
        if (validationException != null) {
            channel.sendResponse(new BytesRestResponse(channel, validationException));
            return;
        }
        Task task = action.execute(internalRequest, LoggingTaskListener.<Response>instance());
        sendTask(channel, task);
    }

    private void sendTask(RestChannel channel, Task task) throws IOException {
        XContentBuilder builder = channel.newBuilder();
        builder.startObject();
        builder.field("task", clusterService.localNode().getId() + ":" + task.getId());
        builder.endObject();
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
    }
}