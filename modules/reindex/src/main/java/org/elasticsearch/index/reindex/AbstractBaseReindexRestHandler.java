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
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractBaseReindexRestHandler<
                Request extends AbstractBulkByScrollRequest<Request>,
                TA extends TransportAction<Request, BulkIndexByScrollResponse>
            > extends BaseRestHandler {

    protected final IndicesQueriesRegistry indicesQueriesRegistry;
    protected final AggregatorParsers aggParsers;
    protected final Suggesters suggesters;
    private final ClusterService clusterService;
    private final TA action;

    protected AbstractBaseReindexRestHandler(Settings settings, Client client,
            IndicesQueriesRegistry indicesQueriesRegistry, AggregatorParsers aggParsers, Suggesters suggesters,
            ClusterService clusterService, TA action) {
        super(settings, client);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        this.aggParsers = aggParsers;
        this.suggesters = suggesters;
        this.clusterService = clusterService;
        this.action = action;
    }

    protected void handleRequest(RestRequest request, RestChannel channel,
                                 boolean includeCreated, boolean includeUpdated) throws IOException {

        // Build the internal request
        Request internal = setCommonOptions(request, buildRequest(request));

        // Executes the request and waits for completion
        if (request.paramAsBoolean("wait_for_completion", true)) {
            Map<String, String> params = new HashMap<>();
            params.put(BulkByScrollTask.Status.INCLUDE_CREATED, Boolean.toString(includeCreated));
            params.put(BulkByScrollTask.Status.INCLUDE_UPDATED, Boolean.toString(includeUpdated));

            action.execute(internal, new BulkIndexByScrollResponseContentListener<>(channel, params));
            return;
        } else {
            internal.setShouldPersistResult(true);
        }

        /*
         * Lets try and validate before forking so the user gets some error. The
         * task can't totally validate until it starts but this is better than
         * nothing.
         */
        ActionRequestValidationException validationException = internal.validate();
        if (validationException != null) {
            channel.sendResponse(new BytesRestResponse(channel, validationException));
            return;
        }
        sendTask(channel, action.execute(internal, LoggingTaskListener.instance()));
    }

    /**
     * Build the Request based on the RestRequest.
     */
    protected abstract Request buildRequest(RestRequest request) throws IOException;

    /**
     * Sets common options of {@link AbstractBulkByScrollRequest} requests.
     */
    protected Request setCommonOptions(RestRequest restRequest, Request request) {
        assert restRequest != null : "RestRequest should not be null";
        assert request != null : "Request should not be null";

        request.setRefresh(restRequest.paramAsBoolean("refresh", request.isRefresh()));
        request.setTimeout(restRequest.paramAsTime("timeout", request.getTimeout()));

        String consistency = restRequest.param("consistency");
        if (consistency != null) {
            request.setConsistency(WriteConsistencyLevel.fromString(consistency));
        }

        Float requestsPerSecond = parseRequestsPerSecond(restRequest);
        if (requestsPerSecond != null) {
            request.setRequestsPerSecond(requestsPerSecond);
        }
        return request;
    }

    private void sendTask(RestChannel channel, Task task) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("task", clusterService.localNode().getId() + ":" + task.getId());
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * @return requests_per_second from the request as a float if it was on the request, null otherwise
     */
    public static Float parseRequestsPerSecond(RestRequest request) {
        String requestsPerSecondString = request.param("requests_per_second");
        if (requestsPerSecondString == null) {
            return null;
        }
        if ("unlimited".equals(requestsPerSecondString)) {
            return  Float.POSITIVE_INFINITY;
        }
        float requestsPerSecond;
        try {
            requestsPerSecond = Float.parseFloat(requestsPerSecondString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "[requests_per_second] must be a float greater than 0. Use \"unlimited\" to disable throttling.", e);
        }
        if (requestsPerSecond <= 0) {
            // We validate here and in the setters because the setters use "Float.POSITIVE_INFINITY" instead of "unlimited"
            throw new IllegalArgumentException(
                    "[requests_per_second] must be a float greater than 0. Use \"unlimited\" to disable throttling.");
        }
        return requestsPerSecond;
    }
}
