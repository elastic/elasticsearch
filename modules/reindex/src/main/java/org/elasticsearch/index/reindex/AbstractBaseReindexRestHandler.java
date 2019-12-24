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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractBaseReindexRestHandler<
                Request extends AbstractBulkByScrollRequest<Request>,
                A extends ActionType<BulkByScrollResponse>
            > extends BaseRestHandler {

    private final A action;

    protected AbstractBaseReindexRestHandler(A action) {
        this.action = action;
    }

    protected RestChannelConsumer doPrepareRequest(RestRequest request, NodeClient client,
                                                   boolean includeCreated, boolean includeUpdated) throws IOException {
        // Build the internal request
        Request internal = setCommonOptions(request, buildRequest(request));

        // Executes the request and waits for completion
        if (request.paramAsBoolean("wait_for_completion", true)) {
            Map<String, String> params = new HashMap<>();
            params.put(BulkByScrollTask.Status.INCLUDE_CREATED, Boolean.toString(includeCreated));
            params.put(BulkByScrollTask.Status.INCLUDE_UPDATED, Boolean.toString(includeUpdated));

            return channel -> client.executeLocally(action, internal, new BulkIndexByScrollResponseContentListener(channel, params));
        } else {
            internal.setShouldStoreResult(true);
        }

        /*
         * Let's try and validate before forking so the user gets some error. The
         * task can't totally validate until it starts but this is better than
         * nothing.
         */
        ActionRequestValidationException validationException = internal.validate();
        if (validationException != null) {
            throw validationException;
        }
        return sendTask(client.getLocalNodeId(), client.executeLocally(action, internal, LoggingTaskListener.instance()));
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

        Integer slices = parseSlices(restRequest);
        if (slices != null) {
            request.setSlices(slices);
        }

        String waitForActiveShards = restRequest.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            request.setWaitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }

        Float requestsPerSecond = parseRequestsPerSecond(restRequest);
        if (requestsPerSecond != null) {
            request.setRequestsPerSecond(requestsPerSecond);
        }

        if (restRequest.hasParam("max_docs")) {
            setMaxDocsValidateIdentical(request, restRequest.paramAsInt("max_docs", -1));
        }

        return request;
    }

    private RestChannelConsumer sendTask(String localNodeId, Task task) {
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("task", localNodeId + ":" + task.getId());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }

    private static Integer parseSlices(RestRequest request) {
        String slicesString = request.param("slices");
        if (slicesString == null) {
            return null;
        }

        if (slicesString.equals(AbstractBulkByScrollRequest.AUTO_SLICES_VALUE)) {
            return AbstractBulkByScrollRequest.AUTO_SLICES;
        }

        int slices;
        try {
            slices = Integer.parseInt(slicesString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "[slices] must be a positive integer or the string \"auto\", but was [" + slicesString + "]", e);
        }

        if (slices < 1) {
            throw new IllegalArgumentException(
                "[slices] must be a positive integer or the string \"auto\", but was [" + slicesString + "]");
        }

        return slices;
    }

    /**
     * @return requests_per_second from the request as a float if it was on the request, null otherwise
     */
    public static Float parseRequestsPerSecond(RestRequest request) {
        String requestsPerSecondString = request.param("requests_per_second");
        if (requestsPerSecondString == null) {
            return null;
        }
        float requestsPerSecond;
        try {
            requestsPerSecond = Float.parseFloat(requestsPerSecondString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "[requests_per_second] must be a float greater than 0. Use -1 to disable throttling.", e);
        }
        if (requestsPerSecond == -1) {
            return Float.POSITIVE_INFINITY;
        }
        if (requestsPerSecond <= 0) {
            // We validate here and in the setters because the setters use "Float.POSITIVE_INFINITY" instead of -1
            throw new IllegalArgumentException(
                    "[requests_per_second] must be a float greater than 0. Use -1 to disable throttling.");
        }
        return requestsPerSecond;
    }

    static void setMaxDocsValidateIdentical(AbstractBulkByScrollRequest<?> request, int maxDocs) {
        if (request.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES && request.getMaxDocs() != maxDocs) {
            throw new IllegalArgumentException("[max_docs] set to two different values [" + request.getMaxDocs() + "]" +
                " and [" + maxDocs + "]");
        } else {
            request.setMaxDocs(maxDocs);
        }
    }
}
