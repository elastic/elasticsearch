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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Expose reindex over rest.
 */
public class RestReindexAction extends AbstractBaseReindexRestHandler<ReindexRequest, ReindexAction> {

    private final ClusterService clusterService;

    public RestReindexAction(RestController controller, ClusterService clusterService) {
        super(ReindexAction.INSTANCE);
        this.clusterService = clusterService;
        controller.registerHandler(POST, "/_reindex", this);
    }

    @Override
    public String getName() {
        return "reindex_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // todo: remove system property escape hatch in 8.0
        // todo: fix version constant on backport to 7.x
        if (clusterService.state().nodes().getMinNodeVersion().before(Version.V_8_0_0)
                || System.getProperty("es.reindex.persistent", "true").equals("false")) {
            return doPrepareRequest(request, client, true, true);
        }

        boolean resilient = System.getProperty("es.reindex.persistent.resilient", "true").equals("false") == false;
        boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", true);

        // Build the internal request
        StartReindexTaskAction.Request internal = new StartReindexTaskAction.Request(setCommonOptions(request, buildRequest(request)),
            waitForCompletion, resilient);
        /*
         * Let's try and validate before forking so the user gets some error. The
         * task can't totally validate until it starts but this is better than
         * nothing.
         */
        ActionRequestValidationException validationException = internal.getReindexRequest().validate();
        if (validationException != null) {
            throw validationException;
        }

        // Executes the request and waits for completion
        if (waitForCompletion) {
            Map<String, String> params = new HashMap<>();
            params.put(BulkByScrollTask.Status.INCLUDE_CREATED, Boolean.toString(true));
            params.put(BulkByScrollTask.Status.INCLUDE_UPDATED, Boolean.toString(true));

            return channel -> client.execute(StartReindexTaskAction.INSTANCE, internal, new ActionListener<>() {

                private BulkIndexByScrollResponseContentListener listener = new BulkIndexByScrollResponseContentListener(channel, params);

                @Override
                public void onResponse(StartReindexTaskAction.Response response) {
                    listener.onResponse(response.getReindexResponse());
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } else {
            return channel -> client.execute(StartReindexTaskAction.INSTANCE, internal, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(StartReindexTaskAction.Response response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    // This is the ephemeral task-id from the first node that is assigned the task (for BWC).
                    builder.field("task", response.getTaskId());

                    // TODO: Are there error conditions for the non-wait case?
                    return new BytesRestResponse(RestStatus.OK, builder.endObject());
                }
            });
        }

    }

    @Override
    protected ReindexRequest buildRequest(RestRequest request) throws IOException {
        if (request.hasParam("pipeline")) {
            throw new IllegalArgumentException("_reindex doesn't support [pipeline] as a query parameter. "
                    + "Specify it in the [dest] object instead.");
        }

        ReindexRequest internal;
        try (XContentParser parser = request.contentParser()) {
            internal = ReindexRequest.fromXContent(parser);
        }

        if (request.hasParam("scroll")) {
            internal.setScroll(parseTimeValue(request.param("scroll"), "scroll"));
        }
        return internal;
    }
}
