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

package org.elasticsearch.rest.action.admin.cluster.allocation;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.io.IOException;

/**
 * Class handling cluster allocation explanation at the REST level
 */
public class RestClusterAllocationExplainAction extends BaseRestHandler {

    @Inject
    public RestClusterAllocationExplainAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.GET, "/_cluster/allocation/explain", this);
        controller.registerHandler(RestRequest.Method.POST, "/_cluster/allocation/explain", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        ClusterAllocationExplainRequest req;
        if (RestActions.hasBodyContent(request) == false) {
            // Empty request signals "explain the first unassigned shard you find"
            req = new ClusterAllocationExplainRequest();
        } else {
            BytesReference content = RestActions.getRestContent(request);
            try (XContentParser parser = XContentFactory.xContent(content).createParser(content)) {
                req = ClusterAllocationExplainRequest.parse(parser);
            } catch (IOException e) {
                logger.debug("failed to parse allocation explain request", e);
                channel.sendResponse(
                    new BytesRestResponse(ExceptionsHelper.status(e), BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
                return;
            }
        }

        try {
            req.includeYesDecisions(request.paramAsBoolean("include_yes_decisions", false));
            client.admin().cluster().allocationExplain(req, new RestBuilderListener<ClusterAllocationExplainResponse>(channel) {
                @Override
                public RestResponse buildResponse(ClusterAllocationExplainResponse response, XContentBuilder builder) throws Exception {
                    response.getExplanation().toXContent(builder, ToXContent.EMPTY_PARAMS);
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        } catch (Exception e) {
            logger.error("failed to explain allocation", e);
            channel.sendResponse(new BytesRestResponse(ExceptionsHelper.status(e), BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
        }
    }
}
