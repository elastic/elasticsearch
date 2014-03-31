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

package org.elasticsearch.rest.action.admin.indices.recovery;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 * REST handler to report on index recoveries.
 */
public class RestRecoveryAction extends BaseRestHandler  {

    @Inject
    public RestRecoveryAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_recovery", this);
        controller.registerHandler(GET, "/{index}/_recovery", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {

        final RecoveryRequest recoveryRequest = new RecoveryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        recoveryRequest.detailed(request.paramAsBoolean("detailed", false));
        recoveryRequest.activeOnly(request.paramAsBoolean("active_only", false));
        recoveryRequest.listenerThreaded(false);
        recoveryRequest.indicesOptions(IndicesOptions.fromRequest(request, recoveryRequest.indicesOptions()));

        client.admin().indices().recoveries(recoveryRequest, new ActionListener<RecoveryResponse>() {

            @Override
            public void onResponse(RecoveryResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);

                    if (response.hasRecoveries()) {
                        response.detailed(recoveryRequest.detailed());
                        builder.startObject();
                        response.toXContent(builder, request);
                        builder.endObject();
                    }
                    channel.sendResponse(new BytesRestResponse(OK, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new BytesRestResponse(request, e));
                } catch (IOException ioe) {
                    logger.error("Failed to send failure response", ioe);
                }
            }
        });

    }
}

