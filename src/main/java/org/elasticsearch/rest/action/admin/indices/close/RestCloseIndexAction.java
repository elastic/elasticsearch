/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.rest.action.admin.indices.close;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestCloseIndexAction extends BaseRestHandler {

    @Inject
    public RestCloseIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.POST, "/_close", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_close", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        closeIndexRequest.listenerThreaded(false);
        closeIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", closeIndexRequest.masterNodeTimeout()));
        closeIndexRequest.timeout(request.paramAsTime("timeout", timeValueSeconds(10)));
        if (request.hasParam("ignore_indices")) {
            closeIndexRequest.ignoreIndices(IgnoreIndices.fromString(request.param("ignore_indices")));
        }
        client.admin().indices().close(closeIndexRequest, new ActionListener<CloseIndexResponse>() {
            @Override
            public void onResponse(CloseIndexResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject()
                            .field(Fields.OK, true)
                            .field(Fields.ACKNOWLEDGED, response.isAcknowledged())
                            .endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    static final class Fields {
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString ACKNOWLEDGED = new XContentBuilderString("acknowledged");
    }
}
