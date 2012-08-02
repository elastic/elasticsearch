/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices.template.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
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
public class RestPutIndexTemplateAction extends BaseRestHandler {

    @Inject
    public RestPutIndexTemplateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.PUT, "/_template/{name}", this);
        controller.registerHandler(RestRequest.Method.POST, "/_template/{name}", new CreateHandler());
    }

    final class CreateHandler implements RestHandler {
        @Override
        public void handleRequest(RestRequest request, RestChannel channel) {
            request.params().put("create", "true");
            RestPutIndexTemplateAction.this.handleRequest(request, channel);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest(request.param("name"));
        putRequest.listenerThreaded(false);
        putRequest.template(request.param("template", putRequest.template()));
        putRequest.order(request.paramAsInt("order", putRequest.order()));

        try {
            putRequest.create(request.paramAsBoolean("create", false));
            putRequest.cause(request.param("cause", ""));
            putRequest.timeout(request.paramAsTime("timeout", timeValueSeconds(10)));
            putRequest.source(request.content());
        } catch (Exception e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.warn("Failed to send response", e1);
            }
            return;
        }

        client.admin().indices().putTemplate(putRequest, new ActionListener<PutIndexTemplateResponse>() {
            @Override
            public void onResponse(PutIndexTemplateResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject()
                            .field(Fields.OK, true)
                            .field(Fields.ACKNOWLEDGED, response.acknowledged())
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