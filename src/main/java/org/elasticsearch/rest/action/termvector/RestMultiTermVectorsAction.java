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

package org.elasticsearch.rest.action.termvector;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.termvector.MultiTermVectorsRequest;
import org.elasticsearch.action.termvector.MultiTermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

public class RestMultiTermVectorsAction extends BaseRestHandler {

    @Inject
    public RestMultiTermVectorsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_mtermvectors", this);
        controller.registerHandler(POST, "/_mtermvectors", this);
        controller.registerHandler(GET, "/{index}/_mtermvectors", this);
        controller.registerHandler(POST, "/{index}/_mtermvectors", this);
        controller.registerHandler(GET, "/{index}/{type}/_mtermvectors", this);
        controller.registerHandler(POST, "/{index}/{type}/_mtermvectors", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest();
        multiTermVectorsRequest.listenerThreaded(false);
        multiTermVectorsRequest.preference(request.param("preference"));

        String[] sFields = null;
        String sField = request.param("fields");
        if (sField != null) {
            sFields = Strings.splitStringByCommaToArray(sField);
        }

        try {
            multiTermVectorsRequest.add(request.param("index"), request.param("type"), sFields, request.content());
        } catch (Throwable t) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, t));
            } catch (Throwable tIO) {
                logger.error("Failed to send failure response", tIO);
            }
            return;
        }

        client.multiTermVectors(multiTermVectorsRequest, new ActionListener<MultiTermVectorsResponse>() {
            @Override
            public void onResponse(MultiTermVectorsResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    response.toXContent(builder, request);
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Throwable t) {
                    onFailure(t);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (Throwable t) {
                    logger.error("Failed to send failure response", t);
                }
            }
        });
    }
}
