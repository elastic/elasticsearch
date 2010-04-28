/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.rest.action.admin.indices.delete;

import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.rest.*;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.*;
import static org.elasticsearch.rest.RestResponse.Status.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestDeleteIndexAction extends BaseRestHandler {

    @Inject public RestDeleteIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.DELETE, "/{index}", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(request.param("index"));
        deleteIndexRequest.timeout(request.paramAsTime("timeout", timeValueSeconds(10)));
        client.admin().indices().delete(deleteIndexRequest, new ActionListener<DeleteIndexResponse>() {
            @Override public void onResponse(DeleteIndexResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject()
                            .field("ok", true)
                            .field("acknowledged", response.acknowledged())
                            .endObject();
                    channel.sendResponse(new JsonRestResponse(request, OK, builder));
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    Throwable t = unwrapCause(e);
                    if (t instanceof IndexMissingException) {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        channel.sendResponse(new JsonRestResponse(request, BAD_REQUEST, builder.startObject().field("error", t.getMessage()).endObject()));
                    } else {
                        channel.sendResponse(new JsonThrowableRestResponse(request, e));
                    }
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}