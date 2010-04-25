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

package org.elasticsearch.rest.action.get;

import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetField;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.*;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestResponse.Status.*;
import static org.elasticsearch.rest.action.support.RestJsonBuilder.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestGetAction extends BaseRestHandler {

    private final static Pattern fieldsPattern;

    static {
        fieldsPattern = Pattern.compile(",");
    }

    @Inject public RestGetAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        final GetRequest getRequest = new GetRequest(request.param("index"), request.param("type"), request.param("id"));
        // no need to have a threaded listener since we just send back a response
        getRequest.listenerThreaded(false);
        // if we have a local operation, execute it on a thread since we don't spawn
        getRequest.operationThreaded(true);


        String sField = request.param("fields");
        if (sField != null) {
            String[] sFields = fieldsPattern.split(sField);
            if (sFields != null) {
                getRequest.fields(sFields);
            }
        }


        client.get(getRequest, new ActionListener<GetResponse>() {
            @Override public void onResponse(GetResponse response) {
                try {
                    if (!response.exists()) {
                        JsonBuilder builder = restJsonBuilder(request);
                        builder.startObject();
                        builder.field("_index", response.index());
                        builder.field("_type", response.type());
                        builder.field("_id", response.id());
                        builder.endObject();
                        channel.sendResponse(new JsonRestResponse(request, NOT_FOUND, builder));
                    } else {
                        JsonBuilder builder = restJsonBuilder(request);
                        builder.startObject();
                        builder.field("_index", response.index());
                        builder.field("_type", response.type());
                        builder.field("_id", response.id());
                        if (response.source() != null) {
                            builder.raw(", \"_source\" : ");
                            builder.raw(response.source());
                        }

                        if (response.fields() != null && !response.fields().isEmpty()) {
                            builder.startObject("fields");
                            for (GetField field : response.fields().values()) {
                                if (field.values().isEmpty()) {
                                    continue;
                                }
                                if (field.values().size() == 1) {
                                    builder.field(field.name(), field.values().get(0));
                                } else {
                                    builder.field(field.name());
                                    builder.startArray();
                                    for (Object value : field.values()) {
                                        builder.value(value);
                                    }
                                    builder.endArray();
                                }
                            }
                            builder.endObject();
                        }


                        builder.endObject();
                        channel.sendResponse(new JsonRestResponse(request, OK, builder));
                    }
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new JsonThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}