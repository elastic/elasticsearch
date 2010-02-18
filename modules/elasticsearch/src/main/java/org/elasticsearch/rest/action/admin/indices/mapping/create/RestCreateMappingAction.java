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

package org.elasticsearch.rest.action.admin.indices.mapping.create;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.mapper.InvalidTypeNameException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestJsonBuilder;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.*;
import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestResponse.Status.*;
import static org.elasticsearch.rest.action.support.RestActions.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestCreateMappingAction extends BaseRestHandler {

    @Inject public RestCreateMappingAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(PUT, "/{index}/_mapping", this);
        controller.registerHandler(PUT, "/{index}/{type}/_mapping", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        CreateMappingRequest createMappingRequest = createMappingRequest(splitIndices(request.param("index")));
        createMappingRequest.type(request.param("type"));
        createMappingRequest.mappingSource(request.contentAsString());
        createMappingRequest.timeout(TimeValue.parseTimeValue(request.param("timeout"), timeValueSeconds(10)));
        client.admin().indices().execCreateMapping(createMappingRequest, new ActionListener<CreateMappingResponse>() {
            @Override public void onResponse(CreateMappingResponse result) {
                try {
                    JsonBuilder builder = RestJsonBuilder.cached(request);
                    builder.startObject()
                            .field("ok", true)
                            .endObject();
                    channel.sendResponse(new JsonRestResponse(request, OK, builder));
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    Throwable t = unwrapCause(e);
                    if (t instanceof IndexMissingException || t instanceof InvalidTypeNameException) {
                        channel.sendResponse(new JsonRestResponse(request, BAD_REQUEST, JsonBuilder.jsonBuilder().startObject().field("error", t.getMessage()).endObject()));
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