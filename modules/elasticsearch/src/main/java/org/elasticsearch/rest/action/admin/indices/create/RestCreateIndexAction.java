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

package org.elasticsearch.rest.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.*;
import static org.elasticsearch.common.unit.TimeValue.*;
import static org.elasticsearch.rest.RestResponse.Status.*;

/**
 * @author kimchy (shay.banon)
 */
public class RestCreateIndexAction extends BaseRestHandler {

    @Inject public RestCreateIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(RestRequest.Method.PUT, "/{index}", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        String bodySettings = request.contentAsString();
        Settings indexSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        if (Strings.hasText(bodySettings)) {
            try {
                indexSettings = ImmutableSettings.settingsBuilder().loadFromSource(bodySettings).build();
            } catch (Exception e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, BAD_REQUEST, new SettingsException("Failed to parse index settings", e)));
                } catch (IOException e1) {
                    logger.warn("Failed to send response", e1);
                    return;
                }
            }
        }
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(request.param("index"), indexSettings);
        createIndexRequest.timeout(request.paramAsTime("timeout", timeValueSeconds(10)));
        client.admin().indices().create(createIndexRequest, new ActionListener<CreateIndexResponse>() {
            @Override public void onResponse(CreateIndexResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject()
                            .field("ok", true)
                            .field("acknowledged", response.acknowledged())
                            .endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    Throwable t = unwrapCause(e);
                    if (t instanceof IndexAlreadyExistsException || t instanceof InvalidIndexNameException) {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", t.getMessage()).endObject()));
                    } else {
                        channel.sendResponse(new XContentThrowableRestResponse(request, e));
                    }
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
