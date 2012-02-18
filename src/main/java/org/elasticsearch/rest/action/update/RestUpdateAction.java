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

package org.elasticsearch.rest.action.update;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class RestUpdateAction extends BaseRestHandler {

    @Inject
    public RestUpdateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_update", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        UpdateRequest updateRequest = new UpdateRequest(request.param("index"), request.param("type"), request.param("id"));
        updateRequest.routing(request.param("routing"));
        updateRequest.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
        updateRequest.timeout(request.paramAsTime("timeout", updateRequest.timeout()));
        updateRequest.refresh(request.paramAsBoolean("refresh", updateRequest.refresh()));
        String replicationType = request.param("replication");
        if (replicationType != null) {
            updateRequest.replicationType(ReplicationType.fromString(replicationType));
        }
        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            updateRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        updateRequest.percolate(request.param("percolate", null));
        // we just send a response, no need to fork
        updateRequest.listenerThreaded(false);
        updateRequest.script(request.param("script"));
        updateRequest.scriptLang(request.param("lang"));
        for (Map.Entry<String, String> entry : request.params().entrySet()) {
            if (entry.getKey().startsWith("sp_")) {
                updateRequest.addScriptParam(entry.getKey().substring(3), entry.getValue());
            }
        }
        updateRequest.retryOnConflict(request.paramAsInt("retry_on_conflict", updateRequest.retryOnConflict()));

        // see if we have it in the body
        if (request.hasContent()) {
            XContentType xContentType = XContentFactory.xContentType(request.contentByteArray(), request.contentByteArrayOffset(), request.contentLength());
            if (xContentType != null) {
                try {
                    Map<String, Object> content = XContentFactory.xContent(xContentType)
                            .createParser(request.contentByteArray(), request.contentByteArrayOffset(), request.contentLength()).mapAndClose();
                    if (content.containsKey("script")) {
                        updateRequest.script(content.get("script").toString());
                    }
                    if (content.containsKey("lang")) {
                        updateRequest.scriptLang(content.get("lang").toString());
                    }
                    if (content.containsKey("params")) {
                        updateRequest.scriptParams((Map<String, Object>) content.get("params"));
                    }
                } catch (Exception e) {
                    try {
                        channel.sendResponse(new XContentThrowableRestResponse(request, e));
                    } catch (IOException e1) {
                        logger.warn("Failed to send response", e1);
                    }
                    return;
                }
            }
        }

        client.update(updateRequest, new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject()
                            .field(Fields.OK, true)
                            .field(Fields._INDEX, response.index())
                            .field(Fields._TYPE, response.type())
                            .field(Fields._ID, response.id())
                            .field(Fields._VERSION, response.version());
                    if (response.matches() != null) {
                        builder.startArray(Fields.MATCHES);
                        for (String match : response.matches()) {
                            builder.value(match);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                    RestStatus status = OK;
                    if (response.version() == 1) {
                        status = CREATED;
                    }
                    channel.sendResponse(new XContentRestResponse(request, status, builder));
                } catch (Exception e) {
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
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
    }
}
