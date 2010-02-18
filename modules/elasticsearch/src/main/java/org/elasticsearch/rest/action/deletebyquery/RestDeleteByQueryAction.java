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

package org.elasticsearch.rest.action.deletebyquery;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.ShardDeleteByQueryRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestJsonBuilder;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestResponse.Status.*;
import static org.elasticsearch.rest.action.support.RestActions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestDeleteByQueryAction extends BaseRestHandler {

    @Inject public RestDeleteByQueryAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(DELETE, "/{index}/_query", this);
        controller.registerHandler(DELETE, "/{index}/{type}/_query", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(splitIndices(request.param("index")));
        // we just build a response and send it, no need to fork a thread
        deleteByQueryRequest.listenerThreaded(false);
        try {
            deleteByQueryRequest.querySource(RestActions.parseQuerySource(request));
            deleteByQueryRequest.queryParserName(request.param("queryParserName"));
            String typesParam = request.param("type");
            if (typesParam != null) {
                deleteByQueryRequest.types(RestActions.splitTypes(typesParam));
            }
            deleteByQueryRequest.timeout(request.paramAsTime("timeout", ShardDeleteByQueryRequest.DEFAULT_TIMEOUT));
        } catch (Exception e) {
            try {
                channel.sendResponse(new JsonRestResponse(request, PRECONDITION_FAILED, JsonBuilder.jsonBuilder().startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        client.execDeleteByQuery(deleteByQueryRequest, new ActionListener<DeleteByQueryResponse>() {
            @Override public void onResponse(DeleteByQueryResponse result) {
                try {
                    JsonBuilder builder = RestJsonBuilder.cached(request);
                    builder.startObject().field("ok", true);

                    builder.startObject("_indices");
                    for (IndexDeleteByQueryResponse indexDeleteByQueryResponse : result.indices().values()) {
                        builder.startObject(indexDeleteByQueryResponse.index());

                        builder.startObject("_shards");
                        builder.field("total", indexDeleteByQueryResponse.totalShards());
                        builder.field("successful", indexDeleteByQueryResponse.successfulShards());
                        builder.field("failed", indexDeleteByQueryResponse.failedShards());
                        builder.endObject();

                        builder.endObject();
                    }
                    builder.endObject();

                    builder.endObject();
                    channel.sendResponse(new JsonRestResponse(request, OK, builder));
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