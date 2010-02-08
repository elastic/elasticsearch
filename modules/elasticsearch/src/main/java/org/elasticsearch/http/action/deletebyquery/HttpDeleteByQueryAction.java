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

package org.elasticsearch.http.action.deletebyquery;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.ShardDeleteByQueryRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.http.*;
import org.elasticsearch.http.action.support.HttpActions;
import org.elasticsearch.http.action.support.HttpJsonBuilder;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.http.HttpResponse.Status.*;
import static org.elasticsearch.http.action.support.HttpActions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpDeleteByQueryAction extends BaseHttpServerHandler {

    @Inject public HttpDeleteByQueryAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.DELETE, "/{index}/_query", this);
        httpService.registerHandler(HttpRequest.Method.DELETE, "/{index}/{type}/_query", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(splitIndices(request.param("index")));
        // we just build a response and send it, no need to fork a thread
        deleteByQueryRequest.listenerThreaded(false);
        try {
            deleteByQueryRequest.querySource(HttpActions.parseQuerySource(request));
            deleteByQueryRequest.queryParserName(request.param("queryParserName"));
            String typesParam = request.param("type");
            if (typesParam != null) {
                deleteByQueryRequest.types(HttpActions.splitTypes(typesParam));
            }
            deleteByQueryRequest.timeout(TimeValue.parseTimeValue(request.param("timeout"), ShardDeleteByQueryRequest.DEFAULT_TIMEOUT));
        } catch (Exception e) {
            try {
                channel.sendResponse(new JsonHttpResponse(request, PRECONDITION_FAILED, JsonBuilder.cached().startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        client.execDeleteByQuery(deleteByQueryRequest, new ActionListener<DeleteByQueryResponse>() {
            @Override public void onResponse(DeleteByQueryResponse result) {
                try {
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
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
                    channel.sendResponse(new JsonHttpResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }


    @Override public boolean spawn() {
        // we don't spawn since we fork in index replication based on operation
        return false;
    }
}