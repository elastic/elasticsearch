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

package org.elasticsearch.rest.action.admin.indices.alias;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.*;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.*;

/**
 * @author kimchy (shay.banon)
 */
public class RestIndicesAliasesAction extends BaseRestHandler {

    @Inject public RestIndicesAliasesAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_aliases", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        try {
            // {
            //     actions : [
            //         { add : { index : "test1", alias : "alias1" } }
            //         { remove : { index : "test1", alias : "alias1" } }
            //     ]
            // }
            XContentParser parser = XContentFactory.xContent(request.contentByteArray(), request.contentByteArrayOffset(), request.contentLength())
                    .createParser(request.contentByteArray(), request.contentByteArrayOffset(), request.contentLength());
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                throw new ElasticSearchIllegalArgumentException("No action is specified");
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            String action = parser.currentName();
                            AliasAction.Type type;
                            if ("add".equals(action)) {
                                type = AliasAction.Type.ADD;
                            } else if ("remove".equals(action)) {
                                type = AliasAction.Type.REMOVE;
                            } else {
                                throw new ElasticSearchIllegalArgumentException("Alias action [" + action + "] not supported");
                            }
                            String index = null;
                            String alias = null;
                            String currentFieldName = null;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token == XContentParser.Token.VALUE_STRING) {
                                    if ("index".equals(currentFieldName)) {
                                        index = parser.text();
                                    } else if ("alias".equals(currentFieldName)) {
                                        alias = parser.text();
                                    }
                                }
                            }
                            if (index == null) {
                                throw new ElasticSearchIllegalArgumentException("Alias action [" + action + "] requires an [index] to be set");
                            }
                            if (alias == null) {
                                throw new ElasticSearchIllegalArgumentException("Alias action [" + action + "] requires an [alias] to be set");
                            }
                            if (type == AliasAction.Type.ADD) {
                                indicesAliasesRequest.addAlias(index, alias);
                            } else if (type == AliasAction.Type.REMOVE) {
                                indicesAliasesRequest.removeAlias(index, alias);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.warn("Failed to send response", e1);
            }
            return;
        }
        client.admin().indices().aliases(indicesAliasesRequest, new ActionListener<IndicesAliasesResponse>() {
            @Override public void onResponse(IndicesAliasesResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    builder.startObject()
                            .field("ok", true)
                            .endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}