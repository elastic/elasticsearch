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

package org.elasticsearch.rest.action.admin.indices.warmer.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.search.warmer.IndexWarmerMissingException;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetWarmerAction extends BaseRestHandler {

    @Inject
    public RestGetWarmerAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(GET, "/{index}/_warmer", this);
        controller.registerHandler(GET, "/{index}/_warmer/{name}", this);
        controller.registerHandler(GET, "/{index}/{type}/_warmer/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String name = request.param("name");

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .filterAll()
                .filterMetaData(false)
                .filteredIndices(indices);

        clusterStateRequest.listenerThreaded(false);

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(ClusterStateResponse response) {
                try {
                    MetaData metaData = response.getState().metaData();

                    if (indices.length == 1 && metaData.indices().isEmpty()) {
                        channel.sendResponse(new XContentThrowableRestResponse(request, new IndexMissingException(new Index(indices[0]))));
                        return;
                    }

                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();

                    boolean wroteOne = false;
                    for (IndexMetaData indexMetaData : metaData) {
                        IndexWarmersMetaData warmers = indexMetaData.custom(IndexWarmersMetaData.TYPE);
                        if (warmers == null) {
                            continue;
                        }

                        boolean foundOne = false;
                        for (IndexWarmersMetaData.Entry entry : warmers.entries()) {
                            if (name == null || Regex.simpleMatch(name, entry.name())) {
                                foundOne = true;
                                wroteOne = true;
                                break;
                            }
                        }

                        if (foundOne) {
                            builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);
                            builder.startObject(IndexWarmersMetaData.TYPE, XContentBuilder.FieldCaseConversion.NONE);
                            for (IndexWarmersMetaData.Entry entry : warmers.entries()) {
                                if (name == null || Regex.simpleMatch(name, entry.name())) {
                                    IndexWarmersMetaData.FACTORY.toXContent(entry, builder, request);
                                }
                            }
                            builder.endObject();
                            builder.endObject();
                        }
                    }

                    builder.endObject();

                    if (!wroteOne && name != null) {
                        // did not find any...
                        channel.sendResponse(new XContentThrowableRestResponse(request, new IndexWarmerMissingException(name)));
                        return;
                    }

                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Throwable e) {
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
}
