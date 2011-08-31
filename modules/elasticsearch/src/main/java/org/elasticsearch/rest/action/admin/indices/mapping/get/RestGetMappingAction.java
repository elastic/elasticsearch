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

package org.elasticsearch.rest.action.admin.indices.mapping.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.*;
import static org.elasticsearch.rest.action.support.RestActions.*;

/**
 * @author kimchy (shay.banon)
 */
public class RestGetMappingAction extends BaseRestHandler {

    @Inject public RestGetMappingAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_mapping", this);
        controller.registerHandler(GET, "/{index}/_mapping", this);
        controller.registerHandler(GET, "/{index}/{type}/_mapping", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = splitIndices(request.param("index"));
        final Set<String> types = ImmutableSet.copyOf(splitTypes(request.param("type")));

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .filterRoutingTable(true)
                .filterNodes(true)
                .filteredIndices(indices);

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override public void onResponse(ClusterStateResponse response) {
                try {
                    MetaData metaData = response.state().metaData();
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();

                    if (indices.length == 1 && types.size() == 1) {
                        if (metaData.indices().isEmpty()) {
                            channel.sendResponse(new XContentThrowableRestResponse(request, new IndexMissingException(new Index(indices[0]))));
                            return;
                        }
                        boolean foundType = false;
                        IndexMetaData indexMetaData = metaData.iterator().next();
                        for (MappingMetaData mappingMd : indexMetaData.mappings().values()) {
                            if (!types.isEmpty() && !types.contains(mappingMd.type())) {
                                // filter this type out...
                                continue;
                            }
                            foundType = true;
                            byte[] mappingSource = mappingMd.source().uncompressed();
                            XContentParser parser = XContentFactory.xContent(mappingSource).createParser(mappingSource);
                            Map<String, Object> mapping = parser.map();
                            if (mapping.size() == 1 && mapping.containsKey(mappingMd.type())) {
                                // the type name is the root value, reduce it
                                mapping = (Map<String, Object>) mapping.get(mappingMd.type());
                            }
                            builder.field(mappingMd.type());
                            builder.map(mapping);
                        }
                        if (!foundType) {
                            channel.sendResponse(new XContentThrowableRestResponse(request, new TypeMissingException(new Index(indices[0]), types.iterator().next())));
                            return;
                        }
                    } else {
                        for (IndexMetaData indexMetaData : metaData) {
                            builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);

                            for (MappingMetaData mappingMd : indexMetaData.mappings().values()) {
                                if (!types.isEmpty() && !types.contains(mappingMd.type())) {
                                    // filter this type out...
                                    continue;
                                }
                                byte[] mappingSource = mappingMd.source().uncompressed();
                                XContentParser parser = XContentFactory.xContent(mappingSource).createParser(mappingSource);
                                Map<String, Object> mapping = parser.map();
                                if (mapping.size() == 1 && mapping.containsKey(mappingMd.type())) {
                                    // the type name is the root value, reduce it
                                    mapping = (Map<String, Object>) mapping.get(mappingMd.type());
                                }
                                builder.field(mappingMd.type());
                                builder.map(mapping);
                            }

                            builder.endObject();
                        }
                    }

                    builder.endObject();

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
