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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersRequest;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import java.io.IOException;
import java.util.Map;

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
        final String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        final String[] names = request.paramAsStringArray("name", Strings.EMPTY_ARRAY);
        boolean local = request.paramAsBooleanOptional("local", false);

        GetWarmersRequest getWarmersRequest = new GetWarmersRequest();
        getWarmersRequest.indices(indices).types(types).warmers(names).local(local);
        client.admin().indices().getWarmers(getWarmersRequest, new ActionListener<GetWarmersResponse>() {

            @Override
            public void onResponse(GetWarmersResponse response) {
                try {
                    if (indices.length > 0 && response.warmers().isEmpty()) {
                        channel.sendResponse(new XContentThrowableRestResponse(request, new IndexMissingException(new Index(indices[0]))));
                        return;
                    }

                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    for (Map.Entry<String, ImmutableList<IndexWarmersMetaData.Entry>> entry : response.warmers().entrySet()) {
                        builder.startObject(entry.getKey(), XContentBuilder.FieldCaseConversion.NONE);
                        builder.startObject(IndexWarmersMetaData.TYPE, XContentBuilder.FieldCaseConversion.NONE);
                        for (IndexWarmersMetaData.Entry warmerEntry : entry.getValue()) {
                            IndexWarmersMetaData.FACTORY.toXContent(warmerEntry, builder, request);
                        }
                        builder.endObject();
                        builder.endObject();
                    }
                    builder.endObject();

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
