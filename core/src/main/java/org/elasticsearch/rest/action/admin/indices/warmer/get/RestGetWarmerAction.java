/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.rest.action.admin.indices.warmer.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersRequest;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetWarmerAction extends BaseRestHandler {

    @Inject
    public RestGetWarmerAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_warmer/{name}", this);
        controller.registerHandler(GET, "/{index}/_warmer/{name}", this);
        controller.registerHandler(GET, "/{index}/_warmers/{name}", this);
        controller.registerHandler(GET, "/{index}/{type}/_warmer/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        final String[] names = request.paramAsStringArray("name", Strings.EMPTY_ARRAY);

        GetWarmersRequest getWarmersRequest = new GetWarmersRequest();
        getWarmersRequest.indices(indices).types(types).warmers(names);
        getWarmersRequest.local(request.paramAsBoolean("local", getWarmersRequest.local()));
        getWarmersRequest.indicesOptions(IndicesOptions.fromRequest(request, getWarmersRequest.indicesOptions()));
        client.admin().indices().getWarmers(getWarmersRequest, new RestBuilderListener<GetWarmersResponse>(channel) {

            @Override
            public RestResponse buildResponse(GetWarmersResponse response, XContentBuilder builder) throws Exception {
                if (indices.length > 0 && response.warmers().isEmpty()) {
                    return new BytesRestResponse(OK, builder.startObject().endObject());
                }

                builder.startObject();
                for (ObjectObjectCursor<String, List<IndexWarmersMetaData.Entry>> entry : response.warmers()) {
                    builder.startObject(entry.key, XContentBuilder.FieldCaseConversion.NONE);
                    builder.startObject(IndexWarmersMetaData.TYPE, XContentBuilder.FieldCaseConversion.NONE);
                    for (IndexWarmersMetaData.Entry warmerEntry : entry.value) {
                        IndexWarmersMetaData.toXContent(warmerEntry, builder, request);
                    }
                    builder.endObject();
                    builder.endObject();
                }
                builder.endObject();

                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
