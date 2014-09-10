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

package org.elasticsearch.rest.action.admin.indices.cache.clear;

import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 *
 */
public class RestClearIndicesCacheAction extends BaseRestHandler {

    @Inject
    public RestClearIndicesCacheAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_cache/clear", this);
        controller.registerHandler(POST, "/{index}/_cache/clear", this);

        controller.registerHandler(GET, "/_cache/clear", this);
        controller.registerHandler(GET, "/{index}/_cache/clear", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(Strings.splitStringByCommaToArray(request.param("index")));
        clearIndicesCacheRequest.listenerThreaded(false);
        clearIndicesCacheRequest.indicesOptions(IndicesOptions.fromRequest(request, clearIndicesCacheRequest.indicesOptions()));
        fromRequest(request, clearIndicesCacheRequest);
        client.admin().indices().clearCache(clearIndicesCacheRequest, new RestBuilderListener<ClearIndicesCacheResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClearIndicesCacheResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                buildBroadcastShardsHeader(builder, response);
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }

    public static ClearIndicesCacheRequest fromRequest(final RestRequest request, ClearIndicesCacheRequest clearIndicesCacheRequest) {

        for (Map.Entry<String, String> entry : request.params().entrySet()) {

            if (Fields.FILTER.match(entry.getKey())) {
                clearIndicesCacheRequest.filterCache(request.paramAsBoolean(entry.getKey(), clearIndicesCacheRequest.filterCache()));
            }
            if (Fields.FIELD_DATA.match(entry.getKey())) {
                clearIndicesCacheRequest.fieldDataCache(request.paramAsBoolean(entry.getKey(), clearIndicesCacheRequest.fieldDataCache()));
            }
            if (Fields.ID.match(entry.getKey())) {
                clearIndicesCacheRequest.idCache(request.paramAsBoolean(entry.getKey(), clearIndicesCacheRequest.idCache()));
            }
            if (Fields.RECYCLER.match(entry.getKey())) {
                clearIndicesCacheRequest.recycler(request.paramAsBoolean(entry.getKey(), clearIndicesCacheRequest.recycler()));
            }
            if (Fields.FIELDS.match(entry.getKey())) {
                clearIndicesCacheRequest.fields(request.paramAsStringArray(entry.getKey(), clearIndicesCacheRequest.fields()));
            }
            if (Fields.FILTER_KEYS.match(entry.getKey())) {
                clearIndicesCacheRequest.filterKeys(request.paramAsStringArray(entry.getKey(), clearIndicesCacheRequest.filterKeys()));
            }
        }

        return clearIndicesCacheRequest;
    }

    public static class Fields {
        public static final ParseField FILTER = new ParseField("filter", "filter_cache");
        public static final ParseField FIELD_DATA = new ParseField("field_data", "fielddata");
        public static final ParseField ID = new ParseField("id", "id_cache");
        public static final ParseField RECYCLER = new ParseField("recycler");
        public static final ParseField FIELDS = new ParseField("fields");
        public static final ParseField FILTER_KEYS = new ParseField("filter_keys");
    }

}
