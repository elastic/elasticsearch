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

package org.elasticsearch.rest.action.admin.indices.alias.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class RestGetAliasesAction extends BaseRestHandler {

    @Inject
    public RestGetAliasesAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_alias/{name}", this);
        controller.registerHandler(GET, "/{index}/_alias/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] aliases = request.paramAsStringArrayOrEmptyIfAll("name");
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final GetAliasesRequest getAliasesRequest = new GetAliasesRequest(aliases);
        getAliasesRequest.indices(indices);
        getAliasesRequest.indicesOptions(IndicesOptions.fromRequest(request, getAliasesRequest.indicesOptions()));
        getAliasesRequest.local(request.paramAsBoolean("local", getAliasesRequest.local()));

        client.admin().indices().getAliases(getAliasesRequest, new RestBuilderListener<GetAliasesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetAliasesResponse response, XContentBuilder builder) throws Exception {
                // empty body, if indices were specified but no aliases were
                if (indices.length > 0 && response.getAliases().isEmpty()) {
                    return new BytesRestResponse(OK, builder.startObject().endObject());
                } else if (response.getAliases().isEmpty()) {
                    String message = String.format(Locale.ROOT, "alias [%s] missing", toNamesString(getAliasesRequest.aliases()));
                    builder.startObject()
                            .field("error", message)
                            .field("status", RestStatus.NOT_FOUND.getStatus())
                            .endObject();
                    return new BytesRestResponse(RestStatus.NOT_FOUND, builder);
                }

                builder.startObject();
                for (ObjectObjectCursor<String, List<AliasMetaData>> entry : response.getAliases()) {
                    builder.startObject(entry.key, XContentBuilder.FieldCaseConversion.NONE);
                    builder.startObject(Fields.ALIASES);
                    for (AliasMetaData alias : entry.value) {
                        AliasMetaData.Builder.toXContent(alias, builder, ToXContent.EMPTY_PARAMS);
                    }
                    builder.endObject();
                    builder.endObject();
                }
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }

    private static String toNamesString(String... names) {
        if (names == null || names.length == 0) {
            return "";
        } else if (names.length == 1) {
            return names[0];
        } else {
            StringBuilder builder = new StringBuilder(names[0]);
            for (int i = 1; i < names.length; i++) {
                builder.append(',').append(names[i]);
            }
            return builder.toString();
        }
    }

    static class Fields {

        static final XContentBuilderString ALIASES = new XContentBuilderString("aliases");

    }
}
