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

package org.elasticsearch.rest.action.admin.indices;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

/**
 * The REST handler for get alias and head alias APIs.
 */
public class RestGetAliasesAction extends BaseRestHandler {

    public RestGetAliasesAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_alias", this);
        controller.registerHandler(GET, "/_aliases", this);
        controller.registerHandler(GET, "/_alias/{name}", this);
        controller.registerHandler(HEAD, "/_alias/{name}", this);
        controller.registerHandler(GET, "/{index}/_alias", this);
        controller.registerHandler(HEAD, "/{index}/_alias", this);
        controller.registerHandler(GET, "/{index}/_alias/{name}", this);
        controller.registerHandler(HEAD, "/{index}/_alias/{name}", this);
    }

    @Override
    public String getName() {
        return "get_aliases_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        final String[] aliases = request.paramAsStringArrayOrEmptyIfAll("name");
        final GetAliasesRequest getAliasesRequest = new GetAliasesRequest(aliases);
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        getAliasesRequest.indices(indices);
        getAliasesRequest.indicesOptions(IndicesOptions.fromRequest(request, getAliasesRequest.indicesOptions()));
        getAliasesRequest.local(request.paramAsBoolean("local", getAliasesRequest.local()));

        return channel -> client.admin().indices().getAliases(getAliasesRequest, new RestBuilderListener<GetAliasesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetAliasesResponse response, XContentBuilder builder) throws Exception {
                final Set<String> returnedAliasNames = new HashSet<>();
                for (final ObjectObjectCursor<String, List<AliasMetaData>> cursor : response.getAliases()) {
                    for (final AliasMetaData aliasMetaData : cursor.value) {
                        returnedAliasNames.add(aliasMetaData.alias());
                    }
                }
                // compute explicitly requested aliases that have not been found
                final SortedSet<String> missingAliases = new TreeSet<>();
                for (int i = 0; i < aliases.length; i++) {
                    if (MetaData.ALL.equals(aliases[i]) || Regex.isSimpleMatchPattern(aliases[i]) || aliases[i].charAt(0) == '-') {
                        // only explicitly requested aliases will be returning 404
                        continue;
                    }
                    int j = i + 1;
                    for (; j < aliases.length; j++) {
                        if (aliases[j].charAt(0) == '-'
                                && (Regex.isSimpleMatchPattern(aliases[j].substring(1)) || MetaData.ALL.equals(aliases[j].substring(1)))) {
                            // this is an exclude pattern
                            if (Regex.simpleMatch(aliases[j].substring(1), aliases[i]) || MetaData.ALL.equals(aliases[j].substring(1))) {
                                break;
                            }
                        }
                    }
                    if (j == aliases.length) {
                        // explicitly requested alias not excluded by any "-" wildcard in expression
                        if (false == returnedAliasNames.contains(aliases[i])) {
                            missingAliases.add(aliases[i]);
                        }
                    }
                }

                final RestStatus status;
                builder.startObject();
                {
                    if (missingAliases.isEmpty()) {
                        status = RestStatus.OK;
                    } else {
                        status = RestStatus.NOT_FOUND;
                        final String message;
                        if (missingAliases.size() == 1) {
                            message = String.format(Locale.ROOT, "alias [%s] missing",
                                    Strings.collectionToCommaDelimitedString(missingAliases));
                        } else {
                            message = String.format(Locale.ROOT, "aliases [%s] missing",
                                    Strings.collectionToCommaDelimitedString(missingAliases));
                        }
                        builder.field("error", message);
                        builder.field("status", status.getStatus());
                    }

                    for (final ObjectObjectCursor<String, List<AliasMetaData>> entry : response.getAliases()) {
                        builder.startObject(entry.key);
                        {
                            builder.startObject("aliases");
                            {
                                for (final AliasMetaData alias : entry.value) {
                                    AliasMetaData.Builder.toXContent(alias, builder, ToXContent.EMPTY_PARAMS);
                                }
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                }
                builder.endObject();
                return new BytesRestResponse(status, builder);
            }

        });
    }

}
