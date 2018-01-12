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

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * The REST handler for get index and head index APIs.
 */
public class RestGetIndicesAction extends BaseRestHandler {

    private final IndexScopedSettings indexScopedSettings;
    private final SettingsFilter settingsFilter;

    public RestGetIndicesAction(
            final Settings settings,
            final RestController controller,
            final IndexScopedSettings indexScopedSettings,
            final SettingsFilter settingsFilter) {
        super(settings);
        this.indexScopedSettings = indexScopedSettings;
        controller.registerHandler(GET, "/{index}", this);
        controller.registerHandler(HEAD, "/{index}", this);
        this.settingsFilter = settingsFilter;
    }

    @Override
    public String getName() {
        return "get_indices_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indices);
        getIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, getIndexRequest.indicesOptions()));
        getIndexRequest.local(request.paramAsBoolean("local", getIndexRequest.local()));
        getIndexRequest.humanReadable(request.paramAsBoolean("human", false));
        final boolean defaults = request.paramAsBoolean("include_defaults", false);
        return channel -> client.admin().indices().getIndex(getIndexRequest, new RestBuilderListener<GetIndexResponse>(channel) {

            @Override
            public RestResponse buildResponse(final GetIndexResponse response, final XContentBuilder builder) throws Exception {
                builder.startObject();
                {
                    for (final String index : response.indices()) {
                        builder.startObject(index);
                        {
                            for (final Feature feature : getIndexRequest.features()) {
                                switch (feature) {
                                    case ALIASES:
                                        writeAliases(response.aliases().get(index), builder, request);
                                        break;
                                    case MAPPINGS:
                                        writeMappings(response.mappings().get(index), builder);
                                        break;
                                    case SETTINGS:
                                        writeSettings(response.settings().get(index), builder, request, defaults);
                                        break;
                                    default:
                                        throw new IllegalStateException("feature [" + feature + "] is not valid");
                                }
                            }
                        }
                        builder.endObject();

                    }
                }
                builder.endObject();

                return new BytesRestResponse(OK, builder);
            }

            private void writeAliases(
                    final List<AliasMetaData> aliases,
                    final XContentBuilder builder,
                    final Params params) throws IOException {
                builder.startObject("aliases");
                {
                    if (aliases != null) {
                        for (final AliasMetaData alias : aliases) {
                            AliasMetaData.Builder.toXContent(alias, builder, params);
                        }
                    }
                }
                builder.endObject();
            }

            private void writeMappings(final ImmutableOpenMap<String, MappingMetaData> mappings, final XContentBuilder builder)
                    throws IOException {
                builder.startObject("mappings");
                {
                    if (mappings != null) {
                        for (final ObjectObjectCursor<String, MappingMetaData> typeEntry : mappings) {
                            builder.field(typeEntry.key);
                            builder.map(typeEntry.value.sourceAsMap());
                        }
                    }
                }
                builder.endObject();
            }

            private void writeSettings(
                    final Settings settings,
                    final XContentBuilder builder,
                    final Params params,
                    final boolean defaults) throws IOException {
                builder.startObject("settings");
                {
                    settings.toXContent(builder, params);
                }
                builder.endObject();
                if (defaults) {
                    builder.startObject("defaults");
                    {
                        settingsFilter
                                .filter(indexScopedSettings.diff(settings, RestGetIndicesAction.this.settings))
                                .toXContent(builder, request);
                    }
                    builder.endObject();
                }
            }

        });
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

}
