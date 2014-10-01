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
package org.elasticsearch.rest.action.admin.indices.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetIndicesAction extends BaseRestHandler {

    @Inject
    public RestGetIndicesAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}", this);
        controller.registerHandler(GET, "/{index}/{type}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] features = request.paramAsStringArray("type", null);
        // Work out if the indices is a list of features
        if (features == null && indices.length > 0 && indices[0] != null && indices[0].startsWith("_") && !"_all".equals(indices[0])) {
            features = indices;
            indices = new String[] {"_all"};
        }
        final GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indices);
        if (features != null) {
            getIndexRequest.features(features);
        }
        // The order of calls to the request is important here. We must set the indices and features before 
        // we call getIndexRequest.indicesOptions(); or we might get the wrong default indices options
        IndicesOptions defaultIndicesOptions = getIndexRequest.indicesOptions();
        getIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, defaultIndicesOptions));
        getIndexRequest.local(request.paramAsBoolean("local", getIndexRequest.local()));
        client.admin().indices().getIndex(getIndexRequest, new RestBuilderListener<GetIndexResponse>(channel) {

            @Override
            public RestResponse buildResponse(GetIndexResponse response, XContentBuilder builder) throws Exception {

                String[] features = getIndexRequest.features();
                String[] indices = response.indices();

                builder.startObject();
                for (String index : indices) {
                    builder.startObject(index);
                    for (String feature : features) {
                        switch (feature) {
                        case "_alias":
                        case "_aliases":
                            writeAliases(response.aliases().get(index), builder, request);
                            break;
                        case "_mapping":
                        case "_mappings":
                            writeMappings(response.mappings().get(index), builder, request);
                            break;
                        case "_settings":
                            writeSettings(response.settings().get(index), builder, request);
                            break;
                        case "_warmer":
                        case "_warmers":
                            writeWarmers(response.warmers().get(index), builder, request);
                            break;
                        default:
                            throw new ElasticsearchIllegalStateException("feature [" + feature + "] is not valid");
                        }
                    }
                    builder.endObject();

                }
                builder.endObject();

                return new BytesRestResponse(OK, builder);
            }

            private void writeAliases(ImmutableList<AliasMetaData> aliases, XContentBuilder builder, Params params) throws IOException {
                if (aliases != null) {
                    builder.startObject(Fields.ALIASES);
                    for (AliasMetaData alias : aliases) {
                        AliasMetaData.Builder.toXContent(alias, builder, params);
                    }
                    builder.endObject();
                }
            }

            private void writeMappings(ImmutableOpenMap<String, MappingMetaData> mappings, XContentBuilder builder, Params params) throws IOException {
                if (mappings != null) {
                    builder.startObject(Fields.MAPPINGS);
                    for (ObjectObjectCursor<String, MappingMetaData> typeEntry : mappings) {
                        builder.field(typeEntry.key);
                        builder.map(typeEntry.value.sourceAsMap());
                    }
                    builder.endObject();
                }
            }

            private void writeSettings(Settings settings, XContentBuilder builder, Params params) throws IOException {
                builder.startObject(Fields.SETTINGS);
                settings.toXContent(builder, params);
                builder.endObject();
            }

            private void writeWarmers(ImmutableList<IndexWarmersMetaData.Entry> warmers, XContentBuilder builder, Params params) throws IOException {
                if (warmers != null) {
                    builder.startObject(Fields.WARMERS);
                    for (IndexWarmersMetaData.Entry warmer : warmers) {
                        IndexWarmersMetaData.FACTORY.toXContent(warmer, builder, params);
                    }
                    builder.endObject();
                }
            }
        });
    }

    static class Fields {
        static final XContentBuilderString ALIASES = new XContentBuilderString("aliases");
        static final XContentBuilderString MAPPINGS = new XContentBuilderString("mappings");
        static final XContentBuilderString SETTINGS = new XContentBuilderString("settings");
        static final XContentBuilderString WARMERS = new XContentBuilderString("warmers");
    }

}