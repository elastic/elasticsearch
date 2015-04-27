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
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
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
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
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
        String[] featureParams = request.paramAsStringArray("type", null);
        // Work out if the indices is a list of features
        if (featureParams == null && indices.length > 0 && indices[0] != null && indices[0].startsWith("_") && !"_all".equals(indices[0])) {
            featureParams = indices;
            indices = new String[] {"_all"};
        }
        final GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indices);
        if (featureParams != null) {
            Feature[] features = Feature.convertToFeatures(featureParams);
            getIndexRequest.features(features);
        }
        getIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, getIndexRequest.indicesOptions()));
        getIndexRequest.local(request.paramAsBoolean("local", getIndexRequest.local()));
        client.admin().indices().getIndex(getIndexRequest, new RestBuilderListener<GetIndexResponse>(channel) {

            @Override
            public RestResponse buildResponse(GetIndexResponse response, XContentBuilder builder) throws Exception {

                Feature[] features = getIndexRequest.featuresAsEnums();
                String[] indices = response.indices();

                builder.startObject();
                for (String index : indices) {
                    builder.startObject(index);
                    for (Feature feature : features) {
                        switch (feature) {
                        case ALIASES:
                            writeAliases(response.aliases().get(index), builder, request);
                            break;
                        case MAPPINGS:
                            writeMappings(response.mappings().get(index), builder, request);
                            break;
                        case SETTINGS:
                            writeSettings(response.settings().get(index), builder, request);
                            break;
                        case WARMERS:
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
                builder.startObject(Fields.ALIASES);
                if (aliases != null) {
                    for (AliasMetaData alias : aliases) {
                        AliasMetaData.Builder.toXContent(alias, builder, params);
                    }
                }
                builder.endObject();
            }

            private void writeMappings(ImmutableOpenMap<String, MappingMetaData> mappings, XContentBuilder builder, Params params) throws IOException {
                builder.startObject(Fields.MAPPINGS);
                if (mappings != null) {
                    for (ObjectObjectCursor<String, MappingMetaData> typeEntry : mappings) {
                        builder.field(typeEntry.key);
                        builder.map(typeEntry.value.sourceAsMap());
                    }
                }
                builder.endObject();
            }

            private void writeSettings(Settings settings, XContentBuilder builder, Params params) throws IOException {
                builder.startObject(Fields.SETTINGS);
                settings.toXContent(builder, params);
                builder.endObject();
            }

            private void writeWarmers(ImmutableList<IndexWarmersMetaData.Entry> warmers, XContentBuilder builder, Params params) throws IOException {
                builder.startObject(Fields.WARMERS);
                if (warmers != null) {
                    for (IndexWarmersMetaData.Entry warmer : warmers) {
                        IndexWarmersMetaData.FACTORY.toXContent(warmer, builder, params);
                    }
                }
                builder.endObject();
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