/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.rest.action.admin.indices.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 */
public class RestIndicesStatsAction extends BaseRestHandler {

    @Inject
    public RestIndicesStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_stats", this);
        controller.registerHandler(GET, "/{index}/_stats", this);

        controller.registerHandler(GET, "_stats/docs", new RestDocsStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/docs", new RestDocsStatsHandler());

        controller.registerHandler(GET, "/_stats/store", new RestStoreStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/store", new RestStoreStatsHandler());

        controller.registerHandler(GET, "/_stats/indexing", new RestIndexingStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/indexing", new RestIndexingStatsHandler());
        controller.registerHandler(GET, "/_stats/indexing/{indexingTypes1}", new RestIndexingStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/indexing/{indexingTypes2}", new RestIndexingStatsHandler());

        controller.registerHandler(GET, "/_stats/search", new RestSearchStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/search", new RestSearchStatsHandler());
        controller.registerHandler(GET, "/_stats/search/{searchGroupsStats1}", new RestSearchStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/search/{searchGroupsStats2}", new RestSearchStatsHandler());

        controller.registerHandler(GET, "/_stats/get", new RestGetStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/get", new RestGetStatsHandler());

        controller.registerHandler(GET, "/_stats/refresh", new RestRefreshStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/refresh", new RestRefreshStatsHandler());

        controller.registerHandler(GET, "/_stats/merge", new RestMergeStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/merge", new RestMergeStatsHandler());

        controller.registerHandler(GET, "/_stats/flush", new RestFlushStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/flush", new RestFlushStatsHandler());

        controller.registerHandler(GET, "/_stats/warmer", new RestWarmerStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/warmer", new RestWarmerStatsHandler());

        controller.registerHandler(GET, "/_stats/filter_cache", new RestFilterCacheStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/filter_cache", new RestFilterCacheStatsHandler());

        controller.registerHandler(GET, "/_stats/id_cache", new RestIdCacheStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/id_cache", new RestIdCacheStatsHandler());

        controller.registerHandler(GET, "/_stats/fielddata", new RestFieldDataStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/fielddata", new RestFieldDataStatsHandler());
        controller.registerHandler(GET, "/_stats/fielddata/{fields}", new RestFieldDataStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/fielddata/{fields}", new RestFieldDataStatsHandler());

        controller.registerHandler(GET, "/_stats/completion", new RestCompletionStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/completion", new RestCompletionStatsHandler());
        controller.registerHandler(GET, "/_stats/completion/{fields}", new RestCompletionStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/completion/{fields}", new RestCompletionStatsHandler());
        controller.registerHandler(GET, "/_stats/segments", new RestSegmentsStatsHandler());
        controller.registerHandler(GET, "/{index}/_stats/segments", new RestSegmentsStatsHandler());
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.listenerThreaded(false);
        if (request.hasParam("ignore_indices")) {
            indicesStatsRequest.ignoreIndices(IgnoreIndices.fromString(request.param("ignore_indices")));
        }
        boolean clear = request.paramAsBoolean("clear", false);
        if (clear) {
            indicesStatsRequest.clear();
        }
        boolean all = request.paramAsBoolean("all", false);
        if (all) {
            indicesStatsRequest.all();
        }
        indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));
        if (request.hasParam("groups")) {
            indicesStatsRequest.groups(Strings.splitStringByCommaToArray(request.param("groups")));
        }
        /* We use "fields" as the default field list for stats that support field inclusion filters and further down
         * a more specific list of fields that overrides this list.*/
        final String[] defaultIncludedFields = request.paramAsStringArray("fields", null);
        indicesStatsRequest.docs(request.paramAsBoolean("docs", indicesStatsRequest.docs()));
        indicesStatsRequest.store(request.paramAsBoolean("store", indicesStatsRequest.store()));
        indicesStatsRequest.indexing(request.paramAsBoolean("indexing", indicesStatsRequest.indexing()));
        indicesStatsRequest.search(request.paramAsBoolean("search", indicesStatsRequest.search()));
        indicesStatsRequest.get(request.paramAsBoolean("get", indicesStatsRequest.get()));
        indicesStatsRequest.merge(request.paramAsBoolean("merge", indicesStatsRequest.merge()));
        indicesStatsRequest.refresh(request.paramAsBoolean("refresh", indicesStatsRequest.refresh()));
        indicesStatsRequest.flush(request.paramAsBoolean("flush", indicesStatsRequest.flush()));
        indicesStatsRequest.warmer(request.paramAsBoolean("warmer", indicesStatsRequest.warmer()));
        indicesStatsRequest.filterCache(request.paramAsBoolean("filter_cache", indicesStatsRequest.filterCache()));
        indicesStatsRequest.idCache(request.paramAsBoolean("id_cache", indicesStatsRequest.idCache()));
        indicesStatsRequest.fieldData(request.paramAsBoolean("fielddata", indicesStatsRequest.fieldData()));
        indicesStatsRequest.fieldDataFields(request.paramAsStringArray("fielddata_fields", defaultIncludedFields));
        indicesStatsRequest.segments(request.paramAsBoolean("segments", indicesStatsRequest.segments()));
        indicesStatsRequest.completion(request.paramAsBoolean("completion", indicesStatsRequest.completion()));
        indicesStatsRequest.completionFields(request.paramAsStringArray("completion_fields", defaultIncludedFields));

        client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
            @Override
            public void onResponse(IndicesStatsResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("ok", true);
                    buildBroadcastShardsHeader(builder, response);
                    response.toXContent(builder, request);
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

    class RestDocsStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().docs(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestStoreStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().store(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestIndexingStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().indexing(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            if (request.hasParam("types")) {
                indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));
            } else if (request.hasParam("indexingTypes1")) {
                indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("indexingTypes1")));
            } else if (request.hasParam("indexingTypes2")) {
                indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("indexingTypes2")));
            }

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestSearchStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().search(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            if (request.hasParam("groups")) {
                indicesStatsRequest.groups(Strings.splitStringByCommaToArray(request.param("groups")));
            } else if (request.hasParam("searchGroupsStats1")) {
                indicesStatsRequest.groups(Strings.splitStringByCommaToArray(request.param("searchGroupsStats1")));
            } else if (request.hasParam("searchGroupsStats2")) {
                indicesStatsRequest.groups(Strings.splitStringByCommaToArray(request.param("searchGroupsStats2")));
            }

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestGetStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().get(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestMergeStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().merge(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestFlushStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().flush(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestWarmerStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().warmer(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestFilterCacheStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().filterCache(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestIdCacheStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().idCache(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestFieldDataStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().fieldData(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));
            indicesStatsRequest.fieldDataFields(request.paramAsStringArray("fields", null));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestCompletionStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().completion(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));
            indicesStatsRequest.completionFields(request.paramAsStringArray("fields", null));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestRefreshStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().refresh(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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

    class RestSegmentsStatsHandler implements RestHandler {

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
            indicesStatsRequest.listenerThreaded(false);
            indicesStatsRequest.clear().segments(true);
            indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));

            client.admin().indices().stats(indicesStatsRequest, new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();
                        builder.field("ok", true);
                        buildBroadcastShardsHeader(builder, response);
                        response.toXContent(builder, request);
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
}
