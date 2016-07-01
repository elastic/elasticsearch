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

package org.elasticsearch.rest.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class RestActions {

    public static long parseVersion(RestRequest request) {
        if (request.hasParam("version")) {
            return request.paramAsLong("version", Versions.MATCH_ANY);
        }
        String ifMatch = request.header("If-Match");
        if (ifMatch != null) {
            return Long.parseLong(ifMatch);
        }
        return Versions.MATCH_ANY;
    }

    public static long parseVersion(RestRequest request, long defaultVersion) {
        long version = parseVersion(request);
        return (version == Versions.MATCH_ANY) ? defaultVersion : version;
    }

    public static void buildBroadcastShardsHeader(XContentBuilder builder, Params params, BroadcastResponse response) throws IOException {
        buildBroadcastShardsHeader(builder, params,
                                   response.getTotalShards(), response.getSuccessfulShards(), response.getFailedShards(),
                                   response.getShardFailures());
    }

    public static void buildBroadcastShardsHeader(XContentBuilder builder, Params params,
                                                  int total, int successful, int failed,
                                                  ShardOperationFailedException[] shardFailures) throws IOException {
        builder.startObject("_shards");
        builder.field("total", total);
        builder.field("successful", successful);
        builder.field("failed", failed);
        if (shardFailures != null && shardFailures.length > 0) {
            builder.startArray("failures");
            final boolean group = params.paramAsBoolean("group_shard_failures", true); // we group by default
            for (ShardOperationFailedException shardFailure : group ? ExceptionsHelper.groupBy(shardFailures) : shardFailures) {
                builder.startObject();
                shardFailure.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
    }
    /**
     * Create the XContent header for any {@link BaseNodesResponse}.
     *
     * @param builder XContent builder.
     * @param params XContent parameters.
     * @param response The response containing individual, node-level responses.
     * @see #buildNodesHeader(XContentBuilder, Params, int, int, int, List)
     */
    public static <NodeResponse extends BaseNodeResponse> void buildNodesHeader(final XContentBuilder builder, final Params params,
                                                                                final BaseNodesResponse<NodeResponse> response)
            throws IOException {
        final int successful = response.getNodes().size();
        final int failed = response.failures().size();

        buildNodesHeader(builder, params, successful + failed, successful, failed, response.failures());
    }

    /**
     * Create the XContent header for any {@link BaseNodesResponse}. This looks like:
     * <code>
     * "_nodes" : {
     *   "total" : 3,
     *   "successful" : 1,
     *   "failed" : 2,
     *   "failures" : [ { ... }, { ... } ]
     * }
     * </code>
     * Prefer the overload that properly invokes this method to calling this directly.
     *
     * @param builder XContent builder.
     * @param params XContent parameters.
     * @param total The total number of nodes touched.
     * @param successful The successful number of responses received.
     * @param failed The number of failures (effectively {@code total - successful}).
     * @param failures The failure exceptions related to {@code failed}.
     * @see #buildNodesHeader(XContentBuilder, Params, BaseNodesResponse)
     */
    public static void buildNodesHeader(final XContentBuilder builder, final Params params,
                                        final int total, final int successful, final int failed,
                                        final List<FailedNodeException> failures) throws IOException {
        builder.startObject("_nodes");
        builder.field("total", total);
        builder.field("successful", successful);
        builder.field("failed", failed);

        if (failures.isEmpty() == false) {
            builder.startArray("failures");
            for (FailedNodeException failure : failures) {
                builder.startObject();
                failure.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }

        builder.endObject();
    }

    /**
     * Automatically transform the {@link ToXContent}-compatible, nodes-level {@code response} into a a {@link BytesRestResponse}.
     * <p>
     * This looks like:
     * <code>
     * {
     *   "_nodes" : { ... },
     *   "cluster_name" : "...",
     *   ...
     * }
     * </code>
     *
     * @param builder XContent builder.
     * @param params XContent parameters.
     * @param response The nodes-level (plural) response.
     * @return Never {@code null}.
     * @throws IOException if building the response causes an issue
     */
    public static <NodesResponse extends BaseNodesResponse & ToXContent> BytesRestResponse nodesResponse(final XContentBuilder builder,
                                                                                                         final Params params,
                                                                                                         final NodesResponse response)
            throws IOException {
        builder.startObject();
        RestActions.buildNodesHeader(builder, params, response);
        builder.field("cluster_name", response.getClusterName().value());
        response.toXContent(builder, params);
        builder.endObject();

        return new BytesRestResponse(RestStatus.OK, builder);
    }

    public static QueryBuilder urlParamsToQueryBuilder(RestRequest request) {
        String queryString = request.param("q");
        if (queryString == null) {
            return null;
        }
        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(queryString);
        queryBuilder.defaultField(request.param("df"));
        queryBuilder.analyzer(request.param("analyzer"));
        queryBuilder.analyzeWildcard(request.paramAsBoolean("analyze_wildcard", false));
        queryBuilder.lowercaseExpandedTerms(request.paramAsBoolean("lowercase_expanded_terms", true));
        queryBuilder.lenient(request.paramAsBoolean("lenient", null));
        String defaultOperator = request.param("default_operator");
        if (defaultOperator != null) {
            queryBuilder.defaultOperator(Operator.fromString(defaultOperator));
        }
        return queryBuilder;
    }

    /**
     * Get Rest content from either payload or source parameter
     * @param request Rest request
     * @return rest content
     */
    public static BytesReference getRestContent(RestRequest request) {
        assert request != null;

        BytesReference content = request.content();
        if (!request.hasContent()) {
            String source = request.param("source");
            if (source != null) {
                content = new BytesArray(source);
            }
        }

        return content;
    }

    public static QueryBuilder getQueryContent(BytesReference source, IndicesQueriesRegistry indicesQueriesRegistry,
                                               ParseFieldMatcher parseFieldMatcher) {
        try (XContentParser requestParser = XContentFactory.xContent(source).createParser(source)) {
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, requestParser, parseFieldMatcher);
            return context.parseTopLevelQueryBuilder();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to parse source", e);
        }
    }

    /**
     * guesses the content type from either payload or source parameter
     * @param request Rest request
     * @return rest content type or <code>null</code> if not applicable.
     */
    public static XContentType guessBodyContentType(final RestRequest request) {
        final BytesReference restContent = RestActions.getRestContent(request);
        if (restContent == null) {
            return null;
        }
        return XContentFactory.xContentType(restContent);
    }

    /**
     * Returns <code>true</code> if either payload or source parameter is present. Otherwise <code>false</code>
     */
    public static boolean hasBodyContent(final RestRequest request) {
        return request.hasContent() || request.hasParam("source");
    }

    /**
     * {@code NodesResponseRestBuilderListener} automatically translates any {@link BaseNodesResponse} (multi-node) response that is
     * {@link ToXContent}-compatible into a {@link RestResponse} with the necessary header info (e.g., "cluster_name").
     * <p>
     * This is meant to avoid a slew of anonymous classes doing (or worse):
     * <code>
     * client.admin().cluster().request(nodesRequest, new RestBuilderListener&lt;NodesResponse&gt;(channel) {
     *     public RestResponse buildResponse(NodesResponse response, XContentBuilder builder) throws Exception {
     *         return RestActions.nodesResponse(builder, ToXContent.EMPTY_PARAMS, response);
     *     }
     * });
     * </code>
     */
    public static class NodesResponseRestListener<NodesResponse extends BaseNodesResponse & ToXContent>
        extends RestBuilderListener<NodesResponse> {

        public NodesResponseRestListener(RestChannel channel) {
            super(channel);
        }

        @Override
        public RestResponse buildResponse(NodesResponse response, XContentBuilder builder) throws Exception {
            return RestActions.nodesResponse(builder, channel.request(), response);
        }

    }

}
