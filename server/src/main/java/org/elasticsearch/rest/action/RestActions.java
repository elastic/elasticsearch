/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class RestActions {

    public static final ParseField _SHARDS_FIELD = new ParseField("_shards");
    public static final ParseField TOTAL_FIELD = new ParseField("total");
    public static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
    public static final ParseField SKIPPED_FIELD = new ParseField("skipped");
    public static final ParseField FAILED_FIELD = new ParseField("failed");
    public static final ParseField FAILURES_FIELD = new ParseField("failures");

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
                                   response.getTotalShards(), response.getSuccessfulShards(), -1, response.getFailedShards(),
                                   response.getShardFailures());
    }

    public static void buildBroadcastShardsHeader(XContentBuilder builder, Params params,
                                                  int total, int successful, int skipped, int failed,
                                                  ShardOperationFailedException[] shardFailures) throws IOException {
        builder.startObject(_SHARDS_FIELD.getPreferredName());
        builder.field(TOTAL_FIELD.getPreferredName(), total);
        builder.field(SUCCESSFUL_FIELD.getPreferredName(), successful);
        if (skipped >= 0) {
            builder.field(SKIPPED_FIELD.getPreferredName(), skipped);
        }
        builder.field(FAILED_FIELD.getPreferredName(), failed);
        if (CollectionUtils.isEmpty(shardFailures) == false) {
            builder.startArray(FAILURES_FIELD.getPreferredName());
            for (ShardOperationFailedException shardFailure : ExceptionsHelper.groupBy(shardFailures)) {
                shardFailure.toXContent(builder, params);
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
        queryBuilder.lenient(request.paramAsBoolean("lenient", null));
        String defaultOperator = request.param("default_operator");
        if (defaultOperator != null) {
            queryBuilder.defaultOperator(Operator.fromString(defaultOperator));
        }
        return queryBuilder;
    }

    public static QueryBuilder getQueryContent(XContentParser requestParser) {
        return parseTopLevelQueryBuilder("query", requestParser);
    }

    public static QueryBuilder getQueryContent(String fieldName, XContentParser requestParser) {
        return parseTopLevelQueryBuilder(fieldName, requestParser);
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

    /**
     * Parses a top level query including the query element that wraps it
     */
    private static QueryBuilder parseTopLevelQueryBuilder(String fieldName, XContentParser parser) {
        try {
            QueryBuilder queryBuilder = null;
            XContentParser.Token first = parser.nextToken();
            if (first == null) {
                return null;
            } else if (first != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(
                    parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT +
                    "] but found [" + first + "]", parser.getTokenLocation()
                );
            }
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentName = parser.currentName();
                    if (fieldName.equals(currentName)) {
                        queryBuilder = parseInnerQueryBuilder(parser);
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "request does not support [" + parser.currentName() + "]");
                    }
                }
            }
            return queryBuilder;
        } catch (ParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new ParsingException(parser == null ? null : parser.getTokenLocation(), "Failed to parse", e);
        }
    }

}
