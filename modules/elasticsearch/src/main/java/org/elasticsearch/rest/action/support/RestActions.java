/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.rest.action.support;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.index.query.json.JsonQueryBuilders;
import org.elasticsearch.index.query.json.QueryStringJsonQueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * @author kimchy (Shay Banon)
 */
public class RestActions {

    public final static Pattern indicesPattern = Pattern.compile(",");
    public final static Pattern typesPattern = Pattern.compile(",");
    public final static Pattern nodesIdsPattern = Pattern.compile(",");

    public static void buildBroadcastShardsHeader(JsonBuilder builder, BroadcastOperationResponse response) throws IOException {
        builder.startObject("_shards");
        builder.field("total", response.totalShards());
        builder.field("successful", response.successfulShards());
        builder.field("failed", response.failedShards());
        if (!response.shardFailures().isEmpty()) {
            builder.startArray("failures");
            for (ShardOperationFailedException shardFailure : response.shardFailures()) {
                builder.startObject();
                if (shardFailure.index() != null) {
                    builder.field("index", shardFailure.index());
                }
                if (shardFailure.shardId() != -1) {
                    builder.field("shardId", shardFailure.shardId());
                }
                builder.field("reason", shardFailure.reason());
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
    }

    public static SearchType parseSearchType(String searchType) {
        if (searchType == null) {
            return SearchType.DEFAULT;
        }
        if ("dfs_query_then_fetch".equals(searchType)) {
            return SearchType.DFS_QUERY_THEN_FETCH;
        } else if ("dfs_query_and_fetch".equals(searchType)) {
            return SearchType.DFS_QUERY_AND_FETCH;
        } else if ("query_then_fetch".equals(searchType)) {
            return SearchType.QUERY_THEN_FETCH;
        } else if ("query_and_fetch".equals(searchType)) {
            return SearchType.QUERY_AND_FETCH;
        } else {
            throw new ElasticSearchIllegalArgumentException("No search type for [" + searchType + "]");
        }
    }

    public static byte[] parseQuerySource(RestRequest request) {
        if (request.hasContent()) {
            return request.contentAsBytes();
        }
        String queryString = request.param("q");
        if (queryString == null) {
            throw new ElasticSearchIllegalArgumentException("No query to execute, not in body, and not bounded to 'q' parameter");
        }
        QueryStringJsonQueryBuilder queryBuilder = JsonQueryBuilders.queryString(queryString);
        queryBuilder.defaultField(request.param("df"));
        queryBuilder.analyzer(request.param("analyzer"));
        String defaultOperator = request.param("defaultOperator");
        if (defaultOperator != null) {
            if ("OR".equals(defaultOperator)) {
                queryBuilder.defualtOperator(QueryStringJsonQueryBuilder.Operator.OR);
            } else if ("AND".equals(defaultOperator)) {
                queryBuilder.defualtOperator(QueryStringJsonQueryBuilder.Operator.AND);
            } else {
                throw new ElasticSearchIllegalArgumentException("Unsupported defaultOperator [" + defaultOperator + "], can either be [OR] or [AND]");
            }
        }
        return queryBuilder.buildAsBytes();
    }

    public static String[] splitIndices(String indices) {
        if (indices == null) {
            return Strings.EMPTY_ARRAY;
        }
        return indicesPattern.split(indices);
    }

    public static String[] splitTypes(String typeNames) {
        return typesPattern.split(typeNames);
    }

    public static String[] splitNodes(String nodes) {
        if (nodes == null) {
            return Strings.EMPTY_ARRAY;
        }
        return nodesIdsPattern.split(nodes);
    }
}
