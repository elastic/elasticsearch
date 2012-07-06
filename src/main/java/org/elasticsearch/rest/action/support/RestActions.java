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

package org.elasticsearch.rest.action.support;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 *
 */
public class RestActions {

    public static long parseVersion(RestRequest request) {
        if (request.hasParam("version")) {
            return request.paramAsLong("version", 0);
        }
        String ifMatch = request.header("If-Match");
        if (ifMatch != null) {
            return Long.parseLong(ifMatch);
        }
        return 0;
    }

    public static void buildBroadcastShardsHeader(XContentBuilder builder, BroadcastOperationResponse response) throws IOException {
        builder.startObject("_shards");
        builder.field("total", response.totalShards());
        builder.field("successful", response.successfulShards());
        builder.field("failed", response.failedShards());
        if (!response.shardFailures().isEmpty()) {
            builder.startArray("failures");
            for (ShardOperationFailedException shardFailure : response.shardFailures()) {
                builder.startObject();
                if (shardFailure.index() != null) {
                    builder.field("index", shardFailure.index(), XContentBuilder.FieldCaseConversion.NONE);
                }
                if (shardFailure.shardId() != -1) {
                    builder.field("shard", shardFailure.shardId());
                }
                builder.field("reason", shardFailure.reason());
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
    }

    public static BytesReference parseQuerySource(RestRequest request) {
        String queryString = request.param("q");
        if (queryString == null) {
            return null;
        }
        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryString(queryString);
        queryBuilder.defaultField(request.param("df"));
        queryBuilder.analyzer(request.param("analyzer"));
        String defaultOperator = request.param("default_operator");
        if (defaultOperator != null) {
            if ("OR".equals(defaultOperator)) {
                queryBuilder.defaultOperator(QueryStringQueryBuilder.Operator.OR);
            } else if ("AND".equals(defaultOperator)) {
                queryBuilder.defaultOperator(QueryStringQueryBuilder.Operator.AND);
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
        return Strings.splitStringByCommaToArray(indices);
    }

    public static String[] splitTypes(String typeNames) {
        if (typeNames == null) {
            return Strings.EMPTY_ARRAY;
        }
        return Strings.splitStringByCommaToArray(typeNames);
    }

    public static String[] splitNodes(String nodes) {
        if (nodes == null) {
            return Strings.EMPTY_ARRAY;
        }
        return Strings.splitStringByCommaToArray(nodes);
    }
}
