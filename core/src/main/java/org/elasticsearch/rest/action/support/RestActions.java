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
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

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

    static final class Fields {
        static final XContentBuilderString _SHARDS = new XContentBuilderString("_shards");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString SUCCESSFUL = new XContentBuilderString("successful");
        static final XContentBuilderString FAILED = new XContentBuilderString("failed");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
    }

    public static void buildBroadcastShardsHeader(XContentBuilder builder, ToXContent.Params params, BroadcastResponse response) throws IOException {
        buildBroadcastShardsHeader(builder, params, response.getTotalShards(), response.getSuccessfulShards(), response.getFailedShards(), response.getShardFailures());
    }

    public static void buildBroadcastShardsHeader(XContentBuilder builder, ToXContent.Params params, int total, int successful, int failed, ShardOperationFailedException[] shardFailures) throws IOException {
        builder.startObject(Fields._SHARDS);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.SUCCESSFUL, successful);
        builder.field(Fields.FAILED, failed);
        if (shardFailures != null && shardFailures.length > 0) {
            builder.startArray(Fields.FAILURES);
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

    public static QueryBuilder<?> urlParamsToQueryBuilder(RestRequest request) {
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

    public static SearchSourceBuilder getRestSearchSource(BytesReference sourceBytes, IndicesQueriesRegistry queryRegistry,
            ParseFieldMatcher parseFieldMatcher)
            throws IOException {
        XContentParser parser = XContentFactory.xContent(sourceBytes).createParser(sourceBytes);
        QueryParseContext queryParseContext = new QueryParseContext(queryRegistry);
        queryParseContext.reset(parser);
        queryParseContext.parseFieldMatcher(parseFieldMatcher);
        SearchSourceBuilder source = SearchSourceBuilder.parseSearchSource(parser, queryParseContext);
        return source;
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

    public static QueryBuilder<?> getQueryContent(BytesReference source, QueryParseContext context) {
        try (XContentParser requestParser = XContentFactory.xContent(source).createParser(source)) {
            // Save the parseFieldMatcher because its about to be trashed in the
            // QueryParseContext
            ParseFieldMatcher parseFieldMatcher = context.parseFieldMatcher();
            context.reset(requestParser);
            context.parseFieldMatcher(parseFieldMatcher);
            return context.parseInnerQueryBuilder();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to parse source", e);
        } finally {
            context.reset(null);
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
}
