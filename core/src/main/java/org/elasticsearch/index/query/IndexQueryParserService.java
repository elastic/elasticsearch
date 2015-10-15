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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;

public class IndexQueryParserService extends AbstractIndexComponent {

    public static final String DEFAULT_FIELD = "index.query.default_field";
    public static final String QUERY_STRING_LENIENT = "index.query_string.lenient";
    public static final String QUERY_STRING_ANALYZE_WILDCARD = "indices.query.query_string.analyze_wildcard";
    public static final String QUERY_STRING_ALLOW_LEADING_WILDCARD = "indices.query.query_string.allowLeadingWildcard";
    public static final String PARSE_STRICT = "index.query.parse.strict";
    public static final String ALLOW_UNMAPPED = "index.query.parse.allow_unmapped_fields";
    private final InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    private CloseableThreadLocal<QueryShardContext> cache = new CloseableThreadLocal<QueryShardContext>() {
        @Override
        protected QueryShardContext initialValue() {
            return new QueryShardContext(index, IndexQueryParserService.this);
        }
    };

    final AnalysisService analysisService;

    final ScriptService scriptService;

    final MapperService mapperService;

    final SimilarityService similarityService;

    final IndexCache indexCache;

    protected IndexFieldDataService fieldDataService;

    final ClusterService clusterService;

    final IndexNameExpressionResolver indexNameExpressionResolver;

    final BitsetFilterCache bitsetFilterCache;

    private final IndicesQueriesRegistry indicesQueriesRegistry;

    private final String defaultField;
    private final boolean queryStringLenient;
    private final boolean queryStringAnalyzeWildcard;
    private final boolean queryStringAllowLeadingWildcard;
    private final ParseFieldMatcher parseFieldMatcher;
    private final boolean defaultAllowUnmappedFields;
    private final Client client;

    @Inject
    public IndexQueryParserService(Index index, @IndexSettings Settings indexSettings, Settings settings,
                                   IndicesQueriesRegistry indicesQueriesRegistry,
                                   ScriptService scriptService, AnalysisService analysisService,
                                   MapperService mapperService, IndexCache indexCache, IndexFieldDataService fieldDataService,
                                   BitsetFilterCache bitsetFilterCache,
                                   @Nullable SimilarityService similarityService, ClusterService clusterService,
                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                   InnerHitsQueryParserHelper innerHitsQueryParserHelper, Client client) {
        super(index, indexSettings);
        this.scriptService = scriptService;
        this.analysisService = analysisService;
        this.mapperService = mapperService;
        this.similarityService = similarityService;
        this.indexCache = indexCache;
        this.fieldDataService = fieldDataService;
        this.bitsetFilterCache = bitsetFilterCache;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        this.defaultField = indexSettings.get(DEFAULT_FIELD, AllFieldMapper.NAME);
        this.queryStringLenient = indexSettings.getAsBoolean(QUERY_STRING_LENIENT, false);
        this.queryStringAnalyzeWildcard = settings.getAsBoolean(QUERY_STRING_ANALYZE_WILDCARD, false);
        this.queryStringAllowLeadingWildcard = settings.getAsBoolean(QUERY_STRING_ALLOW_LEADING_WILDCARD, true);
        this.parseFieldMatcher = new ParseFieldMatcher(indexSettings);
        this.defaultAllowUnmappedFields = indexSettings.getAsBoolean(ALLOW_UNMAPPED, true);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
        this.client = client;
    }

    public void close() {
        cache.close();
    }

    public String defaultField() {
        return this.defaultField;
    }

    public boolean queryStringAnalyzeWildcard() {
        return this.queryStringAnalyzeWildcard;
    }

    public boolean queryStringAllowLeadingWildcard() {
        return this.queryStringAllowLeadingWildcard;
    }

    public boolean queryStringLenient() {
        return this.queryStringLenient;
    }

    public IndicesQueriesRegistry indicesQueriesRegistry() {
        return indicesQueriesRegistry;
    }

    public ParsedQuery parse(BytesReference source) {
        QueryShardContext context = cache.get();
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            return innerParse(context, parser);
        } catch (ParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new ParsingException(parser == null ? null : parser.getTokenLocation(), "Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public ParsedQuery parse(XContentParser parser) {
        try {
            return innerParse(cache.get(), parser);
        } catch(IOException e) {
            throw new ParsingException(parser.getTokenLocation(), "Failed to parse", e);
        }
    }

    /**
     * Parses an inner filter, returning null if the filter should be ignored.
     */
    @Nullable
    public ParsedQuery parseInnerFilter(XContentParser parser) throws IOException {
        QueryShardContext context = cache.get();
        context.reset(parser);
        try {
            context.parseFieldMatcher(parseFieldMatcher);
            Query filter = context.parseContext().parseInnerQueryBuilder().toFilter(context);
            if (filter == null) {
                return null;
            }
            return new ParsedQuery(filter, context.copyNamedQueries());
        } finally {
            context.reset(null);
        }
    }

    public QueryShardContext getShardContext() {
        return cache.get();
    }

    public boolean defaultAllowUnmappedFields() {
        return defaultAllowUnmappedFields;
    }

    /**
     * @return The lowest node version in the cluster when the index was created or <code>null</code> if that was unknown
     */
    public Version getIndexCreatedVersion() {
        return Version.indexCreated(indexSettings);
    }

    /**
     * Selectively parses a query from a top level query or query_binary json field from the specified source.
     */
    public ParsedQuery parseQuery(BytesReference source) {
        XContentParser parser = null;
        try {
            parser = XContentHelper.createParser(source);
            ParsedQuery parsedQuery = null;
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    if ("query".equals(fieldName)) {
                        parsedQuery = parse(parser);
                    } else if ("query_binary".equals(fieldName) || "queryBinary".equals(fieldName)) {
                        byte[] querySource = parser.binaryValue();
                        XContentParser qSourceParser = XContentFactory.xContent(querySource).createParser(querySource);
                        parsedQuery = parse(qSourceParser);
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "request does not support [" + fieldName + "]");
                    }
                }
            }
            if (parsedQuery == null) {
                throw new ParsingException(parser.getTokenLocation(), "Required query is missing");
            }
            return parsedQuery;
        } catch (ParsingException | QueryShardException e) {
            throw e;
        } catch (Throwable e) {
            throw new ParsingException(parser == null ? null : parser.getTokenLocation(), "Failed to parse", e);
        }
    }

    private ParsedQuery innerParse(QueryShardContext context, XContentParser parser) throws IOException, QueryShardException {
        context.reset(parser);
        try {
            context.parseFieldMatcher(parseFieldMatcher);
            Query query = parseInnerQuery(context);
            return new ParsedQuery(query, context.copyNamedQueries());
        } finally {
            context.reset(null);
        }
    }

    public Query parseInnerQuery(QueryShardContext context) throws IOException {
        return toQuery(context.parseContext().parseInnerQueryBuilder(), context);
    }

    public ParsedQuery toQuery(QueryBuilder<?> queryBuilder) {
        QueryShardContext context = cache.get();
        context.reset();
        context.parseFieldMatcher(parseFieldMatcher);
        try {
            Query query = toQuery(queryBuilder, context);
            return new ParsedQuery(query, context.copyNamedQueries());
        } catch(QueryShardException | ParsingException e ) {
            throw e;
        } catch(Exception e) {
            throw new QueryShardException(context, "failed to create query: {}", e, queryBuilder);
        } finally {
            context.reset();
        }
    }

    private static Query toQuery(QueryBuilder<?> queryBuilder, QueryShardContext context) throws IOException {
        Query query = queryBuilder.toQuery(context);
        if (query == null) {
            query = Queries.newMatchNoDocsQuery();
        }
        return query;
    }

    public ParseFieldMatcher parseFieldMatcher() {
        return parseFieldMatcher;
    }

    public boolean matchesIndices(String... indices) {
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterService.state(), IndicesOptions.lenientExpandOpen(), indices);
        for (String index : concreteIndices) {
            if (Regex.simpleMatch(index, this.index.name())) {
                return true;
            }
        }
        return false;
    }

    public InnerHitsQueryParserHelper getInnerHitsQueryParserHelper() {
        return innerHitsQueryParserHelper;
    }

    public Client getClient() {
        return client;
    }
}
