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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
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
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;

public class IndexQueryParserService extends AbstractIndexComponent {

    public static final String DEFAULT_FIELD = "index.query.default_field";
    public static final String QUERY_STRING_LENIENT = "index.query_string.lenient";
    public static final String PARSE_STRICT = "index.query.parse.strict";
    public static final String ALLOW_UNMAPPED = "index.query.parse.allow_unmapped_fields";

    private CloseableThreadLocal<QueryParseContext> cache = new CloseableThreadLocal<QueryParseContext>() {
        @Override
        protected QueryParseContext initialValue() {
            return new QueryParseContext(index, IndexQueryParserService.this);
        }
    };

    final AnalysisService analysisService;

    final ScriptService scriptService;

    final MapperService mapperService;

    final SimilarityService similarityService;

    final IndexCache indexCache;

    final IndexFieldDataService fieldDataService;

    final BitsetFilterCache bitsetFilterCache;

    private final IndicesQueriesRegistry indicesQueriesRegistry;

    private String defaultField;
    private boolean queryStringLenient;
    private final ParseFieldMatcher parseFieldMatcher;
    private final boolean defaultAllowUnmappedFields;

    @Inject
    public IndexQueryParserService(Index index, @IndexSettings Settings indexSettings,
                                   IndicesQueriesRegistry indicesQueriesRegistry,
                                   ScriptService scriptService, AnalysisService analysisService,
                                   MapperService mapperService, IndexCache indexCache, IndexFieldDataService fieldDataService,
                                   BitsetFilterCache bitsetFilterCache,
                                   @Nullable SimilarityService similarityService) {
        super(index, indexSettings);
        this.scriptService = scriptService;
        this.analysisService = analysisService;
        this.mapperService = mapperService;
        this.similarityService = similarityService;
        this.indexCache = indexCache;
        this.fieldDataService = fieldDataService;
        this.bitsetFilterCache = bitsetFilterCache;

        this.defaultField = indexSettings.get(DEFAULT_FIELD, AllFieldMapper.NAME);
        this.queryStringLenient = indexSettings.getAsBoolean(QUERY_STRING_LENIENT, false);
        this.parseFieldMatcher = new ParseFieldMatcher(indexSettings);
        this.defaultAllowUnmappedFields = indexSettings.getAsBoolean(ALLOW_UNMAPPED, true);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
    }

    public void close() {
        cache.close();
    }

    public String defaultField() {
        return this.defaultField;
    }

    public boolean queryStringLenient() {
        return this.queryStringLenient;
    }

    public QueryParser queryParser(String name) {
        return indicesQueriesRegistry.queryParsers().get(name);
    }

    public ParsedQuery parse(QueryBuilder queryBuilder) {
        XContentParser parser = null;
        try {
            BytesReference bytes = queryBuilder.buildAsBytes();
            parser = XContentFactory.xContent(bytes).createParser(bytes);
            return parse(cache.get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(getParseContext(), "Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public ParsedQuery parse(byte[] source) {
        return parse(source, 0, source.length);
    }

    public ParsedQuery parse(byte[] source, int offset, int length) {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source, offset, length).createParser(source, offset, length);
            return parse(cache.get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(getParseContext(), "Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public ParsedQuery parse(BytesReference source) {
        return parse(cache.get(), source);
    }

    public ParsedQuery parse(QueryParseContext context, BytesReference source) {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            return innerParse(context, parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(context, "Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public ParsedQuery parse(String source) throws QueryParsingException {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            return innerParse(cache.get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(getParseContext(), "Failed to parse [" + source + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public ParsedQuery parse(XContentParser parser) {
        return parse(cache.get(), parser);
    }

    public ParsedQuery parse(QueryParseContext context, XContentParser parser) {
        try {
            return innerParse(context, parser);
        } catch (IOException e) {
            throw new QueryParsingException(context, "Failed to parse", e);
        }
    }

    /**
     * Parses an inner filter, returning null if the filter should be ignored.
     */
    @Nullable
    public ParsedQuery parseInnerFilter(XContentParser parser) throws IOException {
        QueryParseContext context = cache.get();
        context.reset(parser);
        try {
            Query filter = context.parseInnerFilter();
            if (filter == null) {
                return null;
            }
            return new ParsedQuery(filter, context.copyNamedQueries());
        } finally {
            context.reset(null);
        }
    }

    @Nullable
    public Query parseInnerQuery(XContentParser parser) throws IOException {
        QueryParseContext context = cache.get();
        context.reset(parser);
        try {
            return context.parseInnerQuery();
        } finally {
            context.reset(null);
        }
    }

    @Nullable
    public Query parseInnerQuery(QueryParseContext parseContext) throws IOException {
        parseContext.parseFieldMatcher(parseFieldMatcher);
        Query query = parseContext.parseInnerQuery();
        if (query == null) {
            query = Queries.newMatchNoDocsQuery();
        }
        return query;
    }

    public QueryParseContext getParseContext() {
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
        try {
            ParsedQuery parsedQuery = null;
            XContentParser parser = XContentHelper.createParser(source);
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
                        throw new QueryParsingException(getParseContext(), "request does not support [" + fieldName + "]");
                    }
                }
            }
            if (parsedQuery != null) {
                return parsedQuery;
            }
        } catch (QueryParsingException e) {
            throw e;
        } catch (Throwable e) {
            throw new QueryParsingException(getParseContext(), "Failed to parse", e);
        }

        throw new QueryParsingException(getParseContext(), "Required query is missing");
    }

    private ParsedQuery innerParse(QueryParseContext parseContext, XContentParser parser) throws IOException, QueryParsingException {
        parseContext.reset(parser);
        try {
            parseContext.parseFieldMatcher(parseFieldMatcher);
            Query query = parseContext.parseInnerQuery();
            if (query == null) {
                query = Queries.newMatchNoDocsQuery();
            }
            return new ParsedQuery(query, parseContext.copyNamedQueries());
        } finally {
            parseContext.reset(null);
        }
    }

    public ParseFieldMatcher parseFieldMatcher() {
        return parseFieldMatcher;
    }
}
