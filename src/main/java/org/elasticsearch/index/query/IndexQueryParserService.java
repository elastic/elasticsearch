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

package org.elasticsearch.index.query;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 *
 */
public class IndexQueryParserService extends AbstractIndexComponent {

    public static final class Defaults {
        public static final String QUERY_PREFIX = "index.queryparser.query";
        public static final String FILTER_PREFIX = "index.queryparser.filter";
    }

    private CloseableThreadLocal<QueryParseContext> cache = new CloseableThreadLocal<QueryParseContext>() {
        @Override
        protected QueryParseContext initialValue() {
            return new QueryParseContext(index, IndexQueryParserService.this);
        }
    };

    final CacheRecycler cacheRecycler;

    final AnalysisService analysisService;

    final ScriptService scriptService;

    final MapperService mapperService;

    final SimilarityService similarityService;

    final IndexCache indexCache;

    final IndexFieldDataService fieldDataService;

    final IndexEngine indexEngine;

    private final Map<String, QueryParser> queryParsers;

    private final Map<String, FilterParser> filterParsers;

    private String defaultField;
    private boolean queryStringLenient;

    @Inject
    public IndexQueryParserService(Index index, @IndexSettings Settings indexSettings,
                                   IndicesQueriesRegistry indicesQueriesRegistry, CacheRecycler cacheRecycler,
                                   ScriptService scriptService, AnalysisService analysisService,
                                   MapperService mapperService, IndexCache indexCache, IndexFieldDataService fieldDataService, IndexEngine indexEngine,
                                   @Nullable SimilarityService similarityService,
                                   @Nullable Map<String, QueryParserFactory> namedQueryParsers,
                                   @Nullable Map<String, FilterParserFactory> namedFilterParsers) {
        super(index, indexSettings);
        this.cacheRecycler = cacheRecycler;
        this.scriptService = scriptService;
        this.analysisService = analysisService;
        this.mapperService = mapperService;
        this.similarityService = similarityService;
        this.indexCache = indexCache;
        this.fieldDataService = fieldDataService;
        this.indexEngine = indexEngine;

        this.defaultField = indexSettings.get("index.query.default_field", AllFieldMapper.NAME);
        this.queryStringLenient = indexSettings.getAsBoolean("index.query_string.lenient", false);

        List<QueryParser> queryParsers = newArrayList();
        if (namedQueryParsers != null) {
            Map<String, Settings> queryParserGroups = indexSettings.getGroups(IndexQueryParserService.Defaults.QUERY_PREFIX);
            for (Map.Entry<String, QueryParserFactory> entry : namedQueryParsers.entrySet()) {
                String queryParserName = entry.getKey();
                QueryParserFactory queryParserFactory = entry.getValue();
                Settings queryParserSettings = queryParserGroups.get(queryParserName);
                if (queryParserSettings == null) {
                    queryParserSettings = EMPTY_SETTINGS;
                }
                queryParsers.add(queryParserFactory.create(queryParserName, queryParserSettings));
            }
        }

        Map<String, QueryParser> queryParsersMap = newHashMap();
        queryParsersMap.putAll(indicesQueriesRegistry.queryParsers());
        if (queryParsers != null) {
            for (QueryParser queryParser : queryParsers) {
                add(queryParsersMap, queryParser);
            }
        }
        this.queryParsers = ImmutableMap.copyOf(queryParsersMap);

        List<FilterParser> filterParsers = newArrayList();
        if (namedFilterParsers != null) {
            Map<String, Settings> filterParserGroups = indexSettings.getGroups(IndexQueryParserService.Defaults.FILTER_PREFIX);
            for (Map.Entry<String, FilterParserFactory> entry : namedFilterParsers.entrySet()) {
                String filterParserName = entry.getKey();
                FilterParserFactory filterParserFactory = entry.getValue();
                Settings filterParserSettings = filterParserGroups.get(filterParserName);
                if (filterParserSettings == null) {
                    filterParserSettings = EMPTY_SETTINGS;
                }
                filterParsers.add(filterParserFactory.create(filterParserName, filterParserSettings));
            }
        }

        Map<String, FilterParser> filterParsersMap = newHashMap();
        filterParsersMap.putAll(indicesQueriesRegistry.filterParsers());
        if (filterParsers != null) {
            for (FilterParser filterParser : filterParsers) {
                add(filterParsersMap, filterParser);
            }
        }
        this.filterParsers = ImmutableMap.copyOf(filterParsersMap);
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
        return queryParsers.get(name);
    }

    public FilterParser filterParser(String name) {
        return filterParsers.get(name);
    }

    public ParsedQuery parse(QueryBuilder queryBuilder) throws ElasticSearchException {
        XContentParser parser = null;
        try {
            BytesReference bytes = queryBuilder.buildAsBytes();
            parser = XContentFactory.xContent(bytes).createParser(bytes);
            return parse(cache.get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(index, "Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public ParsedQuery parse(byte[] source) throws ElasticSearchException {
        return parse(source, 0, source.length);
    }

    public ParsedQuery parse(byte[] source, int offset, int length) throws ElasticSearchException {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source, offset, length).createParser(source, offset, length);
            return parse(cache.get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(index, "Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public ParsedQuery parse(BytesReference source) throws ElasticSearchException {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            return parse(cache.get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(index, "Failed to parse", e);
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
            return parse(cache.get(), parser);
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(index, "Failed to parse [" + source + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public ParsedQuery parse(XContentParser parser) {
        try {
            return parse(cache.get(), parser);
        } catch (IOException e) {
            throw new QueryParsingException(index, "Failed to parse", e);
        }
    }

    /**
     * Parses an inner filter, returning null if the filter should be ignored.
     */
    @Nullable
    public ParsedFilter parseInnerFilter(XContentParser parser) throws IOException {
        QueryParseContext context = cache.get();
        context.reset(parser);
        Filter filter = context.parseInnerFilter();
        if (filter == null) {
            return null;
        }
        return new ParsedFilter(filter, context.copyNamedFilters());
    }

    @Nullable
    public Query parseInnerQuery(XContentParser parser) throws IOException {
        QueryParseContext context = cache.get();
        context.reset(parser);
        return context.parseInnerQuery();
    }

    private ParsedQuery parse(QueryParseContext parseContext, XContentParser parser) throws IOException, QueryParsingException {
        parseContext.reset(parser);
        Query query = parseContext.parseInnerQuery();
        if (query == null) {
            query = Queries.NO_MATCH_QUERY;
        }
        return new ParsedQuery(query, parseContext.copyNamedFilters());
    }

    private void add(Map<String, FilterParser> map, FilterParser filterParser) {
        for (String name : filterParser.names()) {
            map.put(name.intern(), filterParser);
        }
    }

    private void add(Map<String, QueryParser> map, QueryParser queryParser) {
        for (String name : queryParser.names()) {
            map.put(name.intern(), queryParser);
        }
    }
}
