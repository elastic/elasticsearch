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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class QueryParseContext {

    private static final ParseField CACHE = new ParseField("_cache").withAllDeprecated("Elasticsearch makes its own caching decisions");
    private static final ParseField CACHE_KEY = new ParseField("_cache_key").withAllDeprecated("Filters are always used as cache keys");

    private static ThreadLocal<String[]> typesContext = new ThreadLocal<>();

    public static void setTypes(String[] types) {
        typesContext.set(types);
    }

    public static String[] getTypes() {
        return typesContext.get();
    }

    public static String[] setTypesWithPrevious(String[] types) {
        String[] old = typesContext.get();
        setTypes(types);
        return old;
    }

    public static void removeTypes() {
        typesContext.remove();
    }

    private final Index index;

    private final Version indexVersionCreated;

    private final IndexQueryParserService indexQueryParser;

    private final Map<String, Query> namedQueries = new HashMap<>();

    private final MapperQueryParser queryParser = new MapperQueryParser(this);

    private XContentParser parser;

    private ParseFieldMatcher parseFieldMatcher;

    private boolean allowUnmappedFields;

    private boolean mapUnmappedFieldAsString;

    private NestedScope nestedScope;

    private boolean isFilter;

    public QueryParseContext(Index index, IndexQueryParserService indexQueryParser) {
        this.index = index;
        this.indexVersionCreated = Version.indexCreated(indexQueryParser.indexSettings());
        this.indexQueryParser = indexQueryParser;
    }

    public void parseFieldMatcher(ParseFieldMatcher parseFieldMatcher) {
        this.parseFieldMatcher = parseFieldMatcher;
    }

    public ParseFieldMatcher parseFieldMatcher() {
        return parseFieldMatcher;
    }

    public void reset(XContentParser jp) {
        allowUnmappedFields = indexQueryParser.defaultAllowUnmappedFields();
        this.parseFieldMatcher = ParseFieldMatcher.EMPTY;
        this.lookup = null;
        this.parser = jp;
        this.namedQueries.clear();
        this.nestedScope = new NestedScope();
        this.isFilter = false;
    }

    public Index index() {
        return this.index;
    }

    public void parser(XContentParser parser) {
        this.parser = parser;
    }

    public XContentParser parser() {
        return parser;
    }
    
    public IndexQueryParserService indexQueryParserService() {
        return indexQueryParser;
    }

    public AnalysisService analysisService() {
        return indexQueryParser.analysisService;
    }

    public ScriptService scriptService() {
        return indexQueryParser.scriptService;
    }

    public MapperService mapperService() {
        return indexQueryParser.mapperService;
    }

    @Nullable
    public SimilarityService similarityService() {
        return indexQueryParser.similarityService;
    }

    public Similarity searchSimilarity() {
        return indexQueryParser.similarityService != null ? indexQueryParser.similarityService.similarity() : null;
    }

    public String defaultField() {
        return indexQueryParser.defaultField();
    }

    public boolean queryStringLenient() {
        return indexQueryParser.queryStringLenient();
    }

    public MapperQueryParser queryParser(QueryParserSettings settings) {
        queryParser.reset(settings);
        return queryParser;
    }

    public BitSetProducer bitsetFilter(Filter filter) {
        return indexQueryParser.bitsetFilterCache.getBitSetProducer(filter);
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType mapper) {
        return indexQueryParser.fieldDataService.getForField(mapper);
    }

    public void addNamedQuery(String name, Query query) {
        if (query != null) {
            namedQueries.put(name, query);
        }
    }

    public ImmutableMap<String, Query> copyNamedQueries() {
        return ImmutableMap.copyOf(namedQueries);
    }

    public void combineNamedQueries(QueryParseContext context) {
        namedQueries.putAll(context.namedQueries);
    }

    /**
     * Return whether we are currently parsing a filter or a query.
     */
    public boolean isFilter() {
        return isFilter;
    }

    public void addInnerHits(String name, InnerHitsContext.BaseInnerHits context) {
        SearchContext sc = SearchContext.current();
        if (sc == null) {
            throw new QueryParsingException(this, "inner_hits unsupported");
        }

        InnerHitsContext innerHitsContext;
        if (sc.innerHits() == null) {
            innerHitsContext = new InnerHitsContext(new HashMap<String, InnerHitsContext.BaseInnerHits>());
            sc.innerHits(innerHitsContext);
        } else {
            innerHitsContext = sc.innerHits();
        }
        innerHitsContext.addInnerHitDefinition(name, context);
    }

    @Nullable
    public Query parseInnerQuery() throws QueryParsingException, IOException {
        // move to START object
        XContentParser.Token token;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new QueryParsingException(this, "[_na] query malformed, must start with start_object");
            }
        }
        token = parser.nextToken();
        if (token == XContentParser.Token.END_OBJECT) {
            // empty query
            return null;
        }
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(this, "[_na] query malformed, no field after start_object");
        }
        String queryName = parser.currentName();
        // move to the next START_OBJECT
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT && token != XContentParser.Token.START_ARRAY) {
            throw new QueryParsingException(this, "[_na] query malformed, no field after start_object");
        }

        QueryParser queryParser = indexQueryParser.queryParser(queryName);
        if (queryParser == null) {
            throw new QueryParsingException(this, "No query registered for [" + queryName + "]");
        }
        Query result = queryParser.parse(this);
        if (parser.currentToken() == XContentParser.Token.END_OBJECT || parser.currentToken() == XContentParser.Token.END_ARRAY) {
            // if we are at END_OBJECT, move to the next one...
            parser.nextToken();
        }
        return result;
    }

    @Nullable
    public Query parseInnerFilter() throws QueryParsingException, IOException {
        final boolean originalIsFilter = isFilter;
        try {
            isFilter = true;
            return parseInnerQuery();
        } finally {
            isFilter = originalIsFilter;
        }
    }

    public Query parseInnerFilter(String queryName) throws IOException, QueryParsingException {
        final boolean originalIsFilter = isFilter;
        try {
            isFilter = true;
            QueryParser queryParser = indexQueryParser.queryParser(queryName);
            if (queryParser == null) {
                throw new QueryParsingException(this, "No query registered for [" + queryName + "]");
            }
            return queryParser.parse(this);
        } finally {
            isFilter = originalIsFilter;
        }
    }

    public Collection<String> simpleMatchToIndexNames(String pattern) {
        return indexQueryParser.mapperService.simpleMatchToIndexNames(pattern, getTypes());
    }

    public MappedFieldType fieldMapper(String name) {
        return failIfFieldMappingNotFound(name, indexQueryParser.mapperService.smartNameFieldType(name, getTypes()));
    }

    public ObjectMapper getObjectMapper(String name) {
        return indexQueryParser.mapperService.getObjectMapper(name, getTypes());
    }

    /** Gets the search analyzer for the given field, or the default if there is none present for the field
     * TODO: remove this by moving defaults into mappers themselves
     */
    public Analyzer getSearchAnalyzer(MappedFieldType fieldType) {
        if (fieldType.searchAnalyzer() != null) {
            return fieldType.searchAnalyzer();
        }
        return mapperService().searchAnalyzer();
    }

    /** Gets the search quote nalyzer for the given field, or the default if there is none present for the field
     * TODO: remove this by moving defaults into mappers themselves
     */
    public Analyzer getSearchQuoteAnalyzer(MappedFieldType fieldType) {
        if (fieldType.searchQuoteAnalyzer() != null) {
            return fieldType.searchQuoteAnalyzer();
        }
        return mapperService().searchQuoteAnalyzer();
    }

    public void setAllowUnmappedFields(boolean allowUnmappedFields) {
        this.allowUnmappedFields = allowUnmappedFields;
    }

    public void setMapUnmappedFieldAsString(boolean mapUnmappedFieldAsString) {
        this.mapUnmappedFieldAsString = mapUnmappedFieldAsString;
    }

    private MappedFieldType failIfFieldMappingNotFound(String name, MappedFieldType fieldMapping) {
        if (allowUnmappedFields) {
            return fieldMapping;
        } else if (mapUnmappedFieldAsString){
            StringFieldMapper.Builder builder = MapperBuilders.stringField(name);
            // it would be better to pass the real index settings, but they are not easily accessible from here...
            Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, indexQueryParser.getIndexCreatedVersion()).build();
            return builder.build(new Mapper.BuilderContext(settings, new ContentPath(1))).fieldType();
        } else {
            Version indexCreatedVersion = indexQueryParser.getIndexCreatedVersion();
            if (fieldMapping == null && indexCreatedVersion.onOrAfter(Version.V_1_4_0_Beta1)) {
                throw new QueryParsingException(this, "Strict field resolution and no field mapping can be found for the field with name ["
                        + name + "]");
            } else {
                return fieldMapping;
            }
        }
    }

    /**
     * Returns the narrowed down explicit types, or, if not set, all types.
     */
    public Collection<String> queryTypes() {
        String[] types = getTypes();
        if (types == null || types.length == 0) {
            return mapperService().types();
        }
        if (types.length == 1 && types[0].equals("_all")) {
            return mapperService().types();
        }
        return Arrays.asList(types);
    }

    private SearchLookup lookup = null;

    public SearchLookup lookup() {
        SearchContext current = SearchContext.current();
        if (current != null) {
            return current.lookup();
        }
        if (lookup == null) {
            lookup = new SearchLookup(mapperService(), indexQueryParser.fieldDataService, null);
        }
        return lookup;
    }

    public long nowInMillis() {
        SearchContext current = SearchContext.current();
        if (current != null) {
            return current.nowInMillis();
        }
        return System.currentTimeMillis();
    }

    public NestedScope nestedScope() {
        return nestedScope;
    }

    /**
     * Return whether the setting is deprecated.
     */
    public boolean isDeprecatedSetting(String setting) {
        return parseFieldMatcher.match(setting, CACHE) || parseFieldMatcher.match(setting, CACHE_KEY);
    }

    public Version indexVersionCreated() {
        return indexVersionCreated;
    }
}
