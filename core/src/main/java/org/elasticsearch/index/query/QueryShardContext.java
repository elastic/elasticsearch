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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
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
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Context object used to create lucene queries on the shard level.
 */
public class QueryShardContext {

    private static ThreadLocal<String[]> typesContext = new ThreadLocal<>();

    public static void setTypes(String[] types) {
        typesContext.set(types);
    }

    public static String[] getTypes() {
        return typesContext.get();
    }

    public static String[] setTypesWithPrevious(String... types) {
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

    private boolean allowUnmappedFields;

    private boolean mapUnmappedFieldAsString;

    private NestedScope nestedScope;

    private QueryParseContext parseContext;

    boolean isFilter;

    public QueryShardContext(Index index, IndexQueryParserService indexQueryParser) {
        this.index = index;
        this.indexVersionCreated = Version.indexCreated(indexQueryParser.indexSettings());
        this.indexQueryParser = indexQueryParser;
        this.parseContext = new QueryParseContext(indexQueryParser.indicesQueriesRegistry());
    }

    public void parseFieldMatcher(ParseFieldMatcher parseFieldMatcher) {
        this.parseContext.parseFieldMatcher(parseFieldMatcher);
    }

    public ParseFieldMatcher parseFieldMatcher() {
        return parseContext.parseFieldMatcher();
    }

    public void reset() {
        allowUnmappedFields = indexQueryParser.defaultAllowUnmappedFields();
        this.parseFieldMatcher(ParseFieldMatcher.EMPTY);
        this.lookup = null;
        this.namedQueries.clear();
        this.nestedScope = new NestedScope();
    }

    public void reset(XContentParser jp) {
        this.reset();
        this.parseContext.reset(jp);
    }

    public Index index() {
        return this.index;
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

    public Similarity searchSimilarity() {
        return indexQueryParser.similarityService != null ? indexQueryParser.similarityService.similarity(indexQueryParser.mapperService) : null;
    }

    public String defaultField() {
        return indexQueryParser.defaultField();
    }

    public boolean queryStringLenient() {
        return indexQueryParser.queryStringLenient();
    }

    public boolean queryStringAnalyzeWildcard() {
        return indexQueryParser.queryStringAnalyzeWildcard();
    }

    public boolean queryStringAllowLeadingWildcard() {
        return indexQueryParser.queryStringAllowLeadingWildcard();
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

    public Map<String, Query> copyNamedQueries() {
        // This might be a good use case for CopyOnWriteHashMap
        return unmodifiableMap(new HashMap<>(namedQueries));
    }

    public void combineNamedQueries(QueryShardContext context) {
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
            throw new QueryShardException(this, "inner_hits unsupported");
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

    public Collection<String> simpleMatchToIndexNames(String pattern) {
        return indexQueryParser.mapperService.simpleMatchToIndexNames(pattern);
    }

    public MappedFieldType fieldMapper(String name) {
        return failIfFieldMappingNotFound(name, indexQueryParser.mapperService.smartNameFieldType(name, getTypes()));
    }

    public ObjectMapper getObjectMapper(String name) {
        return indexQueryParser.mapperService.getObjectMapper(name, getTypes());
    }

    /**
     * Gets the search analyzer for the given field, or the default if there is none present for the field
     * TODO: remove this by moving defaults into mappers themselves
     */
    public Analyzer getSearchAnalyzer(MappedFieldType fieldType) {
        if (fieldType.searchAnalyzer() != null) {
            return fieldType.searchAnalyzer();
        }
        return mapperService().searchAnalyzer();
    }

    /**
     * Gets the search quote analyzer for the given field, or the default if there is none present for the field
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
        } else if (mapUnmappedFieldAsString) {
            StringFieldMapper.Builder builder = MapperBuilders.stringField(name);
            // it would be better to pass the real index settings, but they are not easily accessible from here...
            Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, indexQueryParser.getIndexCreatedVersion()).build();
            return builder.build(new Mapper.BuilderContext(settings, new ContentPath(1))).fieldType();
        } else {
            Version indexCreatedVersion = indexQueryParser.getIndexCreatedVersion();
            if (fieldMapping == null && indexCreatedVersion.onOrAfter(Version.V_1_4_0_Beta1)) {
                throw new QueryShardException(this, "Strict field resolution and no field mapping can be found for the field with name ["
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

    public Version indexVersionCreated() {
        return indexVersionCreated;
    }

    public QueryParseContext parseContext() {
        return this.parseContext;
    }

    public boolean matchesIndices(String... indices) {
        return this.indexQueryParser.matchesIndices(indices);
    }

    /*
    * Executes the given template, and returns the response.
    */
    public BytesReference executeQueryTemplate(Template template, SearchContext searchContext) {
        ExecutableScript executable = scriptService().executable(template, ScriptContext.Standard.SEARCH, searchContext);
        return (BytesReference) executable.run();
    }

    public Client getClient() {
        return indexQueryParser.getClient();
    }
}
