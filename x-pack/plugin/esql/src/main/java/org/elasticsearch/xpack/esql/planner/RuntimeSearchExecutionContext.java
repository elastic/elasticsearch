/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A shard-free {@link SearchExecutionContext} for runtime ES|QL columns. It exposes each column as an indexed text
 * field and provides the analyzer callers need when indexing values for the resulting query.
 */
public final class RuntimeSearchExecutionContext extends SearchExecutionContext {

    // QueryStringQueryParser requires a default search analyzer.
    private static final String DEFAULT_ANALYZER_KEY = "default";

    // SearchExecutionContext requires non-null search stats.
    private static final ShardSearchStats SHARD_SEARCH_STATS = new ShardSearchStats(
        new SearchStatsSettings(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
    );

    // Reuse these settings because they do not vary between contexts and are expensive to build.
    private static final IndexSettings INDEX_SETTINGS = syntheticIndexSettings();

    private final Map<String, MappedFieldType> fields;
    private final IndexAnalyzers indexAnalyzers;
    private final NamedAnalyzer searchAnalyzer;

    public static RuntimeSearchExecutionContext create(List<String> fieldNames) {
        return create(fieldNames, Lucene.STANDARD_ANALYZER);
    }

    public static RuntimeSearchExecutionContext create(List<String> fieldNames, NamedAnalyzer searchAnalyzer) {
        Map<String, MappedFieldType> fields = new LinkedHashMap<>();
        TextSearchInfo tsi = new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, searchAnalyzer, searchAnalyzer);
        for (String name : fieldNames) {
            fields.put(name, new TextFieldMapper.TextFieldType(name, true, false, tsi, false, false, null, Map.of(), false, false));
        }
        IndexAnalyzers analyzers = IndexAnalyzers.of(Map.of(DEFAULT_ANALYZER_KEY, searchAnalyzer));
        return new RuntimeSearchExecutionContext(fields, analyzers, searchAnalyzer);
    }

    private static IndexSettings syntheticIndexSettings() {
        IndexMetadata meta = IndexMetadata.builder("_esql_runtime_")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();
        return new IndexSettings(meta, Settings.EMPTY);
    }

    private RuntimeSearchExecutionContext(
        Map<String, MappedFieldType> fields,
        IndexAnalyzers indexAnalyzers,
        NamedAnalyzer searchAnalyzer
    ) {
        super(
            0,                              // shardId
            0,                              // shardRequestIndex
            INDEX_SETTINGS,
            null,                           // bitsetFilterCache
            null,                           // indexFieldDataLookup
            null,                           // mapperService
            MappingLookup.EMPTY,            // mappingLookup (field lookup is overridden below)
            null,                           // similarityService
            null,                           // scriptService
            XContentParserConfiguration.EMPTY,
            null,                           // namedWriteableRegistry
            null,                           // client
            null,                           // searcher
            System::currentTimeMillis,      // nowInMillis (date-math only; unreachable for text fields)
            null,                           // clusterAlias
            null,                           // indexNameMatcher
            () -> true,                     // allowExpensiveQueries (asserted non-null)
            null,                           // valuesSourceRegistry
            Map.of(),                       // runtimeMappings
            null,                           // requestSize
            MapperMetrics.NOOP,
            SHARD_SEARCH_STATS
        );
        this.fields = fields;
        this.indexAnalyzers = indexAnalyzers;
        this.searchAnalyzer = searchAnalyzer;
    }

    /** Returns the analyzer configured on the synthetic fields. */
    public NamedAnalyzer searchAnalyzer() {
        return searchAnalyzer;
    }

    @Override
    public MappedFieldType getFieldType(String name) {
        return fieldType(name);
    }

    @Override
    protected MappedFieldType fieldType(String name) {
        return fields.get(name);
    }

    @Override
    public Set<String> getMatchingFieldNames(String pattern) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            return fields.containsKey(pattern) ? Set.of(pattern) : Set.of();
        }
        return fields.keySet().stream().filter(f -> Regex.simpleMatch(pattern, f)).collect(Collectors.toSet());
    }

    /** Avoids the inherited searcher lookup because this context has no searcher. */
    @Override
    public boolean fieldExistsInIndex(String fieldname) {
        return fields.containsKey(fieldname);
    }

    @Override
    public IndexAnalyzers getIndexAnalyzers() {
        return indexAnalyzers;
    }

    // These inherited methods require a mapper service, which this context does not have.
    @Override
    public boolean isMetadataField(String field) {
        return false;
    }

    @Override
    public boolean isMultiField(String field) {
        return false;
    }

    // Make unsupported shard-dependent operations fail with a useful message.
    @Override
    public BitSetProducer bitsetFilter(Query filter) {
        throw shardStateUnavailable("bitsetFilter");
    }

    @Override
    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType, MappedFieldType.FielddataOperation operation) {
        throw shardStateUnavailable("getForField");
    }

    @Override
    public ParsedDocument parseDocument(SourceToParse source) {
        throw shardStateUnavailable("parseDocument");
    }

    @Override
    public SearchLookup lookup() {
        throw shardStateUnavailable("lookup");
    }

    @Override
    public NestedDocuments getNestedDocuments() {
        throw shardStateUnavailable("getNestedDocuments");
    }

    @Override
    public MappedFieldType buildAnonymousFieldType(String type) {
        throw shardStateUnavailable("buildAnonymousFieldType");
    }

    private static UnsupportedOperationException shardStateUnavailable(String method) {
        return new UnsupportedOperationException(
            "["
                + method
                + "] needs shard state that "
                + RuntimeSearchExecutionContext.class.getSimpleName()
                + " does not have, see its javadoc"
        );
    }
}
