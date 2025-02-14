/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.lookup.LeafFieldLookupProvider;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * This is NOT a simple clone of the SearchExecutionContext.
 * While it does "clone-esque" things, it delegates everything it can to the passed search execution context.
 *
 * Do NOT use this if you mean to clone the context as you are planning to make modifications
 */
public class FilteredSearchExecutionContext extends SearchExecutionContext {
    private final SearchExecutionContext in;

    public FilteredSearchExecutionContext(SearchExecutionContext in) {
        super(in);
        this.in = in;
    }

    @Override
    public Similarity getSearchSimilarity() {
        return in.getSearchSimilarity();
    }

    @Override
    public Similarity getDefaultSimilarity() {
        return in.getDefaultSimilarity();
    }

    @Override
    public List<String> defaultFields() {
        return in.defaultFields();
    }

    @Override
    public boolean queryStringLenient() {
        return in.queryStringLenient();
    }

    @Override
    public boolean queryStringAnalyzeWildcard() {
        return in.queryStringAnalyzeWildcard();
    }

    @Override
    public boolean queryStringAllowLeadingWildcard() {
        return in.queryStringAllowLeadingWildcard();
    }

    @Override
    public BitSetProducer bitsetFilter(Query filter) {
        return in.bitsetFilter(filter);
    }

    @Override
    public <IFD extends IndexFieldData<?>> IFD getForField(
        MappedFieldType fieldType,
        MappedFieldType.FielddataOperation fielddataOperation
    ) {
        return in.getForField(fieldType, fielddataOperation);
    }

    @Override
    public void addNamedQuery(String name, Query query) {
        in.addNamedQuery(name, query);
    }

    @Override
    public Map<String, Query> copyNamedQueries() {
        return in.copyNamedQueries();
    }

    @Override
    public ParsedDocument parseDocument(SourceToParse source) throws DocumentParsingException {
        return in.parseDocument(source);
    }

    @Override
    public NestedLookup nestedLookup() {
        return in.nestedLookup();
    }

    @Override
    public boolean hasMappings() {
        return in.hasMappings();
    }

    @Override
    public boolean isFieldMapped(String name) {
        return in.isFieldMapped(name);
    }

    @Override
    public boolean isMetadataField(String field) {
        return in.isMetadataField(field);
    }

    @Override
    public boolean isMultiField(String field) {
        return in.isMultiField(field);
    }

    @Override
    public Set<String> sourcePath(String fullName) {
        return in.sourcePath(fullName);
    }

    @Override
    public boolean isSourceEnabled() {
        return in.isSourceEnabled();
    }

    @Override
    public boolean isSourceSynthetic() {
        return in.isSourceSynthetic();
    }

    @Override
    public SourceLoader newSourceLoader(boolean forceSyntheticSource) {
        return in.newSourceLoader(forceSyntheticSource);
    }

    @Override
    public MappedFieldType buildAnonymousFieldType(String type) {
        return in.buildAnonymousFieldType(type);
    }

    @Override
    public Analyzer getIndexAnalyzer(Function<String, NamedAnalyzer> unindexedFieldAnalyzer) {
        return in.getIndexAnalyzer(unindexedFieldAnalyzer);
    }

    @Override
    public void setAllowedFields(Predicate<String> allowedFields) {
        in.setAllowedFields(allowedFields);
    }

    @Override
    public boolean containsBrokenAnalysis(String field) {
        return in.containsBrokenAnalysis(field);
    }

    @Override
    public SearchLookup lookup() {
        return in.lookup();
    }

    @Override
    public void setLookupProviders(
        SourceProvider sourceProvider,
        Function<LeafReaderContext, LeafFieldLookupProvider> fieldLookupProvider
    ) {
        in.setLookupProviders(sourceProvider, fieldLookupProvider);
    }

    @Override
    public NestedScope nestedScope() {
        return in.nestedScope();
    }

    @Override
    public IndexVersion indexVersionCreated() {
        return in.indexVersionCreated();
    }

    @Override
    public boolean indexSortedOnField(String field) {
        return in.indexSortedOnField(field);
    }

    @Override
    public ParsedQuery toQuery(QueryBuilder queryBuilder) {
        return in.toQuery(queryBuilder);
    }

    @Override
    public Index index() {
        return in.index();
    }

    @Override
    public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
        return in.compile(script, context);
    }

    @Override
    public void disableCache() {
        in.disableCache();
    }

    @Override
    public void registerAsyncAction(BiConsumer<Client, ActionListener<?>> asyncAction) {
        in.registerAsyncAction(asyncAction);
    }

    @Override
    public void executeAsyncActions(ActionListener<Void> listener) {
        in.executeAsyncActions(listener);
    }

    @Override
    public int getShardId() {
        return in.getShardId();
    }

    @Override
    public int getShardRequestIndex() {
        return in.getShardRequestIndex();
    }

    @Override
    public long nowInMillis() {
        return in.nowInMillis();
    }

    @Override
    public Client getClient() {
        return in.getClient();
    }

    @Override
    public IndexReader getIndexReader() {
        return in.getIndexReader();
    }

    @Override
    public IndexSearcher searcher() {
        return in.searcher();
    }

    @Override
    public boolean fieldExistsInIndex(String fieldname) {
        return in.fieldExistsInIndex(fieldname);
    }

    @Override
    public MappingLookup.CacheKey mappingCacheKey() {
        return in.mappingCacheKey();
    }

    @Override
    public NestedDocuments getNestedDocuments() {
        return in.getNestedDocuments();
    }

    @Override
    public XContentParserConfiguration getParserConfig() {
        return in.getParserConfig();
    }

    @Override
    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return in.convertToCoordinatorRewriteContext();
    }

    @Override
    public QueryRewriteContext convertToIndexMetadataContext() {
        return in.convertToIndexMetadataContext();
    }

    @Override
    public DataRewriteContext convertToDataRewriteContext() {
        return in.convertToDataRewriteContext();
    }

    @Override
    public MappedFieldType getFieldType(String name) {
        return in.getFieldType(name);
    }

    @Override
    protected MappedFieldType fieldType(String name) {
        return in.fieldType(name);
    }

    @Override
    public IndexAnalyzers getIndexAnalyzers() {
        return in.getIndexAnalyzers();
    }

    @Override
    MappedFieldType failIfFieldMappingNotFound(String name, MappedFieldType fieldMapping) {
        return in.failIfFieldMappingNotFound(name, fieldMapping);
    }

    @Override
    public void setAllowUnmappedFields(boolean allowUnmappedFields) {
        in.setAllowUnmappedFields(allowUnmappedFields);
    }

    @Override
    public void setMapUnmappedFieldAsString(boolean mapUnmappedFieldAsString) {
        in.setMapUnmappedFieldAsString(mapUnmappedFieldAsString);
    }

    @Override
    public NamedWriteableRegistry getWriteableRegistry() {
        return in.getWriteableRegistry();
    }

    @Override
    public ValuesSourceRegistry getValuesSourceRegistry() {
        return in.getValuesSourceRegistry();
    }

    @Override
    public boolean allowExpensiveQueries() {
        return in.allowExpensiveQueries();
    }

    @Override
    public boolean hasAsyncActions() {
        return in.hasAsyncActions();
    }

    @Override
    public Index getFullyQualifiedIndex() {
        return in.getFullyQualifiedIndex();
    }

    @Override
    public IndexSettings getIndexSettings() {
        return in.getIndexSettings();
    }

    @Override
    public boolean indexMatches(String pattern) {
        return in.indexMatches(pattern);
    }

    @Override
    public Set<String> getMatchingFieldNames(String pattern) {
        return in.getMatchingFieldNames(pattern);
    }

    @Override
    public void setRewriteToNamedQueries() {
        in.setRewriteToNamedQueries();
    }

    @Override
    public boolean rewriteToNamedQuery() {
        return in.rewriteToNamedQuery();
    }
}
