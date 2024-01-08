/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.Set;
import java.util.function.BiConsumer;

public class FilteredQueryRewriteContext extends QueryRewriteContext {

    private final QueryRewriteContext in;

    public FilteredQueryRewriteContext(QueryRewriteContext in) {
        super(in);
        this.in = in;
    }

    @Override
    public XContentParserConfiguration getParserConfig() {
        return in.getParserConfig();
    }

    @Override
    public long nowInMillis() {
        return in.nowInMillis();
    }

    @Override
    public SearchExecutionContext convertToSearchExecutionContext() {
        return in.convertToSearchExecutionContext();
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
    public void registerAsyncAction(BiConsumer<Client, ActionListener<?>> asyncAction) {
        in.registerAsyncAction(asyncAction);
    }

    @Override
    public boolean hasAsyncActions() {
        return in.hasAsyncActions();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void executeAsyncActions(ActionListener listener) {
        in.executeAsyncActions(listener);
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
    public Iterable<String> getAllFieldNames() {
        return in.getAllFieldNames();
    }
}
