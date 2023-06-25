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
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version.
 */
public class QueryRewriteContext {
    protected final MapperService mapperService;
    protected final MappingLookup mappingLookup;
    protected final Map<String, MappedFieldType> runtimeMappings;
    protected final IndexSettings indexSettings;
    protected final Index fullyQualifiedIndex;
    protected final Predicate<String> indexNameMatcher;
    protected final NamedWriteableRegistry writeableRegistry;
    protected final ValuesSourceRegistry valuesSourceRegistry;
    protected final BooleanSupplier allowExpensiveQueries;
    protected final ScriptService scriptService;
    private final XContentParserConfiguration parserConfiguration;
    protected final Client client;
    protected final LongSupplier nowInMillis;
    private final List<BiConsumer<Client, ActionListener<?>>> asyncActions = new ArrayList<>();
    protected boolean allowUnmappedFields;
    protected boolean mapUnmappedFieldAsString;
    protected Predicate<String> allowedFields;

    public QueryRewriteContext(
        final XContentParserConfiguration parserConfiguration,
        final Client client,
        final LongSupplier nowInMillis,
        final MapperService mapperService,
        final MappingLookup mappingLookup,
        final Map<String, MappedFieldType> runtimeMappings,
        final Predicate<String> allowedFields,
        final IndexSettings indexSettings,
        final Index fullyQualifiedIndex,
        final Predicate<String> indexNameMatcher,
        final NamedWriteableRegistry namedWriteableRegistry,
        final ValuesSourceRegistry valuesSourceRegistry,
        final BooleanSupplier allowExpensiveQueries,
        final ScriptService scriptService
    ) {

        this.parserConfiguration = parserConfiguration;
        this.client = client;
        this.nowInMillis = nowInMillis;
        this.mapperService = mapperService;
        this.mappingLookup = Objects.requireNonNull(mappingLookup);
        this.allowUnmappedFields = indexSettings == null || indexSettings.isDefaultAllowUnmappedFields();
        this.runtimeMappings = runtimeMappings;
        this.allowedFields = allowedFields;
        this.indexSettings = indexSettings;
        this.fullyQualifiedIndex = fullyQualifiedIndex;
        this.indexNameMatcher = indexNameMatcher;
        this.writeableRegistry = namedWriteableRegistry;
        this.valuesSourceRegistry = valuesSourceRegistry;
        this.allowExpensiveQueries = allowExpensiveQueries;
        this.scriptService = scriptService;
    }

    public QueryRewriteContext(final XContentParserConfiguration parserConfiguration, final Client client, final LongSupplier nowInMillis) {
        this(
            parserConfiguration,
            client,
            nowInMillis,
            null,
            MappingLookup.EMPTY,
            Collections.emptyMap(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    /**
     * The registry used to build new {@link XContentParser}s. Contains registered named parsers needed to parse the query.
     *
     * Used by WrapperQueryBuilder
     */
    public XContentParserConfiguration getParserConfig() {
        return parserConfiguration;
    }

    /**
     * Returns the time in milliseconds that is shared across all resources involved. Even across shards and nodes.
     *
     * Used in date field query rewriting
     */
    public long nowInMillis() {
        return nowInMillis.getAsLong();
    }

    /**
     * Returns an instance of {@link SearchExecutionContext} if available or null otherwise
     */
    public SearchExecutionContext convertToSearchExecutionContext() {
        return null;
    }

    /**
     * Returns an instance of {@link CoordinatorRewriteContext} if available or null otherwise
     */
    public CoordinatorRewriteContext convertToCoordinatorRewriteContext() {
        return null;
    }

    /**
     * @return an {@link QueryRewriteContext} instance that is aware of the mapping and other index metadata or <code>null</code> otherwise.
     */
    public QueryRewriteContext convertToIndexMetadataContext() {
        return mapperService != null ? this : null;
    }

    /**
     * Returns the {@link MappedFieldType} for the provided field name.
     * If the field is not mapped, the behaviour depends on the index.query.parse.allow_unmapped_fields setting, which defaults to true.
     * In case unmapped fields are allowed, null is returned when the field is not mapped.
     * In case unmapped fields are not allowed, either an exception is thrown or the field is automatically mapped as a text field.
     * @throws QueryShardException if unmapped fields are not allowed and automatically mapping unmapped fields as text is disabled.
     * @see QueryRewriteContext#setAllowUnmappedFields(boolean)
     * @see QueryRewriteContext#setMapUnmappedFieldAsString(boolean)
     */
    public MappedFieldType getFieldType(String name) {
        return failIfFieldMappingNotFound(name, fieldType(name));
    }

    protected MappedFieldType fieldType(String name) {
        // If the field is not allowed, behave as if it is not mapped
        if (allowedFields != null && false == allowedFields.test(name)) {
            return null;
        }
        MappedFieldType fieldType = runtimeMappings.get(name);
        return fieldType == null ? mappingLookup.getFieldType(name) : fieldType;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        if (mapperService == null) {
            return IndexAnalyzers.of(Collections.emptyMap());
        }
        return mapperService.getIndexAnalyzers();
    }

    MappedFieldType failIfFieldMappingNotFound(String name, MappedFieldType fieldMapping) {
        if (fieldMapping != null || allowUnmappedFields) {
            return fieldMapping;
        } else if (mapUnmappedFieldAsString) {
            TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, getIndexAnalyzers());
            return builder.build(MapperBuilderContext.root(false)).fieldType();
        } else {
            throw new QueryShardException(this, "No field mapping can be found for the field with name [{}]", name);
        }
    }

    public void setAllowUnmappedFields(boolean allowUnmappedFields) {
        this.allowUnmappedFields = allowUnmappedFields;
    }

    public void setMapUnmappedFieldAsString(boolean mapUnmappedFieldAsString) {
        this.mapUnmappedFieldAsString = mapUnmappedFieldAsString;
    }

    public NamedWriteableRegistry getWriteableRegistry() {
        return writeableRegistry;
    }

    public ValuesSourceRegistry getValuesSourceRegistry() {
        return valuesSourceRegistry;
    }

    public boolean allowExpensiveQueries() {
        assert allowExpensiveQueries != null;
        return allowExpensiveQueries.getAsBoolean();
    }

    /**
     * Registers an async action that must be executed before the next rewrite round in order to make progress.
     * This should be used if a rewriteable needs to fetch some external resources in order to be executed ie. a document
     * from an index.
     */
    public void registerAsyncAction(BiConsumer<Client, ActionListener<?>> asyncAction) {
        asyncActions.add(asyncAction);
    }

    /**
     * Returns <code>true</code> if there are any registered async actions.
     */
    public boolean hasAsyncActions() {
        return asyncActions.isEmpty() == false;
    }

    /**
     * Executes all registered async actions and notifies the listener once it's done. The value that is passed to the listener is always
     * <code>null</code>. The list of registered actions is cleared once this method returns.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void executeAsyncActions(ActionListener listener) {
        if (asyncActions.isEmpty()) {
            listener.onResponse(null);
        } else {
            CountDown countDown = new CountDown(asyncActions.size());
            ActionListener<?> internalListener = new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    if (countDown.countDown()) {
                        listener.onResponse(null);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (countDown.fastForward()) {
                        listener.onFailure(e);
                    }
                }
            };
            // make a copy to prevent concurrent modification exception
            List<BiConsumer<Client, ActionListener<?>>> biConsumers = new ArrayList<>(asyncActions);
            asyncActions.clear();
            for (BiConsumer<Client, ActionListener<?>> action : biConsumers) {
                action.accept(client, internalListener);
            }
        }
    }

    /**
     * Returns the fully qualified index including a remote cluster alias if applicable, and the index uuid
     */
    public Index getFullyQualifiedIndex() {
        return fullyQualifiedIndex;
    }

    /**
     * Returns the index settings for this context. This might return null if the
     * context has not index scope.
     */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     *  Given an index pattern, checks whether it matches against the current shard. The pattern
     *  may represent a fully qualified index name if the search targets remote shards.
     */
    public boolean indexMatches(String pattern) {
        assert indexNameMatcher != null;
        return indexNameMatcher.test(pattern);
    }
}
