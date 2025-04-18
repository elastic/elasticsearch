/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
    protected final ScriptCompiler scriptService;
    private final XContentParserConfiguration parserConfiguration;
    protected final Client client;
    protected final LongSupplier nowInMillis;
    private final List<BiConsumer<Client, ActionListener<?>>> asyncActions = new ArrayList<>();
    protected boolean allowUnmappedFields;
    protected boolean mapUnmappedFieldAsString;
    protected Predicate<String> allowedFields;
    private final ResolvedIndices resolvedIndices;
    private final PointInTimeBuilder pit;
    private QueryRewriteInterceptor queryRewriteInterceptor;
    private final boolean isExplain;

    public QueryRewriteContext(
        final XContentParserConfiguration parserConfiguration,
        final Client client,
        final LongSupplier nowInMillis,
        final MapperService mapperService,
        final MappingLookup mappingLookup,
        final Map<String, MappedFieldType> runtimeMappings,
        final IndexSettings indexSettings,
        final Index fullyQualifiedIndex,
        final Predicate<String> indexNameMatcher,
        final NamedWriteableRegistry namedWriteableRegistry,
        final ValuesSourceRegistry valuesSourceRegistry,
        final BooleanSupplier allowExpensiveQueries,
        final ScriptCompiler scriptService,
        final ResolvedIndices resolvedIndices,
        final PointInTimeBuilder pit,
        final QueryRewriteInterceptor queryRewriteInterceptor,
        final boolean isExplain
    ) {

        this.parserConfiguration = parserConfiguration;
        this.client = client;
        this.nowInMillis = nowInMillis;
        this.mapperService = mapperService;
        this.mappingLookup = Objects.requireNonNull(mappingLookup);
        this.allowUnmappedFields = indexSettings == null || indexSettings.isDefaultAllowUnmappedFields();
        this.runtimeMappings = runtimeMappings;
        this.indexSettings = indexSettings;
        this.fullyQualifiedIndex = fullyQualifiedIndex;
        this.indexNameMatcher = indexNameMatcher;
        this.writeableRegistry = namedWriteableRegistry;
        this.valuesSourceRegistry = valuesSourceRegistry;
        this.allowExpensiveQueries = allowExpensiveQueries;
        this.scriptService = scriptService;
        this.resolvedIndices = resolvedIndices;
        this.pit = pit;
        this.queryRewriteInterceptor = queryRewriteInterceptor;
        this.isExplain = isExplain;
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
            null,
            null,
            null,
            false
        );
    }

    public QueryRewriteContext(
        final XContentParserConfiguration parserConfiguration,
        final Client client,
        final LongSupplier nowInMillis,
        final ResolvedIndices resolvedIndices,
        final PointInTimeBuilder pit,
        final QueryRewriteInterceptor queryRewriteInterceptor
    ) {
        this(parserConfiguration, client, nowInMillis, resolvedIndices, pit, queryRewriteInterceptor, false);
    }

    public QueryRewriteContext(
        final XContentParserConfiguration parserConfiguration,
        final Client client,
        final LongSupplier nowInMillis,
        final ResolvedIndices resolvedIndices,
        final PointInTimeBuilder pit,
        final QueryRewriteInterceptor queryRewriteInterceptor,
        final boolean isExplain
    ) {
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
            resolvedIndices,
            pit,
            queryRewriteInterceptor,
            isExplain
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
     * Returns an instance of {@link DataRewriteContext} if available or null otherwise
     */
    public DataRewriteContext convertToDataRewriteContext() {
        return null;
    }

    public PerDocumentQueryRewriteContext convertToPerDocumentQueryRewriteContext() {
        return null;
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
            TextFieldMapper.Builder builder = new TextFieldMapper.Builder(
                name,
                getIndexAnalyzers(),
                getIndexSettings() != null && SourceFieldMapper.isSynthetic(getIndexSettings())
            );
            return builder.build(MapperBuilderContext.root(false, false)).fieldType();
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

    public boolean isExplain() {
        return this.isExplain;
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
    public void executeAsyncActions(ActionListener<Void> listener) {
        if (asyncActions.isEmpty()) {
            listener.onResponse(null);
        } else {
            CountDown countDown = new CountDown(asyncActions.size());
            ActionListener<?> internalListener = new ActionListener<>() {
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
     * Returns the MappingLookup for the queried index.
     */
    public MappingLookup getMappingLookup() {
        return mappingLookup;
    }

    /**
     *  Given an index pattern, checks whether it matches against the current shard. The pattern
     *  may represent a fully qualified index name if the search targets remote shards.
     */
    public boolean indexMatches(String pattern) {
        assert indexNameMatcher != null;
        return indexNameMatcher.test(pattern);
    }

    /**
     * Returns the names of all mapped fields that match a given pattern
     *
     * All names returned by this method are guaranteed to resolve to a
     * MappedFieldType if passed to {@link #getFieldType(String)}
     *
     * @param pattern the field name pattern
     */
    public Set<String> getMatchingFieldNames(String pattern) {
        Set<String> matches;
        if (runtimeMappings.isEmpty()) {
            matches = mappingLookup.getMatchingFieldNames(pattern);
        } else {
            matches = new HashSet<>(mappingLookup.getMatchingFieldNames(pattern));
            if ("*".equals(pattern)) {
                matches.addAll(runtimeMappings.keySet());
            } else if (Regex.isSimpleMatchPattern(pattern) == false) {
                // no wildcard
                if (runtimeMappings.containsKey(pattern)) {
                    matches.add(pattern);
                }
            } else {
                for (String name : runtimeMappings.keySet()) {
                    if (Regex.simpleMatch(pattern, name)) {
                        matches.add(name);
                    }
                }
            }
        }
        // If the field is not allowed, behave as if it is not mapped
        return allowedFields == null ? matches : matches.stream().filter(allowedFields).collect(Collectors.toSet());
    }

    /**
     * @return An {@link Iterable} with key the field name and value the MappedFieldType
     */
    public Iterable<Map.Entry<String, MappedFieldType>> getAllFields() {
        Map<String, MappedFieldType> allFromMapping = mappingLookup.getFullNameToFieldType();
        Set<Map.Entry<String, MappedFieldType>> allEntrySet = allowedFields == null
            ? allFromMapping.entrySet()
            : allFromMapping.entrySet().stream().filter(entry -> allowedFields.test(entry.getKey())).collect(Collectors.toSet());
        if (runtimeMappings.isEmpty()) {
            return allEntrySet;
        }
        Set<Map.Entry<String, MappedFieldType>> runtimeEntrySet = allowedFields == null
            ? runtimeMappings.entrySet()
            : runtimeMappings.entrySet().stream().filter(entry -> allowedFields.test(entry.getKey())).collect(Collectors.toSet());
        // runtime mappings and non-runtime fields don't overlap, so we can simply concatenate the iterables here
        return () -> Iterators.concat(allEntrySet.iterator(), runtimeEntrySet.iterator());
    }

    public ResolvedIndices getResolvedIndices() {
        return resolvedIndices;
    }

    /**
     * Returns the {@link PointInTimeBuilder} used by the search request, or null if not specified.
     */
    @Nullable
    public PointInTimeBuilder getPointInTimeBuilder() {
        return pit;
    }

    /**
     * Retrieve the first tier preference from the index setting. If the setting is not
     * present, then return null.
     */
    @Nullable
    public String getTierPreference() {
        Settings settings = getIndexSettings().getSettings();
        String value = DataTier.TIER_PREFERENCE_SETTING.get(settings);

        if (Strings.hasText(value) == false) {
            return null;
        }

        // Tier preference can be a comma-delimited list of tiers, ordered by preference
        // It was decided we should only test the first of these potentially multiple preferences.
        return value.split(",")[0].trim();
    }

    public QueryRewriteInterceptor getQueryRewriteInterceptor() {
        return queryRewriteInterceptor;
    }

    public void setQueryRewriteInterceptor(QueryRewriteInterceptor queryRewriteInterceptor) {
        this.queryRewriteInterceptor = queryRewriteInterceptor;
    }

}
