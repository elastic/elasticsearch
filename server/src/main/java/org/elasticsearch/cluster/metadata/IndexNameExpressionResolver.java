/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction.Type;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexNameExpressionResolver {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(IndexNameExpressionResolver.class);

    public static final String EXCLUDED_DATA_STREAMS_KEY = "es.excluded_ds";
    public static final IndexVersion SYSTEM_INDEX_ENFORCEMENT_INDEX_VERSION = IndexVersions.V_8_0_0;

    private final ThreadContext threadContext;
    private final SystemIndices systemIndices;

    public IndexNameExpressionResolver(ThreadContext threadContext, SystemIndices systemIndices) {
        this.threadContext = Objects.requireNonNull(threadContext, "Thread Context must not be null");
        this.systemIndices = Objects.requireNonNull(systemIndices, "System Indices must not be null");
    }

    /**
     * Same as {@link #concreteIndexNames(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
        Context context = new Context(
            state,
            request.indicesOptions(),
            false,
            false,
            request.includeDataStreams(),
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        return concreteIndexNames(context, request.indices());
    }

    /**
     * Same as {@link #concreteIndexNames(ClusterState, IndicesRequest)}, but access to system indices is always allowed.
     */
    public String[] concreteIndexNamesWithSystemIndexAccess(ClusterState state, IndicesRequest request) {
        Context context = new Context(
            state,
            request.indicesOptions(),
            false,
            false,
            request.includeDataStreams(),
            SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY,
            Predicates.always(),
            this.getNetNewSystemIndexPredicate()
        );
        return concreteIndexNames(context, request.indices());
    }

    /**
     * Same as {@link #concreteIndices(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request and resolves data streams.
     */
    public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
        Context context = new Context(
            state,
            request.indicesOptions(),
            false,
            false,
            request.includeDataStreams(),
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        return concreteIndices(context, request.indices());
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param state             the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions  expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexNotFoundException if one of the index expressions is pointing to a missing index or alias and the
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case; if a remote index is requested.
     */
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(
            state,
            options,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        return concreteIndexNames(context, indexExpressions);
    }

    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... indexExpressions) {
        Context context = new Context(
            state,
            options,
            false,
            false,
            includeDataStreams,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        return concreteIndexNames(context, indexExpressions);
    }

    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, IndicesRequest request) {
        Context context = new Context(
            state,
            options,
            false,
            false,
            request.includeDataStreams(),
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        return concreteIndexNames(context, request.indices());
    }

    public List<String> dataStreamNames(ClusterState state, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(
            state,
            options,
            false,
            false,
            true,
            true,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        final Collection<ResolvedExpression> expressions = resolveExpressions(context, indexExpressions);
        // TODO: FIXME - Update to return ResolvedExpression
        return expressions.stream()
            .filter(expression -> {
                IndexAbstraction ia = state.metadata().getIndicesLookup().get(expression.resource());
                return ia != null && Type.DATA_STREAM == ia.getType();
            })
            .map(ResolvedExpression::resource)
            .distinct()
            .toList();
    }

    /**
     * Returns {@link IndexAbstraction} instance for the provided write request. This instance isn't fully resolved,
     * meaning that {@link IndexAbstraction#getWriteIndex()} should be invoked in order to get concrete write index.
     *
     * @param state The cluster state
     * @param request The provided write request
     * @return {@link IndexAbstraction} instance for the provided write request
     */
    public IndexAbstraction resolveWriteIndexAbstraction(ClusterState state, DocWriteRequest<?> request) {
        boolean includeDataStreams = request.opType() == DocWriteRequest.OpType.CREATE && request.includeDataStreams();
        Context context = new Context(
            state,
            request.indicesOptions(),
            false,
            false,
            includeDataStreams,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );

        // TODO: FIXME We will likely want to return an abstraction here that respects the selector returned from the expression resolution
        final Collection<ResolvedExpression> expressions = resolveExpressions(context, request.index());

        if (expressions.size() == 1) {
            ResolvedExpression expression = expressions.iterator().next();
            IndexAbstraction ia = state.metadata().getIndicesLookup().get(expression.resource());
            if (ia.getType() == Type.ALIAS) {
                Index writeIndex = ia.getWriteIndex();
                if (writeIndex == null) {
                    throw new IllegalArgumentException(
                        "no write index is defined for alias ["
                            + ia.getName()
                            + "]."
                            + " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple"
                            + " indices without one being designated as a write index"
                    );
                }
            }
            checkSystemIndexAccess(context, Set.of(ia.getWriteIndex()));
            return ia;
        } else {
            throw new IllegalArgumentException(
                "unable to return a single target as the provided expression and options got resolved to multiple targets"
            );
        }
    }

    public record ResolvedExpression(String resource, @Nullable IndexComponentSelector selector) {
        public ResolvedExpression(String indexAbstraction) {
            this(indexAbstraction, null);
        }

        public String combined() {
            return resource + (selector == null ? "" : ("$" + selector.getKey()));
        }
    }

    protected static Collection<ResolvedExpression> resolveExpressions(Context context, String... expressions) {
        if (context.getOptions().expandWildcardExpressions() == false) {
            if (expressions == null
                || expressions.length == 0
                || expressions.length == 1
                    && SelectorResolver.matchesWithOrWithoutSelector(expressions[0], context, Metadata.ALL::equals)) {
                return List.of();
            } else {
                return ExplicitResourceNameFilter.filterUnavailable(
                    context,
                    DateMathExpressionResolver.resolve(context, SelectorResolver.resolve(context, List.of(expressions)))
                );
            }
        } else {
            Predicate<String> isMatchAll = (((Predicate<String>) Metadata.ALL::equals)).or(Regex::isMatchAllPattern);
            if (expressions == null
                || expressions.length == 0
                || expressions.length == 1 && (SelectorResolver.matchesWithOrWithoutSelector(expressions[0], context, isMatchAll))) {
                EnumSet<IndexComponentSelector> selectors = EnumSet.noneOf(IndexComponentSelector.class);
                if (expressions != null && expressions.length == 1) {
                    selectors = SelectorResolver.resolveMatchAllToSelectors(context, expressions[0]);
                }
                return WildcardExpressionResolver.resolveAll(context, selectors);
            } else {
                return WildcardExpressionResolver.resolve(
                    context,
                    ExplicitResourceNameFilter.filterUnavailable(
                        context,
                        DateMathExpressionResolver.resolve(context, SelectorResolver.resolve(context, List.of(expressions)))
                    )
                );
            }
        }
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param state             the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions  expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexNotFoundException if one of the index expressions is pointing to a missing index or alias and the
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case; if a remote index is requested.
     */
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, String... indexExpressions) {
        return concreteIndices(state, options, false, indexExpressions);
    }

    public Index[] concreteIndices(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... indexExpressions) {
        Context context = new Context(
            state,
            options,
            false,
            false,
            includeDataStreams,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        return concreteIndices(context, indexExpressions);
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param state      the cluster state containing all the data to resolve to expressions to concrete indices
     * @param startTime  The start of the request where concrete indices is being invoked for
     * @param request    request containing expressions that can be resolved to alias, index, or data stream names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case; if a remote index is requested.
     */
    public Index[] concreteIndices(ClusterState state, IndicesRequest request, long startTime) {
        Context context = new Context(
            state,
            request.indicesOptions(),
            startTime,
            false,
            false,
            request.includeDataStreams(),
            false,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        return concreteIndices(context, request.indices());
    }

    String[] concreteIndexNames(Context context, String... indexExpressions) {
        Index[] indexes = concreteIndices(context, indexExpressions);
        String[] names = new String[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            names[i] = indexes[i].getName();
        }
        return names;
    }

    Index[] concreteIndices(Context context, String... indexExpressions) {
        final Collection<ResolvedExpression> expressions = resolveExpressions(context, indexExpressions);

        final Set<Index> concreteIndicesResult = Sets.newLinkedHashSetWithExpectedSize(expressions.size());
        final Map<String, IndexAbstraction> indicesLookup = context.getState().metadata().getIndicesLookup();
        for (ResolvedExpression expression : expressions) {
            final IndexAbstraction indexAbstraction = indicesLookup.get(expression.resource());
            assert indexAbstraction != null;
            if (indexAbstraction.getType() == Type.ALIAS && context.isResolveToWriteIndex()) {
                Index writeIndex = indexAbstraction.getWriteIndex();
                if (writeIndex == null) {
                    throw new IllegalArgumentException(
                        "no write index is defined for alias ["
                            + indexAbstraction.getName()
                            + "]."
                            + " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple"
                            + " indices without one being designated as a write index"
                    );
                }
                if (indexAbstraction.isDataStreamRelated()) {
                    DataStream dataStream = indicesLookup.get(indexAbstraction.getWriteIndex().getName()).getParentDataStream();
                    resolveWriteIndexForDataStreams(context, dataStream, concreteIndicesResult, expression.selector());
                } else {
                    if (addIndex(writeIndex, null, context)) {
                        concreteIndicesResult.add(writeIndex);
                    }
                }
            } else if (indexAbstraction.getType() == Type.DATA_STREAM && context.isResolveToWriteIndex()) {
                resolveWriteIndexForDataStreams(context, (DataStream) indexAbstraction, concreteIndicesResult, expression.selector());
            } else {
                if (resolvesToMoreThanOneIndex(indexAbstraction, context, expression)
                    && context.getOptions().allowAliasesToMultipleIndices() == false) {
                    String[] indexNames = new String[indexAbstraction.getIndices().size()];
                    int i = 0;
                    for (Index indexName : indexAbstraction.getIndices()) {
                        indexNames[i++] = indexName.getName();
                    }
                    throw new IllegalArgumentException(
                        indexAbstraction.getType().getDisplayName()
                            + " ["
                            + expression
                            + "] has more than one index associated with it "
                            + Arrays.toString(indexNames)
                            + ", can't execute a single index op"
                    );
                }

                if (indexAbstraction.getType() == Type.DATA_STREAM) {
                    resolveIndicesForDataStream(context, (DataStream) indexAbstraction, concreteIndicesResult, expression.selector());
                } else if (indexAbstraction.getType() == Type.ALIAS
                    && indexAbstraction.isDataStreamRelated()
                    && DataStream.isFailureStoreFeatureFlagEnabled()
                    && IndexComponentSelector.FAILURES.equals(expression.selector())) {
                        for (DataStream dataStream : getAliasDataStreams(indexAbstraction, indicesLookup)) {
                            resolveIndicesForDataStream(context, dataStream, concreteIndicesResult, expression.selector());
                        }
                    } else {
                        for (Index index : indexAbstraction.getIndices()) {
                            if (shouldTrackConcreteIndex(context, context.getOptions(), index)) {
                                concreteIndicesResult.add(index);
                            }
                        }
                    }
            }
        }

        if (context.getOptions().allowNoIndices() == false && concreteIndicesResult.isEmpty()) {
            throw notFoundException(indexExpressions);
        }
        checkSystemIndexAccess(context, concreteIndicesResult);
        return concreteIndicesResult.toArray(Index.EMPTY_ARRAY);
    }

    private static Set<DataStream> getAliasDataStreams(IndexAbstraction indexAbstraction, Map<String, IndexAbstraction> indicesLookup) {
        // Collect the data streams involved with the alias and resolve their failure stores
        assert indexAbstraction.getType().equals(Type.ALIAS) && indexAbstraction.isDataStreamRelated()
            : "Non data stream alias [" + indexAbstraction.getName() + "]";
        Set<DataStream> aliasDataStreams = new HashSet<>();
        for (Index index : indexAbstraction.getIndices()) {
            DataStream parentDataStream = indicesLookup.get(index.getName()).getParentDataStream();
            if (parentDataStream != null) {
                aliasDataStreams.add(parentDataStream);
            }
        }
        return aliasDataStreams;
    }

    private static void resolveIndicesForDataStream(
        Context context,
        DataStream dataStream,
        Set<Index> concreteIndicesResult,
        IndexComponentSelector selector
    ) {
        if (shouldIncludeRegularIndices(context.getOptions(), selector)) {
            for (Index index : dataStream.getIndices()) {
                if (shouldTrackConcreteIndex(context, context.getOptions(), index)) {
                    concreteIndicesResult.add(index);
                }
            }
        }
        if (shouldIncludeFailureIndices(context.getOptions(), selector)) {
            // We short-circuit here, if failure indices are not allowed and they can be skipped
            if (context.getOptions().allowSelectors() || context.getOptions().ignoreUnavailable() == false) {
                for (Index index : dataStream.getFailureIndices().getIndices()) {
                    if (shouldTrackConcreteIndex(context, context.getOptions(), index)) {
                        concreteIndicesResult.add(index);
                    }
                }
            }
        }
    }

    private static void resolveWriteIndexForDataStreams(
        Context context,
        DataStream dataStream,
        Set<Index> concreteIndicesResult,
        IndexComponentSelector selector
    ) {
        if (shouldIncludeRegularIndices(context.getOptions(), selector)) {
            Index writeIndex = dataStream.getWriteIndex();
            if (addIndex(writeIndex, null, context)) {
                concreteIndicesResult.add(writeIndex);
            }
        }
        if (shouldIncludeFailureIndices(context.getOptions(), selector)) {
            Index failureStoreWriteIndex = dataStream.getFailureStoreWriteIndex();
            if (failureStoreWriteIndex != null && addIndex(failureStoreWriteIndex, null, context)) {
                concreteIndicesResult.add(failureStoreWriteIndex);
            }
        }
    }

    private static boolean shouldIncludeRegularIndices(IndicesOptions indicesOptions, IndexComponentSelector expressionSelector) {
        if (indicesOptions.allowSelectors()) {
            if (expressionSelector != null) {
                return IndexComponentSelector.DATA.equals(expressionSelector);
            } else {
                return indicesOptions.selectorOptions().defaultSelectors().contains(IndexComponentSelector.DATA);
            }
        }
        return true;
    }

    private static boolean shouldIncludeFailureIndices(IndicesOptions indicesOptions, IndexComponentSelector expressionSelector) {
        // We return failure indices regardless of whether the data stream actually has the `failureStoreEnabled` flag set to true.
        if (indicesOptions.allowSelectors()) {
            if (expressionSelector != null) {
                return IndexComponentSelector.FAILURES.equals(expressionSelector);
            } else {
                return indicesOptions.selectorOptions().defaultSelectors().contains(IndexComponentSelector.FAILURES);
            }
        }
        return false;
    }

    private static boolean resolvesToMoreThanOneIndex(IndexAbstraction indexAbstraction, Context context, ResolvedExpression expression) {
        if (indexAbstraction.getType() == Type.ALIAS && indexAbstraction.isDataStreamRelated()) {
            int count = 0;
            for (DataStream aliasDataStream : getAliasDataStreams(indexAbstraction, context.state.metadata().getIndicesLookup())) {
                if (shouldIncludeRegularIndices(context.getOptions(), expression.selector())) {
                    count += aliasDataStream.getIndices().size();
                }
                if (shouldIncludeFailureIndices(context.getOptions(), expression.selector())) {
                    count += aliasDataStream.getFailureIndices().getIndices().size();
                }
                if (count > 1) {
                    // Early out if we already have more than one index accounted
                    return true;
                }
            }
            return false;
        }
        if (indexAbstraction.getType() == Type.DATA_STREAM) {
            DataStream dataStream = (DataStream) indexAbstraction;
            int count = 0;
            if (shouldIncludeRegularIndices(context.getOptions(), expression.selector())) {
                count += dataStream.getIndices().size();
            }
            if (shouldIncludeFailureIndices(context.getOptions(), expression.selector())) {
                count += dataStream.getFailureIndices().getIndices().size();
            }
            return count > 1;
        }
        return indexAbstraction.getIndices().size() > 1;
    }

    private void checkSystemIndexAccess(Context context, Set<Index> concreteIndices) {
        final Predicate<String> systemIndexAccessPredicate = context.getSystemIndexAccessPredicate();
        if (systemIndexAccessPredicate == Predicates.<String>always()) {
            return;
        }
        doCheckSystemIndexAccess(context, concreteIndices, systemIndexAccessPredicate);
    }

    private void doCheckSystemIndexAccess(Context context, Set<Index> concreteIndices, Predicate<String> systemIndexAccessPredicate) {
        final Metadata metadata = context.getState().metadata();
        final List<String> resolvedSystemIndices = new ArrayList<>();
        final List<String> resolvedNetNewSystemIndices = new ArrayList<>();
        final Set<String> resolvedSystemDataStreams = new HashSet<>();
        final SortedMap<String, IndexAbstraction> indicesLookup = metadata.getIndicesLookup();
        boolean matchedIndex = false;
        for (Index concreteIndex : concreteIndices) {
            IndexMetadata idxMetadata = metadata.index(concreteIndex);
            String name = concreteIndex.getName();
            if (idxMetadata.isSystem() && systemIndexAccessPredicate.test(name) == false) {
                matchedIndex = true;
                IndexAbstraction indexAbstraction = indicesLookup.get(name);
                if (indexAbstraction.getParentDataStream() != null) {
                    resolvedSystemDataStreams.add(indexAbstraction.getParentDataStream().getName());
                } else if (systemIndices.isNetNewSystemIndex(name)) {
                    resolvedNetNewSystemIndices.add(name);
                } else {
                    resolvedSystemIndices.add(name);
                }
            }
        }
        if (matchedIndex) {
            handleMatchedSystemIndices(resolvedSystemIndices, resolvedSystemDataStreams, resolvedNetNewSystemIndices);
        }
    }

    private void handleMatchedSystemIndices(
        List<String> resolvedSystemIndices,
        Set<String> resolvedSystemDataStreams,
        List<String> resolvedNetNewSystemIndices
    ) {
        if (resolvedSystemIndices.isEmpty() == false) {
            Collections.sort(resolvedSystemIndices);
            deprecationLogger.warn(
                DeprecationCategory.API,
                "open_system_index_access",
                "this request accesses system indices: {}, but in a future major version, direct access to system "
                    + "indices will be prevented by default",
                resolvedSystemIndices
            );
        }
        if (resolvedSystemDataStreams.isEmpty() == false) {
            throw SystemIndices.dataStreamAccessException(threadContext, resolvedSystemDataStreams);
        }
        if (resolvedNetNewSystemIndices.isEmpty() == false) {
            throw SystemIndices.netNewSystemIndexAccessException(threadContext, resolvedNetNewSystemIndices);
        }
    }

    private static IndexNotFoundException notFoundException(String... indexExpressions) {
        final IndexNotFoundException infe;
        if (indexExpressions == null
            || indexExpressions.length == 0
            || (indexExpressions.length == 1 && Metadata.ALL.equals(indexExpressions[0]))) {
            infe = new IndexNotFoundException("no indices exist", Metadata.ALL);
            infe.setResources("index_or_alias", Metadata.ALL);
        } else if (indexExpressions.length == 1) {
            if (indexExpressions[0].startsWith("-")) {
                // this can arise when multi-target syntax is used with an exclusion not in the "inclusion" list, such as
                // "GET test1,test2,-test3"
                // the caller should have put "GET test*,-test3"
                infe = new IndexNotFoundException(
                    "if you intended to exclude this index, ensure that you use wildcards that include it before explicitly excluding it",
                    indexExpressions[0]
                );
            } else {
                infe = new IndexNotFoundException(indexExpressions[0]);
            }
            infe.setResources("index_or_alias", indexExpressions[0]);
        } else {
            infe = new IndexNotFoundException((String) null);
            infe.setResources("index_expression", indexExpressions);
        }
        return infe;
    }

    private static boolean shouldTrackConcreteIndex(Context context, IndicesOptions options, Index index) {
        if (context.systemIndexAccessLevel == SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY
            && context.netNewSystemIndexPredicate.test(index.getName())) {
            // Exclude this one as it's a net-new system index, and we explicitly don't want those.
            return false;
        }
        final IndexMetadata imd = context.state.metadata().index(index);
        if (imd.getState() == IndexMetadata.State.CLOSE) {
            if (options.forbidClosedIndices() && options.ignoreUnavailable() == false) {
                throw new IndexClosedException(index);
            } else {
                return options.forbidClosedIndices() == false && addIndex(index, imd, context);
            }
        } else if (imd.getState() == IndexMetadata.State.OPEN) {
            return addIndex(index, imd, context);
        } else {
            throw new IllegalStateException("index state [" + index + "] not supported");
        }
    }

    private static boolean addIndex(Index index, IndexMetadata imd, Context context) {
        // This used to check the `index.search.throttled` setting, but we eventually decided that it was
        // trappy to hide throttled indices by default. In order to avoid breaking backward compatibility,
        // we changed it to look at the `index.frozen` setting instead, since frozen indices were the only
        // type of index to use the `search_throttled` threadpool at that time.
        // NOTE: We can't reference the Setting object, which is only defined and registered in x-pack.
        if (context.options.ignoreThrottled()) {
            imd = imd != null ? imd : context.state.metadata().index(index);
            return imd.getSettings().getAsBoolean("index.frozen", false) == false;
        } else {
            return true;
        }
    }

    private static IllegalArgumentException aliasesNotSupportedException(String expression) {
        return new IllegalArgumentException(
            "The provided expression [" + expression + "] matches an " + "alias, specify the corresponding concrete indices instead."
        );
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single concrete index.
     * Callers should make sure they provide proper {@link org.elasticsearch.action.support.IndicesOptions}
     * that require a single index as a result. The indices resolution must in fact return a single index when
     * using this method, an {@link IllegalArgumentException} gets thrown otherwise.
     *
     * @param state             the cluster state containing all the data to resolve to expression to a concrete index
     * @param request           The request that defines how the an alias or an index need to be resolved to a concrete index
     *                          and the expression that can be resolved to an alias or an index name.
     * @throws IllegalArgumentException if the index resolution returns more than one index; if a remote index is requested.
     * @return the concrete index obtained as a result of the index resolution
     */
    public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
        String indexExpression = CollectionUtils.isEmpty(request.indices()) ? null : request.indices()[0];
        Index[] indices = concreteIndices(state, request.indicesOptions(), indexExpression);
        if (indices.length != 1) {
            throw new IllegalArgumentException(
                "unable to return a single index as the index and options" + " provided got resolved to multiple indices"
            );
        }
        return indices[0];
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single write index.
     *
     * @param state             the cluster state containing all the data to resolve to expression to a concrete index
     * @param request           The request that defines how the an alias or an index need to be resolved to a concrete index
     *                          and the expression that can be resolved to an alias or an index name.
     * @throws IllegalArgumentException if the index resolution does not lead to an index, or leads to more than one index
     * @return the write index obtained as a result of the index resolution
     */
    public Index concreteWriteIndex(ClusterState state, IndicesRequest request) {
        if (request.indices() == null || (request.indices() != null && request.indices().length != 1)) {
            throw new IllegalArgumentException("indices request must specify a single index expression");
        }
        return concreteWriteIndex(state, request.indicesOptions(), request.indices()[0], false, request.includeDataStreams());
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single write index.
     *
     * @param state             the cluster state containing all the data to resolve to expression to a concrete index
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param index             index that can be resolved to alias or index name.
     * @param allowNoIndices    whether to allow resolve to no index
     * @param includeDataStreams Whether data streams should be included in the evaluation.
     * @throws IllegalArgumentException if the index resolution does not lead to an index, or leads to more than one index, as well as
     * if a remote index is requested.
     * @return the write index obtained as a result of the index resolution or null if no index
     */
    public Index concreteWriteIndex(
        ClusterState state,
        IndicesOptions options,
        String index,
        boolean allowNoIndices,
        boolean includeDataStreams
    ) {
        IndicesOptions combinedOptions = IndicesOptions.fromOptions(
            options.ignoreUnavailable(),
            allowNoIndices,
            options.expandWildcardsOpen(),
            options.expandWildcardsClosed(),
            options.expandWildcardsHidden(),
            options.allowAliasesToMultipleIndices(),
            options.forbidClosedIndices(),
            options.ignoreAliases(),
            options.ignoreThrottled()
        );

        Context context = new Context(
            state,
            combinedOptions,
            false,
            true,
            includeDataStreams,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        Index[] indices = concreteIndices(context, index);
        if (allowNoIndices && indices.length == 0) {
            return null;
        }
        if (indices.length != 1) {
            throw new IllegalArgumentException(
                "The index expression [" + index + "] and options provided did not point to a single write-index"
            );
        }
        return indices[0];
    }

    /**
     * @return whether the specified index, data stream or alias exists.
     *         If the data stream, index or alias contains date math then that is resolved too.
     */
    public boolean hasIndexAbstraction(String indexAbstraction, ClusterState state) {
        String resolvedAliasOrIndex = DateMathExpressionResolver.resolveExpression(indexAbstraction);
        return state.metadata().hasIndexAbstraction(resolvedAliasOrIndex);
    }

    /**
     * @return If the specified string is data math expression then this method returns the resolved expression.
     */
    public static String resolveDateMathExpression(String dateExpression) {
        return DateMathExpressionResolver.resolveExpression(dateExpression);
    }

    /**
     * @param time instant to consider when parsing the expression
     * @return If the specified string is data math expression then this method returns the resolved expression.
     */
    public static String resolveDateMathExpression(String dateExpression, long time) {
        return DateMathExpressionResolver.resolveExpression(dateExpression, () -> time);
    }

    /**
     * Resolve an array of expressions to the set of indices and aliases that these expressions match.
     */
    public Set<ResolvedExpression> resolveExpressions(ClusterState state, String... expressions) {
        return resolveExpressions(state, IndicesOptions.lenientExpandOpen(), false, expressions);
    }

    /**
     * Resolve the expression to the set of indices, aliases, and, optionally, datastreams that the expression matches.
     * If {@param preserveDataStreams} is {@code true}, datastreams that are covered by the wildcards from the
     * {@param expressions} are returned as-is, without expanding them further to their respective backing indices.
     */
    public Set<ResolvedExpression> resolveExpressions(
        ClusterState state,
        IndicesOptions indicesOptions,
        boolean preserveDataStreams,
        String... expressions
    ) {
        Context context = new Context(
            state,
            indicesOptions,
            true,
            false,
            true,
            preserveDataStreams,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        Collection<ResolvedExpression> resolved = resolveExpressions(context, expressions);
        if (resolved instanceof Set<ResolvedExpression>) {
            // unmodifiable without creating a new collection as it might contain many items
            return Collections.unmodifiableSet((Set<ResolvedExpression>) resolved);
        } else {
            return Set.copyOf(resolved);
        }
    }

    /**
     * Iterates through the list of indices and selects the effective list of filtering aliases for the
     * given index.
     * <p>Only aliases with filters are returned. If the indices list contains a non-filtering reference to
     * the index itself - null is returned. Returns {@code null} if no filtering is required.
     * <b>NOTE</b>: The provided expressions must have been resolved already via {@link #resolveExpressions}.
     */
    public String[] filteringAliases(ClusterState state, String index, Set<ResolvedExpression> resolvedExpressions) {
        return indexAliases(state, index, AliasMetadata::filteringRequired, DataStreamAlias::filteringRequired, false, resolvedExpressions);
    }

    /**
     * Whether to generate the candidate set from index aliases, or from the set of resolved expressions.
     * @param indexAliasesSize        the number of aliases of the index
     * @param resolvedExpressionsSize the number of resolved expressions
     */
    // pkg-private for testing
    boolean iterateIndexAliases(int indexAliasesSize, int resolvedExpressionsSize) {
        return indexAliasesSize <= resolvedExpressionsSize;
    }

    /**
     * Iterates through the list of indices and selects the effective list of required aliases for the given index.
     * <p>Only aliases where the given predicate tests successfully are returned. If the indices list contains a non-required reference to
     * the index itself - null is returned. Returns {@code null} if no filtering is required.
     * <p><b>NOTE</b>: the provided expressions must have been resolved already via {@link #resolveExpressions}.
     */
    public String[] indexAliases(
        ClusterState state,
        String index,
        Predicate<AliasMetadata> requiredAlias,
        Predicate<DataStreamAlias> requiredDataStreamAlias,
        boolean skipIdentity,
        Set<ResolvedExpression> resolvedExpressions
    ) {
        if (isAllIndices(resolvedExpressions)) {
            return null;
        }

        final IndexMetadata indexMetadata = state.metadata().getIndices().get(index);
        if (indexMetadata == null) {
            // Shouldn't happen
            throw new IndexNotFoundException(index);
        }

        if (skipIdentity == false) {
            if (resolvedExpressions.contains(new ResolvedExpression(index))) {
                return null;
            }
            for (IndexComponentSelector selector : IndexComponentSelector.values()) {
                if (resolvedExpressions.contains(new ResolvedExpression(index, selector))) {
                    return null;
                }
            }
        }

        IndexAbstraction ia = state.metadata().getIndicesLookup().get(index);
        DataStream dataStream = ia.getParentDataStream();
        if (dataStream != null) {
            // Determine which data stream indices this index is from - Multiple aliases with different filters that refer to the same data
            // stream could be present in the expression list using different selectors, so find which selectors would be valid for this
            // index and filter the expressions down to just those that match the expected selector.
            // e.g. alias-1$data,alias-2$failures ---> alias-1 filters only used for its data streams' backing indices, alias-2 filters only
            // used for its data streams' failure indices
            final IndexComponentSelector expectedSelector;
            if (dataStream.getBackingIndices().containsIndex(index)) {
                expectedSelector = IndexComponentSelector.DATA;
            } else if (dataStream.getFailureIndices().containsIndex(index)) {
                expectedSelector = IndexComponentSelector.FAILURES;
            } else {
                expectedSelector = null;
            }
            assert expectedSelector != null
                : "Could not locate index [" + index + "] in any of the data stream indices for [" + dataStream.getName() + "]";
            if (skipIdentity == false) {
                // skip the filters when the request targets the data stream name + selector directly
                if (IndexComponentSelector.DATA.equals(expectedSelector)
                    && (resolvedExpressions.contains(new ResolvedExpression(dataStream.getName()))
                        || resolvedExpressions.contains(new ResolvedExpression(dataStream.getName(), IndexComponentSelector.DATA)))) {
                    // account for selectors being disabled by checking for null selector,
                    // though unlikely to be the case in a search operation
                    return null;
                }
                if (IndexComponentSelector.FAILURES.equals(expectedSelector)
                    && resolvedExpressions.contains(new ResolvedExpression(dataStream.getName(), IndexComponentSelector.FAILURES))) {
                    return null;
                }
            }
            // Again, account for selectors being disabled by checking for null selectors in the following filters.
            boolean selectorNullable = IndexComponentSelector.DATA.equals(expectedSelector);
            Map<String, DataStreamAlias> dataStreamAliases = state.metadata().dataStreamAliases();
            List<DataStreamAlias> aliasesForDataStream;
            if (iterateIndexAliases(dataStreamAliases.size(), resolvedExpressions.size())) {
                aliasesForDataStream = dataStreamAliases.values()
                    .stream()
                    .filter(
                        dataStreamAlias -> resolvedExpressions.contains(new ResolvedExpression(dataStreamAlias.getName(), expectedSelector))
                            || (selectorNullable && resolvedExpressions.contains(new ResolvedExpression(dataStreamAlias.getName())))
                    )
                    .filter(dataStreamAlias -> dataStreamAlias.getDataStreams().contains(dataStream.getName()))
                    .toList();
            } else {
                aliasesForDataStream = resolvedExpressions.stream()
                    .filter(
                        expression -> expectedSelector.equals(expression.selector()) || (selectorNullable && expression.selector() == null)
                    )
                    .map(dataStreamAliases::get)
                    .filter(dataStreamAlias -> dataStreamAlias != null && dataStreamAlias.getDataStreams().contains(dataStream.getName()))
                    .toList();
            }

            List<String> requiredAliases = null;
            for (DataStreamAlias dataStreamAlias : aliasesForDataStream) {
                if (requiredDataStreamAlias.test(dataStreamAlias)) {
                    if (requiredAliases == null) {
                        requiredAliases = new ArrayList<>(aliasesForDataStream.size());
                    }
                    requiredAliases.add(dataStreamAlias.getName());
                } else {
                    // we have a non-required alias for this data stream so no need to check further
                    return null;
                }
            }
            if (requiredAliases == null) {
                return null;
            }
            return requiredAliases.toArray(Strings.EMPTY_ARRAY);
        } else {
            final Map<String, AliasMetadata> indexAliases = indexMetadata.getAliases();
            final AliasMetadata[] aliasCandidates;
            if (iterateIndexAliases(indexAliases.size(), resolvedExpressions.size())) {
                // faster to iterate indexAliases
                aliasCandidates = indexAliases.values()
                    .stream()
                    .filter(
                        // Indices can only be referenced with a data selector, or a null selector if selectors are disabled
                        aliasMetadata -> resolvedExpressions.contains(new ResolvedExpression(aliasMetadata.alias()))
                            || resolvedExpressions.contains(new ResolvedExpression(aliasMetadata.alias(), IndexComponentSelector.DATA))
                    )
                    .toArray(AliasMetadata[]::new);
            } else {
                // faster to iterate resolvedExpressions
                aliasCandidates = resolvedExpressions.stream()
                    .map(expression -> indexAliases.get(expression.resource))
                    .filter(Objects::nonNull)
                    .toArray(AliasMetadata[]::new);
            }
            List<String> aliases = null;
            for (AliasMetadata aliasMetadata : aliasCandidates) {
                if (requiredAlias.test(aliasMetadata)) {
                    // If required - add it to the list of aliases
                    if (aliases == null) {
                        aliases = new ArrayList<>();
                    }
                    aliases.add(aliasMetadata.alias());
                } else {
                    // If not, we have a non required alias for this index - no further checking needed
                    return null;
                }
            }
            if (aliases == null) {
                return null;
            }
            return aliases.toArray(Strings.EMPTY_ARRAY);
        }
    }

    /**
     * Resolves the search routing if in the expression aliases are used. If expressions point to concrete indices
     * or aliases with no routing defined the specified routing is used.
     *
     * @return routing values grouped by concrete index
     */
    public Map<String, Set<String>> resolveSearchRouting(ClusterState state, @Nullable String routing, String... expressions) {
        Context context = new Context(
            state,
            IndicesOptions.lenientExpandOpen(),
            false,
            false,
            true,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        final Collection<ResolvedExpression> resolvedExpressions = resolveExpressions(context, expressions);

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();
        if (routing != null) {
            paramRouting = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
        }

        for (ResolvedExpression expression : resolvedExpressions) {
            IndexAbstraction indexAbstraction = state.metadata().getIndicesLookup().get(expression.resource());
            if (indexAbstraction != null && indexAbstraction.getType() == Type.ALIAS) {
                // Determine which set of indices to resolve for the alias
                List<Index> aliasIndices = indexAbstraction.getIndices();
                if (context.getOptions().allowSelectors() && IndexComponentSelector.FAILURES.equals(expression.selector())) {
                    Set<DataStream> dataStreams = getAliasDataStreams(indexAbstraction, context.state.metadata().getIndicesLookup());
                    aliasIndices = new ArrayList<>(dataStreams.size());
                    for (DataStream dataStream : dataStreams) {
                        aliasIndices.addAll(dataStream.getFailureIndices().getIndices());
                    }
                }
                for (Index index : aliasIndices) {
                    String concreteIndex = index.getName();
                    if (norouting.contains(concreteIndex) == false) {
                        AliasMetadata aliasMetadata = state.metadata().index(concreteIndex).getAliases().get(indexAbstraction.getName());
                        if (aliasMetadata != null && aliasMetadata.searchRoutingValues().isEmpty() == false) {
                            // Routing alias
                            if (routings == null) {
                                routings = new HashMap<>();
                            }
                            Set<String> r = routings.computeIfAbsent(concreteIndex, k -> new HashSet<>());
                            r.addAll(aliasMetadata.searchRoutingValues());
                            if (paramRouting != null) {
                                r.retainAll(paramRouting);
                            }
                            if (r.isEmpty()) {
                                routings.remove(concreteIndex);
                            }
                        } else {
                            // Non-routing alias
                            routings = collectRoutings(routings, paramRouting, norouting, concreteIndex);
                        }
                    }
                }
            } else if (indexAbstraction != null && indexAbstraction.getType() == Type.DATA_STREAM) {
                DataStream dataStream = (DataStream) indexAbstraction;
                if (dataStream.isAllowCustomRouting() == false) {
                    continue;
                }
                if (shouldIncludeRegularIndices(context.getOptions(), expression.selector())) {
                    if (dataStream.getIndices() != null) {
                        for (Index index : dataStream.getIndices()) {
                            String concreteIndex = index.getName();
                            routings = collectRoutings(routings, paramRouting, norouting, concreteIndex);
                        }
                    }
                }
                if (shouldIncludeFailureIndices(context.getOptions(), expression.selector())) {
                    if (dataStream.getFailureIndices().getIndices() != null) {
                        for (Index failureIndex : dataStream.getFailureIndices().getIndices()) {
                            String concreteIndex = failureIndex.getName();
                            routings = collectRoutings(routings, paramRouting, norouting, concreteIndex);
                        }
                    }
                }
            } else {
                // Index
                assert expression.selector() == null || IndexComponentSelector.DATA.equals(expression.selector())
                    : "Concrete index is being resolved with a selector other than [data] which is illegal";
                routings = collectRoutings(routings, paramRouting, norouting, expression.resource());
            }

        }
        if (routings == null || routings.isEmpty()) {
            return null;
        }
        return routings;
    }

    @Nullable
    private static Map<String, Set<String>> collectRoutings(
        @Nullable Map<String, Set<String>> routings,
        @Nullable Set<String> paramRouting,
        Set<String> noRouting,
        String concreteIndex
    ) {
        if (noRouting.add(concreteIndex)) {
            if (paramRouting != null) {
                if (routings == null) {
                    routings = new HashMap<>();
                }
                routings.put(concreteIndex, new HashSet<>(paramRouting));
            } else if (routings != null) {
                routings.remove(concreteIndex);
            }
        }
        return routings;
    }

    /**
     * Sets the same routing for all indices
     */
    public static Map<String, Set<String>> resolveSearchRoutingAllIndices(Metadata metadata, String routing) {
        if (routing != null) {
            Set<String> r = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
            Map<String, Set<String>> routings = new HashMap<>();
            String[] concreteIndices = metadata.getConcreteAllIndices();
            for (String index : concreteIndices) {
                routings.put(index, r);
            }
            return routings;
        }
        return null;
    }

    /**
     * Identifies whether the array containing index names given as argument refers to all indices
     * The empty or null array identifies all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array maps to all indices, false otherwise
     */
    public static boolean isAllIndices(Collection<ResolvedExpression> aliasesOrIndices) {
        return aliasesOrIndices == null || aliasesOrIndices.isEmpty() || isExplicitAllPattern(aliasesOrIndices);
    }

    /**
     * Identifies whether the array containing index names given as argument explicitly refers to all indices
     * The empty or null array doesn't explicitly map to all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array explicitly maps to all indices, false otherwise
     */
    static boolean isExplicitAllPattern(Collection<ResolvedExpression> aliasesOrIndices) {
        return aliasesOrIndices != null
            && aliasesOrIndices.size() == 1
            && Metadata.ALL.equals(aliasesOrIndices.iterator().next().resource());
    }

    public SystemIndexAccessLevel getSystemIndexAccessLevel() {
        final SystemIndexAccessLevel accessLevel = SystemIndices.getSystemIndexAccessLevel(threadContext);
        assert accessLevel != SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY
            : "BACKWARDS_COMPATIBLE_ONLY access level should never be used automatically, it should only be used in known special cases";
        return accessLevel;
    }

    public Predicate<String> getSystemIndexAccessPredicate() {
        final SystemIndexAccessLevel systemIndexAccessLevel = getSystemIndexAccessLevel();
        final Predicate<String> systemIndexAccessLevelPredicate;
        if (systemIndexAccessLevel == SystemIndexAccessLevel.NONE) {
            systemIndexAccessLevelPredicate = Predicates.never();
        } else if (systemIndexAccessLevel == SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY) {
            systemIndexAccessLevelPredicate = getNetNewSystemIndexPredicate();
        } else if (systemIndexAccessLevel == SystemIndexAccessLevel.ALL) {
            systemIndexAccessLevelPredicate = Predicates.always();
        } else {
            // everything other than allowed should be included in the deprecation message
            systemIndexAccessLevelPredicate = systemIndices.getProductSystemIndexNamePredicate(threadContext);
        }
        return systemIndexAccessLevelPredicate;
    }

    public Automaton getSystemNameAutomaton() {
        return systemIndices.getSystemNameAutomaton();
    }

    public Predicate<String> getNetNewSystemIndexPredicate() {
        return systemIndices::isNetNewSystemIndex;
    }

    public static class Context {

        private final ClusterState state;
        private final IndicesOptions options;
        private final long startTime;
        private final boolean preserveAliases;
        private final boolean resolveToWriteIndex;
        private final boolean includeDataStreams;
        private final boolean preserveDataStreams;
        private final SystemIndexAccessLevel systemIndexAccessLevel;
        private final Predicate<String> systemIndexAccessPredicate;
        private final Predicate<String> netNewSystemIndexPredicate;

        Context(ClusterState state, IndicesOptions options, SystemIndexAccessLevel systemIndexAccessLevel) {
            this(state, options, systemIndexAccessLevel, Predicates.always(), Predicates.never());
        }

        Context(
            ClusterState state,
            IndicesOptions options,
            SystemIndexAccessLevel systemIndexAccessLevel,
            Predicate<String> systemIndexAccessPredicate,
            Predicate<String> netNewSystemIndexPredicate
        ) {
            this(
                state,
                options,
                System.currentTimeMillis(),
                systemIndexAccessLevel,
                systemIndexAccessPredicate,
                netNewSystemIndexPredicate
            );
        }

        Context(
            ClusterState state,
            IndicesOptions options,
            boolean preserveAliases,
            boolean resolveToWriteIndex,
            boolean includeDataStreams,
            SystemIndexAccessLevel systemIndexAccessLevel,
            Predicate<String> systemIndexAccessPredicate,
            Predicate<String> netNewSystemIndexPredicate
        ) {
            this(
                state,
                options,
                System.currentTimeMillis(),
                preserveAliases,
                resolveToWriteIndex,
                includeDataStreams,
                false,
                systemIndexAccessLevel,
                systemIndexAccessPredicate,
                netNewSystemIndexPredicate
            );
        }

        Context(
            ClusterState state,
            IndicesOptions options,
            boolean preserveAliases,
            boolean resolveToWriteIndex,
            boolean includeDataStreams,
            boolean preserveDataStreams,
            SystemIndexAccessLevel systemIndexAccessLevel,
            Predicate<String> systemIndexAccessPredicate,
            Predicate<String> netNewSystemIndexPredicate
        ) {
            this(
                state,
                options,
                System.currentTimeMillis(),
                preserveAliases,
                resolveToWriteIndex,
                includeDataStreams,
                preserveDataStreams,
                systemIndexAccessLevel,
                systemIndexAccessPredicate,
                netNewSystemIndexPredicate
            );
        }

        Context(
            ClusterState state,
            IndicesOptions options,
            long startTime,
            SystemIndexAccessLevel systemIndexAccessLevel,
            Predicate<String> systemIndexAccessPredicate,
            Predicate<String> netNewSystemIndexPredicate
        ) {
            this(
                state,
                options,
                startTime,
                false,
                false,
                false,
                false,
                systemIndexAccessLevel,
                systemIndexAccessPredicate,
                netNewSystemIndexPredicate
            );
        }

        protected Context(
            ClusterState state,
            IndicesOptions options,
            long startTime,
            boolean preserveAliases,
            boolean resolveToWriteIndex,
            boolean includeDataStreams,
            boolean preserveDataStreams,
            SystemIndexAccessLevel systemIndexAccessLevel,
            Predicate<String> systemIndexAccessPredicate,
            Predicate<String> netNewSystemIndexPredicate
        ) {
            this.state = state;
            this.options = options;
            this.startTime = startTime;
            this.preserveAliases = preserveAliases;
            this.resolveToWriteIndex = resolveToWriteIndex;
            this.includeDataStreams = includeDataStreams;
            this.preserveDataStreams = preserveDataStreams;
            this.systemIndexAccessLevel = systemIndexAccessLevel;
            this.systemIndexAccessPredicate = systemIndexAccessPredicate;
            this.netNewSystemIndexPredicate = netNewSystemIndexPredicate;
        }

        public ClusterState getState() {
            return state;
        }

        public IndicesOptions getOptions() {
            return options;
        }

        public long getStartTime() {
            return startTime;
        }

        /**
         * This is used to prevent resolving aliases to concrete indices but this also means
         * that we might return aliases that point to a closed index. This is currently only used
         * by {@link #filteringAliases(ClusterState, String, Set)} since it's the only one that needs aliases
         */
        boolean isPreserveAliases() {
            return preserveAliases;
        }

        /**
         * This is used to require that aliases resolve to their write-index. It is currently not used in conjunction
         * with <code>preserveAliases</code>.
         */
        boolean isResolveToWriteIndex() {
            return resolveToWriteIndex;
        }

        public boolean includeDataStreams() {
            return includeDataStreams;
        }

        public boolean isPreserveDataStreams() {
            return preserveDataStreams;
        }

        /**
         * Used to determine system index access is allowed in this context (e.g. for this request).
         */
        public Predicate<String> getSystemIndexAccessPredicate() {
            return systemIndexAccessPredicate;
        }
    }

    /**
     * Resolves alias/index name expressions with wildcards into the corresponding concrete indices/aliases
     */
    static final class WildcardExpressionResolver {

        private WildcardExpressionResolver() {
            // Utility class
        }

        /**
         * Returns all the indices, datastreams, and aliases, considering the open/closed, system, and hidden context parameters.
         * Depending on the context, returns the names of the datastreams themselves or their backing indices.
         */
        public static Collection<ResolvedExpression> resolveAll(Context context, EnumSet<IndexComponentSelector> selectors) {
            List<ResolvedExpression> concreteIndices = resolveEmptyOrTrivialWildcard(context, selectors);

            if (context.includeDataStreams() == false && context.getOptions().ignoreAliases()) {
                return concreteIndices;
            }

            Stream<Tuple<IndexAbstraction, IndexComponentSelector>> ias = context.getState()
                .metadata()
                .getIndicesLookup()
                .values()
                .stream()
                .filter(ia -> context.getOptions().expandWildcardsHidden() || ia.isHidden() == false)
                .filter(ia -> shouldIncludeIfDataStream(ia, context) || shouldIncludeIfAlias(ia, context))
                .filter(ia -> ia.isSystem() == false || context.systemIndexAccessPredicate.test(ia.getName()))
                .flatMap(ia -> {
                    // We propagate the selectors here because expandToOpenClosed needs them when it
                    // converts any data streams into concrete indices for verifying they are open/closed/etc...
                    if (context.options.allowSelectors()) {
                        if ((ia.getType() == Type.ALIAS && ia.isDataStreamRelated()) || ia.getType() == Type.DATA_STREAM) {
                            // Aliases can handle both $data and $failures iff they contain data streams
                            // Data streams can always handle both $data and $failures
                            return selectors.stream().map(selector -> new Tuple<>(ia, selector));
                        } else {
                            // Everything else only supports $data,
                            if (selectors.contains(IndexComponentSelector.DATA)) {
                                return Stream.of(new Tuple<>(ia, IndexComponentSelector.DATA));
                            } else {
                                return Stream.empty();
                            }
                        }
                    } else {
                        // No selectors present, set it null
                        return Stream.of(new Tuple<>(ia, null));
                    }
                });

            Set<ResolvedExpression> resolved = expandToOpenClosed(context, ias).collect(Collectors.toSet());
            resolved.addAll(concreteIndices);
            return resolved;
        }

        private static boolean shouldIncludeIfDataStream(IndexAbstraction ia, IndexNameExpressionResolver.Context context) {
            return context.includeDataStreams() && ia.getType() == Type.DATA_STREAM;
        }

        private static boolean shouldIncludeIfAlias(IndexAbstraction ia, IndexNameExpressionResolver.Context context) {
            return context.getOptions().ignoreAliases() == false && ia.getType() == Type.ALIAS;
        }

        /**
         * Returns all the existing resource (index, alias and datastream) names that the {@param expressions} list resolves to.
         * The passed-in {@param expressions} can contain wildcards and exclusions, as well as plain resource names.
         * <br>
         * The return is a {@code Collection} (usually a {@code Set} but can also be a {@code List}, for performance reasons) of plain
         * resource names only. All the returned resources are "accessible", in the given context, i.e. the resources exist
         * and are not an alias or a datastream if the context does not permit it.
         * Wildcard expressions, depending on the context:
         * <ol>
         *   <li>might throw an exception if they don't resolve to anything</li>
         *   <li>might not resolve to hidden or system resources (but plain names can refer to hidden or system resources)</li>
         *   <li>might resolve to aliases and datastreams, and it could be (depending on the context) that their backing indices are what's
         * ultimately returned, instead of the alias or datastream name</li>
         * </ol>
         */
        public static Collection<ResolvedExpression> resolve(Context context, List<ResolvedExpression> expressions) {
            ExpressionList expressionList = new ExpressionList(context, expressions);
            // fast exit if there are no wildcards to evaluate
            if (expressionList.hasWildcard() == false) {
                return expressions;
            }
            Set<ResolvedExpression> result = new HashSet<>();
            for (ExpressionList.Expression expression : expressionList) {
                if (expression.isWildcard()) {
                    Stream<IndexAbstraction> matchingResources = matchResourcesToWildcard(context, expression.get());
                    Stream<Tuple<IndexAbstraction, IndexComponentSelector>> matchingSelectedResources = expandSelectors(
                        context,
                        expression,
                        matchingResources
                    );
                    Stream<ResolvedExpression> matchingOpenClosedNames = expandToOpenClosed(context, matchingSelectedResources);
                    AtomicBoolean emptyWildcardExpansion = new AtomicBoolean(false);
                    if (context.getOptions().allowNoIndices() == false) {
                        emptyWildcardExpansion.set(true);
                        matchingOpenClosedNames = matchingOpenClosedNames.peek(x -> emptyWildcardExpansion.set(false));
                    }
                    if (expression.isExclusion()) {
                        matchingOpenClosedNames.forEachOrdered(result::remove);
                    } else {
                        matchingOpenClosedNames.forEachOrdered(result::add);
                    }
                    if (emptyWildcardExpansion.get()) {
                        throw notFoundException(expression.get());
                    }
                } else {
                    if (expression.isExclusion()) {
                        result.remove(new ResolvedExpression(expression.get(), expression.expression().selector()));
                    } else {
                        result.add(expression.expression());
                    }
                }
            }
            return result;
        }

        private static IndexMetadata.State excludeState(IndicesOptions options) {
            final IndexMetadata.State excludeState;
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                excludeState = null;
            } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed() == false) {
                excludeState = IndexMetadata.State.CLOSE;
            } else if (options.expandWildcardsClosed() && options.expandWildcardsOpen() == false) {
                excludeState = IndexMetadata.State.OPEN;
            } else {
                assert false : "this shouldn't get called if wildcards expand to none";
                excludeState = null;
            }
            return excludeState;
        }

        /**
         * Given a single wildcard {@param expression}, return the {@code Stream} that contains all the resources (i.e. indices, aliases,
         * and datastreams), that exist in the cluster at this moment in time, and that the wildcard "resolves" to (i.e. the resource's
         * name matches the {@param expression} wildcard).
         * The {@param context} provides the current time-snapshot view of cluster state, as well as conditions
         * on whether to consider alias, datastream, system, and hidden resources.
         * It does NOT consider the open or closed status of index resources.
         */
        private static Stream<IndexAbstraction> matchResourcesToWildcard(Context context, String wildcardExpression) {
            assert isWildcard(wildcardExpression);
            final SortedMap<String, IndexAbstraction> indicesLookup = context.getState().getMetadata().getIndicesLookup();
            Stream<IndexAbstraction> matchesStream;
            if (Regex.isSuffixMatchPattern(wildcardExpression)) {
                // this is an initial pre-filtering in the case where the expression is a common suffix wildcard, eg "test*"
                matchesStream = filterIndicesLookupForSuffixWildcard(indicesLookup, wildcardExpression).values().stream();
            } else {
                matchesStream = indicesLookup.values().stream();
                if (Regex.isMatchAllPattern(wildcardExpression) == false) {
                    matchesStream = matchesStream.filter(
                        indexAbstraction -> Regex.simpleMatch(wildcardExpression, indexAbstraction.getName())
                    );
                }
            }
            if (context.getOptions().ignoreAliases()) {
                matchesStream = matchesStream.filter(indexAbstraction -> indexAbstraction.getType() != Type.ALIAS);
            }
            if (context.includeDataStreams() == false) {
                matchesStream = matchesStream.filter(indexAbstraction -> indexAbstraction.isDataStreamRelated() == false);
            }
            // historic, i.e. not net-new, system indices are included irrespective of the system access predicate
            // the system access predicate is based on the endpoint kind and HTTP request headers that identify the stack feature
            matchesStream = matchesStream.filter(
                indexAbstraction -> indexAbstraction.isSystem() == false
                    || (indexAbstraction.getType() != Type.DATA_STREAM
                        && indexAbstraction.getParentDataStream() == null
                        && context.netNewSystemIndexPredicate.test(indexAbstraction.getName()) == false)
                    || context.systemIndexAccessPredicate.test(indexAbstraction.getName())
            );
            if (context.getOptions().expandWildcardsHidden() == false) {
                if (wildcardExpression.startsWith(".")) {
                    // there is this behavior that hidden indices that start with "." are not hidden if the wildcard expression also
                    // starts with "."
                    matchesStream = matchesStream.filter(
                        indexAbstraction -> indexAbstraction.isHidden() == false || indexAbstraction.getName().startsWith(".")
                    );
                } else {
                    matchesStream = matchesStream.filter(indexAbstraction -> indexAbstraction.isHidden() == false);
                }
            }
            return matchesStream;
        }

        private static Map<String, IndexAbstraction> filterIndicesLookupForSuffixWildcard(
            SortedMap<String, IndexAbstraction> indicesLookup,
            String suffixWildcardExpression
        ) {
            assert Regex.isSuffixMatchPattern(suffixWildcardExpression);
            String fromPrefix = suffixWildcardExpression.substring(0, suffixWildcardExpression.length() - 1);
            char[] toPrefixCharArr = fromPrefix.toCharArray();
            toPrefixCharArr[toPrefixCharArr.length - 1]++;
            String toPrefix = new String(toPrefixCharArr);
            return indicesLookup.subMap(fromPrefix, toPrefix);
        }

        private static Stream<Tuple<IndexAbstraction, IndexComponentSelector>> expandSelectors(
            Context context,
            ExpressionList.Expression expression,
            Stream<IndexAbstraction> resources
        ) {
            if (context.options.allowSelectors()) {
                assert expression.expression().selector() != null
                    : "Earlier logic should have parsed selectors or added the default selectors already";
                IndexComponentSelector selector = expression.expression().selector();
                // Filter out any incompatibilities between the wildcard matches and the selectors for the expression.
                return resources
                    // Let all data stream related abstractions through since they support all selectors presently
                    // Anything not data stream related should not be returned if we are selecting a failure component
                    .filter(ia -> ia.isDataStreamRelated() || IndexComponentSelector.FAILURES.equals(selector) == false)
                    .map(ia -> new Tuple<>(ia, selector));
            } else {
                return resources.map(i -> new Tuple<>(i, null));
            }
        }

        /**
         * Return the {@code Stream} of open and/or closed index names for the given {@param resources}.
         * Data streams and aliases are interpreted to refer to multiple indices,
         * then all index resources are filtered by their open/closed status.
         */
        private static Stream<ResolvedExpression> expandToOpenClosed(
            Context context,
            Stream<Tuple<IndexAbstraction, IndexComponentSelector>> resources
        ) {
            final IndexMetadata.State excludeState = excludeState(context.getOptions());
            return resources.flatMap(tuple -> {
                IndexAbstraction indexAbstraction = tuple.v1();
                IndexComponentSelector selector = tuple.v2();
                if (context.isPreserveAliases() && indexAbstraction.getType() == Type.ALIAS) {
                    return Stream.of(new ResolvedExpression(indexAbstraction.getName(), selector));
                } else if (context.isPreserveDataStreams() && indexAbstraction.getType() == Type.DATA_STREAM) {
                    return Stream.of(new ResolvedExpression(indexAbstraction.getName(), selector));
                } else {
                    Stream<IndexMetadata> indicesStateStream = Stream.of();
                    if (shouldIncludeRegularIndices(context.getOptions(), selector)) {
                        indicesStateStream = indexAbstraction.getIndices().stream().map(context.state.metadata()::index);
                    }
                    if (shouldIncludeFailureIndices(context.getOptions(), selector)) {
                        if (indexAbstraction.getType() == Type.ALIAS && indexAbstraction.isDataStreamRelated()) {
                            Set<DataStream> aliasDataStreams = getAliasDataStreams(
                                indexAbstraction,
                                context.state.metadata().getIndicesLookup()
                            );
                            indicesStateStream = Stream.concat(
                                indicesStateStream,
                                aliasDataStreams.stream()
                                    .flatMap(ds -> ds.getFailureIndices().getIndices().stream())
                                    .map(context.state.metadata()::index)
                            );
                        } else if (indexAbstraction.getType() == Type.DATA_STREAM) {
                            DataStream dataStream = (DataStream) indexAbstraction;
                            indicesStateStream = Stream.concat(
                                indicesStateStream,
                                dataStream.getFailureIndices().getIndices().stream().map(context.state.metadata()::index)
                            );
                        }
                    }
                    if (excludeState != null) {
                        indicesStateStream = indicesStateStream.filter(indexMeta -> indexMeta.getState() != excludeState);
                    }
                    // After resolving this abstraction to its concrete indices, mark those concrete indices with the $data selector if
                    // selectors are allowed. This is because indices cannot handle anything other than $data, and it's possible that we
                    // surfaced index results from abstractions that support other selectors.
                    return indicesStateStream.map(
                        indexMeta -> new ResolvedExpression(
                            indexMeta.getIndex().getName(),
                            context.options.allowSelectors() ? IndexComponentSelector.DATA : null
                        )
                    );
                }
            });
        }

        private static List<ResolvedExpression> resolveEmptyOrTrivialWildcard(
            Context context,
            EnumSet<IndexComponentSelector> selectors
        ) {
            final String[] allIndices = resolveEmptyOrTrivialWildcardToAllIndices(
                context.getOptions(),
                context.getState().metadata(),
                selectors
            );
            Stream<String> indicesStream;
            if (context.systemIndexAccessLevel == SystemIndexAccessLevel.ALL) {
                indicesStream = Arrays.stream(allIndices);
            } else {
                indicesStream = resolveEmptyOrTrivialWildcardWithAllowedSystemIndices(context, allIndices);
            }
            if (context.options.allowSelectors()) {
                // These only have values if the $data selector was in the enum set, and they only support the $data selector
                return indicesStream.map(idx -> new ResolvedExpression(idx, IndexComponentSelector.DATA)).toList();
            } else {
                return indicesStream.map(ResolvedExpression::new).toList();
            }
        }

        private static Stream<String> resolveEmptyOrTrivialWildcardWithAllowedSystemIndices(Context context, String[] allIndices) {
            return Arrays.stream(allIndices).filter(name -> {
                if (name.startsWith(".")) {
                    IndexAbstraction abstraction = context.state.metadata().getIndicesLookup().get(name);
                    assert abstraction != null : "null abstraction for " + name + " but was in array of all indices";
                    if (abstraction.isSystem()) {
                        if (context.netNewSystemIndexPredicate.test(name)) {
                            if (SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY.equals(context.systemIndexAccessLevel)) {
                                return false;
                            } else {
                                return context.systemIndexAccessPredicate.test(name);
                            }
                        } else if (abstraction.getType() == Type.DATA_STREAM || abstraction.getParentDataStream() != null) {
                            return context.systemIndexAccessPredicate.test(name);
                        }
                    } else {
                        return true;
                    }
                }
                return true;
            });
        }

        private static String[] resolveEmptyOrTrivialWildcardToAllIndices(
            IndicesOptions options,
            Metadata metadata,
            EnumSet<IndexComponentSelector> selectors
        ) {
            if (selectors.contains(IndexComponentSelector.DATA) == false) {
                return Strings.EMPTY_ARRAY;
            }
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
                return metadata.getConcreteAllIndices();
            } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                return metadata.getConcreteVisibleIndices();
            } else if (options.expandWildcardsOpen() && options.expandWildcardsHidden()) {
                return metadata.getConcreteAllOpenIndices();
            } else if (options.expandWildcardsOpen()) {
                return metadata.getConcreteVisibleOpenIndices();
            } else if (options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
                return metadata.getConcreteAllClosedIndices();
            } else if (options.expandWildcardsClosed()) {
                return metadata.getConcreteVisibleClosedIndices();
            } else {
                return Strings.EMPTY_ARRAY;
            }
        }
    }

    public static final class DateMathExpressionResolver {

        private static final DateFormatter DEFAULT_DATE_FORMATTER = DateFormatter.forPattern("uuuu.MM.dd");
        private static final String EXPRESSION_LEFT_BOUND = "<";
        private static final String EXPRESSION_RIGHT_BOUND = ">";
        private static final char LEFT_BOUND = '{';
        private static final char RIGHT_BOUND = '}';
        private static final char ESCAPE_CHAR = '\\';
        private static final char TIME_ZONE_BOUND = '|';

        private DateMathExpressionResolver() {
            // utility class
        }

        public static List<ResolvedExpression> resolve(Context context, List<ResolvedExpression> expressions) {
            List<ResolvedExpression> result = new ArrayList<>(expressions.size());
            for (ExpressionList.Expression expression : new ExpressionList(context, expressions)) {
                result.add(resolveExpression(expression, context::getStartTime));
            }
            return result;
        }

        static String resolveExpression(String expression) {
            return resolveExpression(expression, System::currentTimeMillis);
        }

        static ResolvedExpression resolveExpression(ExpressionList.Expression expression, LongSupplier getTime) {
            String result;
            if (expression.isExclusion()) {
                // accepts date-math exclusions that are of the form "-<...{}>", i.e. the "-" is outside the "<>" date-math template
                result = "-" + resolveExpression(expression.get(), getTime);
            } else {
                result = resolveExpression(expression.get(), getTime);
            }
            return new ResolvedExpression(result, expression.expression().selector());
        }

        static String resolveExpression(String expression, LongSupplier getTime) {
            if (expression.startsWith(EXPRESSION_LEFT_BOUND) == false || expression.endsWith(EXPRESSION_RIGHT_BOUND) == false) {
                return expression;
            }
            return doResolveExpression(expression, getTime);
        }

        @SuppressWarnings("fallthrough")
        private static String doResolveExpression(String expression, LongSupplier getTime) {
            boolean escape = false;
            boolean inDateFormat = false;
            boolean inPlaceHolder = false;
            final StringBuilder beforePlaceHolderSb = new StringBuilder();
            StringBuilder inPlaceHolderSb = new StringBuilder();
            final char[] text = expression.toCharArray();
            final int from = 1;
            final int length = text.length - 1;
            for (int i = from; i < length; i++) {
                boolean escapedChar = escape;
                if (escape) {
                    escape = false;
                }

                char c = text[i];
                if (c == ESCAPE_CHAR) {
                    if (escapedChar) {
                        beforePlaceHolderSb.append(c);
                        escape = false;
                    } else {
                        escape = true;
                    }
                    continue;
                }
                if (inPlaceHolder) {
                    switch (c) {
                        case LEFT_BOUND:
                            if (inDateFormat && escapedChar) {
                                inPlaceHolderSb.append(c);
                            } else if (inDateFormat == false) {
                                inDateFormat = true;
                                inPlaceHolderSb.append(c);
                            } else {
                                throw new ElasticsearchParseException(
                                    "invalid dynamic name expression [{}]." + " invalid character in placeholder at position [{}]",
                                    new String(text, from, length),
                                    i
                                );
                            }
                            break;

                        case RIGHT_BOUND:
                            if (inDateFormat && escapedChar) {
                                inPlaceHolderSb.append(c);
                            } else if (inDateFormat) {
                                inDateFormat = false;
                                inPlaceHolderSb.append(c);
                            } else {
                                String inPlaceHolderString = inPlaceHolderSb.toString();
                                int dateTimeFormatLeftBoundIndex = inPlaceHolderString.indexOf(LEFT_BOUND);
                                String mathExpression;
                                String dateFormatterPattern;
                                DateFormatter dateFormatter;
                                final ZoneId timeZone;
                                if (dateTimeFormatLeftBoundIndex < 0) {
                                    mathExpression = inPlaceHolderString;
                                    dateFormatter = DEFAULT_DATE_FORMATTER;
                                    timeZone = ZoneOffset.UTC;
                                } else {
                                    if (inPlaceHolderString.lastIndexOf(RIGHT_BOUND) != inPlaceHolderString.length() - 1) {
                                        throw new ElasticsearchParseException(
                                            "invalid dynamic name expression [{}]. missing closing `}`" + " for date math format",
                                            inPlaceHolderString
                                        );
                                    }
                                    if (dateTimeFormatLeftBoundIndex == inPlaceHolderString.length() - 2) {
                                        throw new ElasticsearchParseException(
                                            "invalid dynamic name expression [{}]. missing date format",
                                            inPlaceHolderString
                                        );
                                    }
                                    mathExpression = inPlaceHolderString.substring(0, dateTimeFormatLeftBoundIndex);
                                    String patternAndTZid = inPlaceHolderString.substring(
                                        dateTimeFormatLeftBoundIndex + 1,
                                        inPlaceHolderString.length() - 1
                                    );
                                    int formatPatternTimeZoneSeparatorIndex = patternAndTZid.indexOf(TIME_ZONE_BOUND);
                                    if (formatPatternTimeZoneSeparatorIndex != -1) {
                                        dateFormatterPattern = patternAndTZid.substring(0, formatPatternTimeZoneSeparatorIndex);
                                        timeZone = DateUtils.of(patternAndTZid.substring(formatPatternTimeZoneSeparatorIndex + 1));
                                    } else {
                                        dateFormatterPattern = patternAndTZid;
                                        timeZone = ZoneOffset.UTC;
                                    }
                                    dateFormatter = DateFormatter.forPattern(dateFormatterPattern);
                                }

                                DateFormatter formatter = dateFormatter.withZone(timeZone);
                                DateMathParser dateMathParser = formatter.toDateMathParser();
                                Instant instant = dateMathParser.parse(mathExpression, getTime, false, timeZone);

                                String time = formatter.format(instant);
                                beforePlaceHolderSb.append(time);
                                inPlaceHolderSb = new StringBuilder();
                                inPlaceHolder = false;
                            }
                            break;

                        default:
                            inPlaceHolderSb.append(c);
                    }
                } else {
                    switch (c) {
                        case LEFT_BOUND:
                            if (escapedChar) {
                                beforePlaceHolderSb.append(c);
                            } else {
                                inPlaceHolder = true;
                            }
                            break;

                        case RIGHT_BOUND:
                            if (escapedChar == false) {
                                throw new ElasticsearchParseException(
                                    "invalid dynamic name expression [{}]."
                                        + " invalid character at position [{}]. `{` and `}` are reserved characters and"
                                        + " should be escaped when used as part of the index name using `\\` (e.g. `\\{text\\}`)",
                                    new String(text, from, length),
                                    i
                                );
                            }
                        default:
                            beforePlaceHolderSb.append(c);
                    }
                }
            }

            if (inPlaceHolder) {
                throw new ElasticsearchParseException(
                    "invalid dynamic name expression [{}]. date math placeholder is open ended",
                    new String(text, from, length)
                );
            }
            if (beforePlaceHolderSb.length() == 0) {
                throw new ElasticsearchParseException("nothing captured");
            }
            return beforePlaceHolderSb.toString();
        }
    }

    public static final class ExplicitResourceNameFilter {

        private ExplicitResourceNameFilter() {
            // Utility class
        }

        /**
         * Returns an expression list with "unavailable" (missing or not acceptable) resource names filtered out.
         * Only explicit resource names are considered for filtering. Wildcard and exclusion expressions are kept in.
         */
        public static List<ResolvedExpression> filterUnavailable(Context context, List<ResolvedExpression> expressions) {
            ensureRemoteIndicesRequireIgnoreUnavailable(context.getOptions(), expressions);
            List<ResolvedExpression> result = new ArrayList<>(expressions.size());
            for (ExpressionList.Expression expression : new ExpressionList(context, expressions)) {
                validateAliasOrIndex(expression);
                if (expression.isWildcard() || expression.isExclusion() || ensureAliasOrIndexExists(context, expression)) {
                    result.add(expression.expression());
                }
            }
            return result;
        }

        /**
         * This returns `true` if the given {@param name} is of a resource that exists.
         * Otherwise, it returns `false` if the `ignore_unvailable` option is `true`, or, if `false`, it throws a "not found" type of
         * exception.
         */
        @Nullable
        private static boolean ensureAliasOrIndexExists(Context context, ExpressionList.Expression expression) {
            String name = expression.get();
            boolean ignoreUnavailable = context.getOptions().ignoreUnavailable();
            IndexAbstraction indexAbstraction = context.getState().getMetadata().getIndicesLookup().get(name);
            if (indexAbstraction == null) {
                if (ignoreUnavailable) {
                    return false;
                } else {
                    throw notFoundException(name);
                }
            }
            // treat aliases as unavailable indices when ignoreAliases is set to true (e.g. delete index and update aliases api)
            if (indexAbstraction.getType() == Type.ALIAS && context.getOptions().ignoreAliases()) {
                if (ignoreUnavailable) {
                    return false;
                } else {
                    throw aliasesNotSupportedException(name);
                }
            }
            if (indexAbstraction.isDataStreamRelated() && context.includeDataStreams() == false) {
                if (ignoreUnavailable) {
                    return false;
                } else {
                    IndexNotFoundException infe = notFoundException(name);
                    // Allows callers to handle IndexNotFoundException differently based on whether data streams were excluded.
                    infe.addMetadata(EXCLUDED_DATA_STREAMS_KEY, "true");
                    throw infe;
                }
            }
            if (context.options.allowSelectors()) {
                // Ensure that the selectors are present and that they are compatible with the abstractions they are used with
                IndexComponentSelector selector = expression.expression().selector();
                assert selector != null : "Earlier logic should have parsed selectors or added the default selectors already";
                if (indexAbstraction.isDataStreamRelated() == false && IndexComponentSelector.FAILURES.equals(selector)) {
                    // If you aren't data stream related you cannot use $failures
                    if (ignoreUnavailable) {
                        return false;
                    } else {
                        // Return the expression with the selector on it since the selector is the part that is incorrect
                        throw notFoundException(expression.getWithSelector());
                    }
                }
            }
            return true;
        }

        private static void validateAliasOrIndex(ExpressionList.Expression expression) {
            if (Strings.isEmpty(expression.expression().resource())) {
                throw notFoundException(expression.get());
            }
            // Expressions can not start with an underscore. This is reserved for APIs. If the check gets here, the API
            // does not exist and the path is interpreted as an expression. If the expression begins with an underscore,
            // throw a specific error that is different from the [[IndexNotFoundException]], which is typically thrown
            // if the expression can't be found.
            if (expression.expression().resource().charAt(0) == '_') {
                throw new InvalidIndexNameException(expression.get(), "must not start with '_'.");
            }
        }

        private static void ensureRemoteIndicesRequireIgnoreUnavailable(IndicesOptions options, List<ResolvedExpression> indexExpressions) {
            if (options.ignoreUnavailable()) {
                return;
            }
            for (ResolvedExpression expression : indexExpressions) {
                String index = expression.resource();
                if (index.contains(":")) {
                    failOnRemoteIndicesNotIgnoringUnavailable(indexExpressions);
                }
            }
        }

        private static void failOnRemoteIndicesNotIgnoringUnavailable(List<ResolvedExpression> indexExpressions) {
            List<String> crossClusterIndices = new ArrayList<>();
            for (ResolvedExpression expression : indexExpressions) {
                String index = expression.resource();
                if (index.contains(":")) {
                    crossClusterIndices.add(expression.combined());
                }
            }
            throw new IllegalArgumentException(
                "Cross-cluster calls are not supported in this context but remote indices were requested: " + crossClusterIndices
            );
        }
    }

    public static final class SelectorResolver {
        private SelectorResolver() {
            // Utility class
        }

        /**
         * Parses the given expressions for selector suffixes. If any suffixes are present and supported by the index options, the
         * expression and its suffix are split apart and returned in a pair. If a suffix is not present on the expression, the default
         * selectors are retrieved from the context and combined with the expression.
         * @param context Context object
         * @param expressions The expressions to check for selectors
         * @return A list of resolved expressions, each optionally paired with a selector if present and supported.
         */
        public static List<ResolvedExpression> resolve(Context context, List<String> expressions) {
            if (context.options.allowSelectors()) {
                return expressions.stream()
                    .flatMap(expression -> resolve(context, expression))
                    .toList();
            } else {
                return expressions.stream().map(ResolvedExpression::new).toList();
            }
        }

        /**
         * Parses an index expression for selector suffixes. If a suffix is present and supported by the index options, the suffix is
         * returned in singleton set. If a suffix is not present on the expression, the default set of selectors are returned. If selectors
         * are disabled in the options, this will return an empty set.
         * @param context Context object
         * @param matchAllExpression The match all expression given to the index request (e.g. `*`, `*$failures`, `_all$data`)
         * @return A set containing the selectors for this match all expression
         */
        public static EnumSet<IndexComponentSelector> resolveMatchAllToSelectors(Context context, String matchAllExpression) {
            if (context.options.allowSelectors()) {
                return SelectorResolver.resolve(context, matchAllExpression)
                    .map(ResolvedExpression::selector)
                    .filter(Objects::nonNull)
                    .collect(() -> EnumSet.noneOf(IndexComponentSelector.class), EnumSet::add, EnumSet::addAll);
            } else {
                return EnumSet.noneOf(IndexComponentSelector.class);
            }
        }

        /**
         * Parses an index expression for selector suffixes. If a suffix is present and supported by the index options, the
         * expression and its suffix are split apart and returned in a pair. If a suffix is not present on the expression, the default
         * selectors are retrieved from the context and combined with the expression.
         * @param context Context object
         * @param expression The expression to check for selectors
         * @return A resolved expression, optionally paired with a selector if present and supported.
         */
        public static Stream<ResolvedExpression> resolve(Context context, String expression) {
            if (context.options.allowSelectors()) {
                return parseAndConsumeSelector(
                    expression,
                    (baseExpression, maybeSelector) -> Optional.ofNullable(maybeSelector)
                        .map(Stream::of)
                        .orElseGet(
                            // if selector was not present in the expression, use the defaults for the API
                            () -> context.options.selectorOptions().defaultSelectors().stream()
                        )
                        .map(selector -> new ResolvedExpression(baseExpression, selector))
                );
            } else {
                return Stream.of(new ResolvedExpression(expression));
            }
        }

        /**
         * Determines if the given expression matches the provided predicate. If selectors are allowed and the test is negative, the test is
         * repeated on just the index-part if the given expression has a valid selector suffix.
         * @param expression Index expression that may contain a selector suffix.
         * @param context Context object.
         * @param predicate Determines match criteria. May be called multiple times if expression contains a valid selector.
         * @return true if the expression matches the predicate, with or without its selector suffix if one is present.
         */
        private static boolean matchesWithOrWithoutSelector(String expression, Context context, Predicate<String> predicate) {
            if (expression == null) {
                return false;
            }
            if (predicate.test(expression)) {
                return true;
            }
            if (context.options.allowSelectors()) {
                // Parse the suffix. Skip evaluating predicate if selector suffix is null since
                // we already evaluated the base case in the previous conditional
                return parseAndConsumeSelector(expression, (base, maybeSelector) -> maybeSelector != null && predicate.test(base));
            }
            return false;
        }

        private static <V> V parseAndConsumeSelector(String expression, BiFunction<String, IndexComponentSelector, V> resultFunction) {
            int lastDollarSign = expression.lastIndexOf("$");
            if (lastDollarSign >= 0) {
                String suffix = expression.substring(lastDollarSign + 1);
                IndexComponentSelector selector = IndexComponentSelector.getByKey(suffix);
                if (selector != null) {
                    String expressionBase = expression.substring(0, lastDollarSign);
                    return resultFunction.apply(expressionBase, selector);
                }
            }
            // Otherwise accept the default
            return resultFunction.apply(expression, null);
        }
    }

    /**
     * Used to iterate expression lists and work out which expression item is a wildcard or an exclusion.
     */
    public static final class ExpressionList implements Iterable<ExpressionList.Expression> {
        private final List<Expression> expressionsList;
        private final boolean hasWildcard;

        public record Expression(ResolvedExpression expression, boolean isWildcard, boolean isExclusion) {
            public String get() {
                if (isExclusion()) {
                    // drop the leading "-" if exclusion because it is easier for callers to handle it like this
                    return expression().resource().substring(1);
                } else {
                    return expression().resource();
                }
            }

            public String getWithSelector() {
                return get() + (expression().selector() == null ? "" : ("$" + expression().selector().getKey()));
            }
        }

        /**
         * Creates the expression iterable that can be used to easily check which expression item is a wildcard or an exclusion (or both).
         * The {@param context} is used to check if wildcards ought to be considered or not.
         */
        public ExpressionList(Context context, List<ResolvedExpression> expressionStrings) {
            List<Expression> expressionsList = new ArrayList<>(expressionStrings.size());
            boolean wildcardSeen = false;
            for (ResolvedExpression resolvedExpression : expressionStrings) {
                String expressionString = resolvedExpression.resource;
                boolean isExclusion = expressionString.startsWith("-") && wildcardSeen;
                if (context.getOptions().expandWildcardExpressions() && isWildcard(expressionString)) {
                    wildcardSeen = true;
                    expressionsList.add(new Expression(resolvedExpression, true, isExclusion));
                } else {
                    expressionsList.add(new Expression(resolvedExpression, false, isExclusion));
                }
            }
            this.expressionsList = expressionsList;
            this.hasWildcard = wildcardSeen;
        }

        /**
         * Returns {@code true} if the expression contains any wildcard and the options allow wildcard expansion
         */
        public boolean hasWildcard() {
            return this.hasWildcard;
        }

        @Override
        public Iterator<ExpressionList.Expression> iterator() {
            return expressionsList.iterator();
        }
    }

    /**
     * This is a context for the DateMathExpressionResolver which does not require {@code IndicesOptions} or {@code ClusterState}
     * since it uses only the start time to resolve expressions.
     */
    public static final class ResolverContext extends Context {
        public ResolverContext() {
            this(System.currentTimeMillis());
        }

        public ResolverContext(long startTime) {
            super(null, null, startTime, false, false, false, false, SystemIndexAccessLevel.ALL, Predicates.never(), Predicates.never());
        }

        @Override
        public ClusterState getState() {
            throw new UnsupportedOperationException("should never be called");
        }

        @Override
        public IndicesOptions getOptions() {
            throw new UnsupportedOperationException("should never be called");
        }
    }

    private static boolean isWildcard(String expression) {
        return Regex.isSimpleMatchPattern(expression);
    }
}
