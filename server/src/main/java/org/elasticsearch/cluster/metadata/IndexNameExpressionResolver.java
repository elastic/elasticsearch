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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.UnsupportedSelectorException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction.Type;
import org.elasticsearch.cluster.project.ProjectResolver;
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
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.transport.RemoteClusterAware;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This class main focus is to resolve multi-syntax target expressions to resources or concrete indices. This resolution is influenced
 * by IndicesOptions and other flags passed through the method call. Examples of the functionality it provides:
 * - Resolve expressions to concrete indices
 * - Resolve expressions to data stream names
 * - Resolve expressions to resources (meaning indices, data streams and aliases)
 * Note: This class is performance sensitive, so we pay extra attention on the data structure usage and we avoid streams and iterators
 * when possible in favor of the classic for-i loops.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class IndexNameExpressionResolver {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(IndexNameExpressionResolver.class);

    public static final String EXCLUDED_DATA_STREAMS_KEY = "es.excluded_ds";
    public static final IndexVersion SYSTEM_INDEX_ENFORCEMENT_INDEX_VERSION = IndexVersions.V_8_0_0;

    private static final BiPredicate<DataStreamAlias, Boolean> ALL_DATA_STREAM_ALIASES = (ignoredAlias, ignoredIsData) -> true;
    // Alias filters are not applied against indices in an abstraction's failure component.
    // They do not match the mapping of the data stream nor are the documents mapped for searching.
    private static final BiPredicate<DataStreamAlias, Boolean> ONLY_FILTERING_DATA_STREAM_ALIASES = (
        dataStreamAlias,
        isData) -> dataStreamAlias.filteringRequired() && isData;

    private final ThreadContext threadContext;
    private final SystemIndices systemIndices;
    private final ProjectResolver projectResolver;

    public IndexNameExpressionResolver(ThreadContext threadContext, SystemIndices systemIndices, ProjectResolver projectResolver) {
        this.threadContext = Objects.requireNonNull(threadContext, "Thread Context must not be null");
        this.systemIndices = Objects.requireNonNull(systemIndices, "System Indices must not be null");
        this.projectResolver = Objects.requireNonNull(projectResolver, "Project Resolver must not be null");
    }

    /**
     * This represents a resolved expression in the form of the name of a resource in the cluster and a potential selector
     * which defines which part of the resource the expression is targeting.
     * @param resource the name of a resource that an expression refers to.
     * @param selector optionally indicates which part of a resource to target during an operation. A null value indicates selectors are
     *                 unsupported in an API and the default data component of the expression should be used where applicable.
     */
    public record ResolvedExpression(String resource, @Nullable IndexComponentSelector selector) {
        public ResolvedExpression(String indexAbstraction) {
            this(indexAbstraction, (IndexComponentSelector) null);
        }

        /**
         * Constructs a ResolvedExpression with the DATA selector if the selectors are allowed
         * or null otherwise.
         * @param indexAbstraction
         * @param options
         */
        public ResolvedExpression(String indexAbstraction, IndicesOptions options) {
            this(indexAbstraction, options.allowSelectors() ? IndexComponentSelector.DATA : null);
        }

        public String combined() {
            return combineSelector(resource, selector);
        }
    }

    /**
     * Same as {@link #concreteIndexNames(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
        return concreteIndexNames(projectResolver.getProjectMetadata(state), request);
    }

    /**
     * Same as {@link #concreteIndexNames(ProjectMetadata, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public String[] concreteIndexNames(ProjectMetadata project, IndicesRequest request) {
        Context context = new Context(
            project,
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
        return concreteIndexNamesWithSystemIndexAccess(projectResolver.getProjectMetadata(state), request);
    }

    /**
     * Same as {@link #concreteIndexNames(ProjectMetadata, IndicesRequest)}, but access to system indices is always allowed.
     */
    public String[] concreteIndexNamesWithSystemIndexAccess(ProjectMetadata project, IndicesRequest request) {
        Context context = new Context(
            project,
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
        return concreteIndices(projectResolver.getProjectMetadata(state), request);
    }

    /**
     * Same as {@link #concreteIndices(ProjectMetadata, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request and resolves data streams.
     */
    public Index[] concreteIndices(ProjectMetadata projectMetadata, IndicesRequest request) {
        Context context = new Context(
            projectMetadata,
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
        return concreteIndexNames(state.metadata().getProject(projectResolver.getProjectId()), options, indexExpressions);
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param project          the project metadata containing all the data to resolve to expressions to concrete indices
     * @param options          defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexNotFoundException   if one of the index expressions is pointing to a missing index or alias and the
     *  provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     *  contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     *   indices options in the context don't allow such a case; if a remote index is requested.
     */
    public String[] concreteIndexNames(ProjectMetadata project, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(
            project,
            options,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        return concreteIndexNames(context, indexExpressions);
    }

    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... indexExpressions) {
        return concreteIndexNames(
            state.metadata().getProject(projectResolver.getProjectId()),
            options,
            includeDataStreams,
            indexExpressions
        );
    }

    public String[] concreteIndexNames(
        ProjectMetadata project,
        IndicesOptions options,
        boolean includeDataStreams,
        String... indexExpressions
    ) {
        Context context = new Context(
            project,
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
        return concreteIndexNames(state.metadata().getProject(projectResolver.getProjectId()), options, request);
    }

    public String[] concreteIndexNames(ProjectMetadata project, IndicesOptions options, IndicesRequest request) {
        Context context = new Context(
            project,
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
        return dataStreamNames(projectResolver.getProjectMetadata(state), options, indexExpressions);
    }

    public List<String> dataStreamNames(ProjectMetadata project, IndicesOptions options, String... indexExpressions) {
        return dataStreams(project, options, indexExpressions).stream().map(ResolvedExpression::resource).distinct().toList();
    }

    public List<ResolvedExpression> dataStreams(ClusterState state, IndicesOptions options, String... indexExpressions) {
        return dataStreams(projectResolver.getProjectMetadata(state), options, indexExpressions);
    }

    public List<ResolvedExpression> dataStreams(ProjectMetadata project, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(
            project,
            options,
            false,
            false,
            true,
            true,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        final Collection<ResolvedExpression> expressions = resolveExpressionsToResources(context, indexExpressions);
        return expressions.stream().filter(expression -> {
            IndexAbstraction ia = project.getIndicesLookup().get(expression.resource());
            return ia != null && Type.DATA_STREAM == ia.getType();
        }).toList();
    }

    /**
     * Returns {@link IndexAbstraction} instance for the provided write request. This instance isn't fully resolved,
     * meaning that {@link IndexAbstraction#getWriteIndex()} should be invoked in order to get concrete write index.
     *
     * @param project The project
     * @param request The provided write request
     * @return {@link IndexAbstraction} instance for the provided write request
     */
    public IndexAbstraction resolveWriteIndexAbstraction(ProjectMetadata project, DocWriteRequest<?> request) {
        boolean includeDataStreams = request.opType() == DocWriteRequest.OpType.CREATE && request.includeDataStreams();
        Context context = new Context(
            project,
            request.indicesOptions(),
            false,
            false,
            includeDataStreams,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );

        final Collection<ResolvedExpression> expressions = resolveExpressionsToResources(context, request.index());

        if (expressions.size() == 1) {
            ResolvedExpression resolvedExpression = expressions.iterator().next();
            IndexAbstraction ia = project.getIndicesLookup().get(resolvedExpression.resource());
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
            SystemResourceAccess.checkSystemIndexAccess(context, threadContext, ia.getWriteIndex());
            return ia;
        } else {
            throw new IllegalArgumentException(
                "unable to return a single target as the provided expression and options got resolved to multiple targets"
            );
        }
    }

    /**
     * Resolve the expression to the set of indices, aliases, and, optionally, data streams that the expression matches.
     * If {@param preserveDataStreams} is {@code true}, data streams that are covered by the wildcards from the
     * {@param expressions} are returned as-is, without expanding them further to their respective backing indices.
     */
    protected static Collection<ResolvedExpression> resolveExpressionsToResources(Context context, String... expressions) {
        // If we do not expand wildcards, then empty or _all expression result in an empty list
        boolean expandWildcards = context.getOptions().expandWildcardExpressions();
        if (expandWildcards == false) {
            if (expressions == null
                || expressions.length == 0
                || expressions.length == 1
                    && SelectorResolver.selectorsValidatedAndMatchesPredicate(expressions[0], context, Metadata.ALL::equals)) {
                return List.of();
            }
        } else {
            if (expressions == null
                || expressions.length == 0
                || expressions.length == 1
                    && (SelectorResolver.selectorsValidatedAndMatchesPredicate(
                        expressions[0],
                        context,
                        s -> Metadata.ALL.equals(s) || Regex.isMatchAllPattern(s)
                    ))) {
                IndexComponentSelector selector;
                if (expressions != null && expressions.length == 1) {
                    selector = SelectorResolver.parseMatchAllToSelector(context, expressions[0]);
                } else {
                    selector = IndexComponentSelector.DATA;
                }
                return WildcardExpressionResolver.resolveAll(context, selector);
            } else if (isNoneExpression(expressions)) {
                return List.of();
            }
        }

        // Using ArrayList when we know we do not have wildcards is an optimisation, given that one expression result in 0 or 1 resources.
        Collection<ResolvedExpression> resources = expandWildcards && WildcardExpressionResolver.hasWildcards(expressions)
            ? new LinkedHashSet<>()
            : new ArrayList<>(expressions.length);
        boolean wildcardSeen = false;
        for (int i = 0, n = expressions.length; i < n; i++) {
            String originalExpression = expressions[i];

            // Resolve exclusion, a `-` prefixed expression is an exclusion only if it succeeds a wildcard.
            boolean isExclusion = wildcardSeen && originalExpression.startsWith("-");
            String baseExpression = isExclusion ? originalExpression.substring(1) : originalExpression;

            // Parse a potential selector from the expression
            ResolvedExpression partiallyResolvedExpression = SelectorResolver.parseExpression(baseExpression, context.getOptions());
            baseExpression = partiallyResolvedExpression.resource();
            IndexComponentSelector selector = partiallyResolvedExpression.selector();

            // Resolve date math
            baseExpression = DateMathExpressionResolver.resolveExpression(baseExpression, context::getStartTime);

            // Validate base expression
            validateResourceExpression(context, baseExpression, expressions);

            // Check if it's wildcard
            boolean isWildcard = expandWildcards && WildcardExpressionResolver.isWildcard(baseExpression);
            wildcardSeen |= isWildcard;

            if (isWildcard) {
                Set<ResolvedExpression> matchingResources = WildcardExpressionResolver.matchWildcardToResources(
                    context,
                    baseExpression,
                    selector
                );

                if (context.getOptions().allowNoIndices() == false && matchingResources.isEmpty()) {
                    throw notFoundException(combineSelector(baseExpression, partiallyResolvedExpression.selector()));
                }

                if (isExclusion) {
                    resources.removeAll(matchingResources);
                } else {
                    resources.addAll(matchingResources);
                }
            } else {
                if (isExclusion) {
                    resources.remove(new ResolvedExpression(baseExpression, selector));
                } else if (ensureAliasOrIndexExists(context, baseExpression, selector)) {
                    resources.add(new ResolvedExpression(baseExpression, selector));
                }
            }
        }
        return resources;
    }

    /**
     * Validates the requested expression by performing the following checks:
     * - Ensure it's not empty
     * - Ensure it doesn't start with `_`
     * - Ensure it's not a remote expression unless the allow unavailable targets is enabled.
     */
    private static void validateResourceExpression(Context context, String current, String[] expressions) {
        if (Strings.isEmpty(current)) {
            throw notFoundException(current);
        }
        // Expressions can not start with an underscore. This is reserved for APIs. If the check gets here, the API
        // does not exist and the path is interpreted as an expression. If the expression begins with an underscore,
        // throw a specific error that is different from the [[IndexNotFoundException]], which is typically thrown
        // if the expression can't be found.
        if (current.charAt(0) == '_') {
            throw new InvalidIndexNameException(current, "must not start with '_'.");
        }
        ensureRemoteExpressionRequireIgnoreUnavailable(context.getOptions(), current, expressions);
    }

    /**
     * Throws an exception if the expression is a remote expression and we do not allow unavailable targets
     */
    private static void ensureRemoteExpressionRequireIgnoreUnavailable(IndicesOptions options, String current, String[] expressions) {
        if (options.ignoreUnavailable()) {
            return;
        }
        if (RemoteClusterAware.isRemoteIndexName(current)) {
            List<String> crossClusterIndices = new ArrayList<>();
            for (int i = 0; i < expressions.length; i++) {
                if (RemoteClusterAware.isRemoteIndexName(expressions[i])) {
                    crossClusterIndices.add(expressions[i]);
                }
            }
            throw new IllegalArgumentException(
                "Cross-cluster calls are not supported in this context but remote indices were requested: " + crossClusterIndices
            );
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

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param project           the project containing all the data to resolve to expressions to concrete indices
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions  expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexNotFoundException if one of the index expressions is pointing to a missing index or alias and the
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case; if a remote index is requested.
     */
    public Index[] concreteIndices(ProjectMetadata project, IndicesOptions options, String... indexExpressions) {
        return concreteIndices(project, options, false, indexExpressions);
    }

    public Index[] concreteIndices(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... indexExpressions) {
        return concreteIndices(projectResolver.getProjectMetadata(state), options, includeDataStreams, indexExpressions);
    }

    public Index[] concreteIndices(
        ProjectMetadata project,
        IndicesOptions options,
        boolean includeDataStreams,
        String... indexExpressions
    ) {
        Context context = new Context(
            project,
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
        return concreteIndices(projectResolver.getProjectMetadata(state), request, startTime);
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param project    the project containing all the data to resolve to expressions to concrete indices
     * @param startTime  The start of the request where concrete indices is being invoked for
     * @param request    request containing expressions that can be resolved to alias, index, or data stream names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case; if a remote index is requested.
     */
    public Index[] concreteIndices(ProjectMetadata project, IndicesRequest request, long startTime) {
        Context context = new Context(
            project,
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
        final Collection<ResolvedExpression> expressions = resolveExpressionsToResources(context, indexExpressions);

        final Set<Index> concreteIndicesResult = Sets.newLinkedHashSetWithExpectedSize(expressions.size());
        final Map<String, IndexAbstraction> indicesLookup = context.getProject().getIndicesLookup();
        for (ResolvedExpression expression : expressions) {
            final IndexAbstraction indexAbstraction = indicesLookup.get(expression.resource());
            assert indexAbstraction != null;
            if (context.isResolveToWriteIndex()) {
                if (shouldIncludeRegularIndices(context.getOptions(), expression.selector())) {
                    Index writeIndex = indexAbstraction.getWriteIndex();
                    if (writeIndex == null && indexAbstraction.getType() == Type.ALIAS) {
                        throw new IllegalArgumentException(
                            "no write index is defined for alias ["
                                + indexAbstraction.getName()
                                + "]."
                                + " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple"
                                + " indices without one being designated as a write index"
                        );
                    }
                    if (addIndex(writeIndex, null, context)) {
                        concreteIndicesResult.add(writeIndex);
                    }
                }
                if (shouldIncludeFailureIndices(context.getOptions(), expression.selector())) {
                    Index failureStoreWriteIndex = indexAbstraction.getWriteFailureIndex(context.getProject());
                    if (failureStoreWriteIndex != null && addIndex(failureStoreWriteIndex, null, context)) {
                        concreteIndicesResult.add(failureStoreWriteIndex);
                    }
                }
            } else {
                if (context.getOptions().allowAliasesToMultipleIndices() == false
                    && resolvesToMoreThanOneIndex(indexAbstraction, context, expression)) {
                    String[] indexNames = new String[indexAbstraction.getIndices().size()];
                    int i = 0;
                    for (Index indexName : indexAbstraction.getIndices()) {
                        indexNames[i++] = indexName.getName();
                    }
                    throw new IllegalArgumentException(
                        indexAbstraction.getType().getDisplayName()
                            + " ["
                            + expression.resource()
                            + "] has more than one index associated with it "
                            + Arrays.toString(indexNames)
                            + ", can't execute a single index op"
                    );
                }

                if (indexAbstraction.isDataStreamRelated()) {
                    resolveIndicesForDataStreamRelatedAbstraction(context, indexAbstraction, concreteIndicesResult, expression.selector());
                } else {
                    List<Index> indices = indexAbstraction.getIndices();
                    for (int i = 0, n = indices.size(); i < n; i++) {
                        Index index = indices.get(i);
                        if (shouldTrackConcreteIndex(context, index)) {
                            concreteIndicesResult.add(index);
                        }
                    }
                }
            }
        }

        if (context.getOptions().allowNoIndices() == false && concreteIndicesResult.isEmpty()) {
            throw notFoundException(indexExpressions);
        }
        Index[] resultArray = concreteIndicesResult.toArray(Index.EMPTY_ARRAY);
        SystemResourceAccess.checkSystemIndexAccess(context, threadContext, resultArray);
        return resultArray;
    }

    private static void resolveIndicesForDataStreamRelatedAbstraction(
        Context context,
        IndexAbstraction indexAbstraction,
        Set<Index> concreteIndicesResult,
        IndexComponentSelector selector
    ) {
        if (shouldIncludeRegularIndices(context.getOptions(), selector)) {
            List<Index> indices = indexAbstraction.getIndices();
            for (int i = 0, n = indices.size(); i < n; i++) {
                Index index = indices.get(i);
                if (shouldTrackConcreteIndex(context, index)) {
                    concreteIndicesResult.add(index);
                }
            }
        }
        if (shouldIncludeFailureIndices(context.getOptions(), selector)) {
            List<Index> failureIndices = indexAbstraction.getFailureIndices(context.getProject());
            for (int i = 0, n = failureIndices.size(); i < n; i++) {
                Index index = failureIndices.get(i);
                if (shouldTrackConcreteIndex(context, index)) {
                    concreteIndicesResult.add(index);
                }
            }
        }
    }

    public static boolean shouldIncludeRegularIndices(IndicesOptions indicesOptions, IndexComponentSelector expressionSelector) {
        if (indicesOptions.allowSelectors()) {
            return expressionSelector == null || expressionSelector.shouldIncludeData();
        }
        return true;
    }

    public static boolean shouldIncludeFailureIndices(IndicesOptions indicesOptions, IndexComponentSelector expressionSelector) {
        // We return failure indices regardless of whether the data stream actually has the `failureStoreEnabled` flag set to true.
        if (indicesOptions.allowSelectors()) {
            return expressionSelector != null && expressionSelector.shouldIncludeFailures();
        }
        return indicesOptions.includeFailureIndices();
    }

    private static boolean resolvesToMoreThanOneIndex(IndexAbstraction indexAbstraction, Context context, ResolvedExpression expression) {
        if (indexAbstraction.getType() == Type.ALIAS && indexAbstraction.isDataStreamRelated()) {
            // We inline this logic instead of calling aliasDataStreams because we want to return as soon as we've identified that we have
            // more than one index. No use resolving a colossal alias to all of its data streams if the first data stream we find puts us
            // over the limit.
            SortedMap<String, IndexAbstraction> indicesLookup = context.getProject().getIndicesLookup();
            int count = 0;
            Set<DataStream> visitedDataStreams = new HashSet<>();
            List<Index> indices = indexAbstraction.getIndices();
            for (int i = 0, n = indices.size(); i < n; i++) {
                Index index = indices.get(i);
                DataStream parentDataStream = indicesLookup.get(index.getName()).getParentDataStream();
                if (parentDataStream != null && visitedDataStreams.add(parentDataStream)) {
                    if (shouldIncludeRegularIndices(context.getOptions(), expression.selector())) {
                        count += parentDataStream.getIndices().size();
                    }
                    if (shouldIncludeFailureIndices(context.getOptions(), expression.selector())) {
                        count += parentDataStream.getFailureIndices().size();
                    }
                    if (count > 1) {
                        // Early out if we already have more than one index accounted
                        return true;
                    }
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
                count += dataStream.getFailureIndices().size();
            }
            return count > 1;
        }
        return indexAbstraction.getIndices().size() > 1;
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

    private static boolean shouldTrackConcreteIndex(Context context, Index index) {
        if (SystemResourceAccess.isNetNewInBackwardCompatibleMode(context, index)) {
            // Exclude this one as it's a net-new system index, and we explicitly don't want those.
            return false;
        }
        final IndexMetadata imd = context.project.index(index);
        if (imd.getState() == IndexMetadata.State.CLOSE) {
            IndicesOptions options = context.getOptions();
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
            imd = imd != null ? imd : context.project.index(index);
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
        return concreteSingleIndex(projectResolver.getProjectMetadata(state), request);
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single concrete index.
     * Callers should make sure they provide proper {@link org.elasticsearch.action.support.IndicesOptions}
     * that require a single index as a result. The indices resolution must in fact return a single index when
     * using this method, an {@link IllegalArgumentException} gets thrown otherwise.
     *
     * @param project           the project containing all the data to resolve to expression to a concrete index
     * @param request           The request that defines how the an alias or an index need to be resolved to a concrete index
     *                          and the expression that can be resolved to an alias or an index name.
     * @throws IllegalArgumentException if the index resolution returns more than one index; if a remote index is requested.
     * @return the concrete index obtained as a result of the index resolution
     */
    public Index concreteSingleIndex(ProjectMetadata project, IndicesRequest request) {
        String indexExpression = CollectionUtils.isEmpty(request.indices()) ? null : request.indices()[0];
        Index[] indices = concreteIndices(project, request.indicesOptions(), indexExpression);
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
     * @param project           the project containing all the data to resolve to expression to a concrete index
     * @param request           The request that defines how the an alias or an index need to be resolved to a concrete index
     *                          and the expression that can be resolved to an alias or an index name.
     * @throws IllegalArgumentException if the index resolution does not lead to an index, or leads to more than one index
     * @return the write index obtained as a result of the index resolution
     */
    public Index concreteWriteIndex(ProjectMetadata project, IndicesRequest request) {
        if (request.indices() == null || (request.indices() != null && request.indices().length != 1)) {
            throw new IllegalArgumentException("indices request must specify a single index expression");
        }
        return concreteWriteIndex(project, request.indicesOptions(), request.indices()[0], false, request.includeDataStreams());
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
        return concreteWriteIndex(projectResolver.getProjectMetadata(state), options, index, allowNoIndices, includeDataStreams);
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single write index.
     *
     * @param project   the project containing all the data to resolve to expression to a concrete index
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param index             index that can be resolved to alias or index name.
     * @param allowNoIndices    whether to allow resolve to no index
     * @param includeDataStreams Whether data streams should be included in the evaluation.
     * @throws IllegalArgumentException if the index resolution does not lead to an index, or leads to more than one index, as well as
     * if a remote index is requested.
     * @return the write index obtained as a result of the index resolution or null if no index
     */
    public Index concreteWriteIndex(
        ProjectMetadata project,
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
            project,
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
    public boolean hasIndexAbstraction(String indexAbstraction, ProjectMetadata project) {
        String resolvedAliasOrIndex = DateMathExpressionResolver.resolveExpression(indexAbstraction);
        return project.hasIndexAbstraction(resolvedAliasOrIndex);
    }

    /**
     * @return True if the provided expression contains the <code>::</code> character sequence.
     */
    public static boolean hasSelectorSuffix(String expression) {
        if (expression == null) {
            return false;
        }
        return expression.contains(SelectorResolver.SELECTOR_SEPARATOR);
    }

    public static boolean hasSelector(@Nullable String expression, IndexComponentSelector selector) {
        Objects.requireNonNull(selector, "null selectors not supported");
        if (expression == null) {
            return false;
        }
        return expression.endsWith(SelectorResolver.SELECTOR_SEPARATOR + selector.getKey());
    }

    /**
     * @return If the specified string is a selector expression then this method returns the base expression and its selector part.
     */
    public static Tuple<String, String> splitSelectorExpression(String expression) {
        return SelectorResolver.splitSelectorExpression(expression, Tuple::new);
    }

    public static String combineSelector(String baseExpression, @Nullable IndexComponentSelector selectorExpression) {
        Objects.requireNonNull(baseExpression, "baseExpression is null");
        return selectorExpression == null || IndexComponentSelector.DATA.equals(selectorExpression)
            ? baseExpression
            : (baseExpression + SelectorResolver.SELECTOR_SEPARATOR + selectorExpression.getKey());
    }

    public static String combineSelectorExpression(String baseExpression, @Nullable String selectorExpression) {
        Objects.requireNonNull(baseExpression, "baseExpression is null");
        return selectorExpression == null || IndexComponentSelector.DATA.getKey().equals(selectorExpression)
            ? baseExpression
            : (baseExpression + SelectorResolver.SELECTOR_SEPARATOR + selectorExpression);
    }

    public static void assertExpressionHasNullOrDataSelector(String expression) {
        if (Assertions.ENABLED) {
            var tuple = splitSelectorExpression(expression);
            assert tuple.v2() == null || IndexComponentSelector.DATA.getKey().equals(tuple.v2())
                : "Expected expression [" + expression + "] to have a data selector but found [" + tuple.v2() + "]";
        }
    }

    /**
     * Resolve an array of expressions to the set of indices and aliases that these expressions match.
     */
    public Set<ResolvedExpression> resolveExpressions(ProjectMetadata project, String... expressions) {
        return resolveExpressions(project, IndicesOptions.lenientExpandOpen(), false, expressions);
    }

    /**
     * Resolve the expression to the set of indices, aliases, and, optionally, datastreams that the expression matches.
     * If {@param preserveDataStreams} is {@code true}, datastreams that are covered by the wildcards from the
     * {@param expressions} are returned as-is, without expanding them further to their respective backing indices.
     */
    public Set<ResolvedExpression> resolveExpressions(
        ProjectMetadata project,
        IndicesOptions indicesOptions,
        boolean preserveDataStreams,
        String... expressions
    ) {
        Context context = new Context(
            project,
            indicesOptions,
            true,
            false,
            true,
            preserveDataStreams,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        // unmodifiable without creating a new collection as it might contain many items
        Collection<ResolvedExpression> resolved = resolveExpressionsToResources(context, expressions);
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
    public String[] filteringAliases(ProjectMetadata project, String index, Set<ResolvedExpression> resolvedExpressions) {
        return indexAliases(
            project,
            index,
            AliasMetadata::filteringRequired,
            ONLY_FILTERING_DATA_STREAM_ALIASES,
            false,
            resolvedExpressions
        );
    }

    /**
     * Iterates through the list of indices and selects the effective list of all aliases for the
     * given index. Aliases are returned even if the index is included in the resolved expressions.
     * <b>NOTE</b>: The provided expressions must have been resolved already via {@link #resolveExpressions}.
     */
    public String[] allIndexAliases(ProjectMetadata project, String index, Set<ResolvedExpression> resolvedExpressions) {
        return indexAliases(project, index, Predicates.always(), ALL_DATA_STREAM_ALIASES, true, resolvedExpressions);
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
        ProjectMetadata project,
        String index,
        Predicate<AliasMetadata> requiredAlias,
        BiPredicate<DataStreamAlias, Boolean> requiredDataStreamAlias,
        boolean skipIdentity,
        Set<ResolvedExpression> resolvedExpressions
    ) {
        if (isAllIndicesExpression(resolvedExpressions)) {
            return null;
        }
        final IndexMetadata indexMetadata = project.indices().get(index);
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

        IndexAbstraction ia = project.getIndicesLookup().get(index);
        DataStream dataStream = ia.getParentDataStream();
        if (dataStream != null) {
            boolean isData = dataStream.isFailureStoreIndex(index) == false;
            if (skipIdentity == false && resolvedExpressionsContainsAbstraction(resolvedExpressions, dataStream.getName(), isData)) {
                // skip the filters when the request targets the data stream name + selector directly
                return null;
            }
            Map<String, DataStreamAlias> dataStreamAliases = project.dataStreamAliases();
            List<DataStreamAlias> aliasesForDataStream;
            if (iterateIndexAliases(dataStreamAliases.size(), resolvedExpressions.size())) {
                aliasesForDataStream = dataStreamAliases.values()
                    .stream()
                    .filter(
                        dataStreamAlias -> resolvedExpressionsContainsAbstraction(resolvedExpressions, dataStreamAlias.getName(), isData)
                    )
                    .filter(dataStreamAlias -> dataStreamAlias.getDataStreams().contains(dataStream.getName()))
                    .toList();
            } else {
                aliasesForDataStream = resolvedExpressions.stream()
                    .filter(expression -> (expression.selector() == null || expression.selector().shouldIncludeData()) == isData)
                    .map(ResolvedExpression::resource)
                    .map(dataStreamAliases::get)
                    .filter(dataStreamAlias -> dataStreamAlias != null && dataStreamAlias.getDataStreams().contains(dataStream.getName()))
                    .toList();
            }

            List<String> requiredAliases = null;
            for (DataStreamAlias dataStreamAlias : aliasesForDataStream) {
                if (requiredDataStreamAlias.test(dataStreamAlias, isData)) {
                    if (requiredAliases == null) {
                        requiredAliases = new ArrayList<>(aliasesForDataStream.size());
                    }
                    String alias = isData
                        ? dataStreamAlias.getName()
                        : combineSelector(dataStreamAlias.getName(), IndexComponentSelector.FAILURES);
                    requiredAliases.add(alias);
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
            final int aliasCount = indexAliases.size();
            if (aliasCount == 0) {
                return null;
            }
            final List<String> aliases = new ArrayList<>(aliasCount);
            if (iterateIndexAliases(aliasCount, resolvedExpressions.size())) {
                // faster to iterate indexAliases
                // Indices can only be referenced with a data selector, or a null selector if selectors are disabled
                for (AliasMetadata aliasMetadata : indexAliases.values()) {
                    var alias = aliasMetadata.alias();
                    if (resolvedExpressionsContainsAbstraction(resolvedExpressions, alias, true)) {
                        if (requiredAlias.test(aliasMetadata) == false) {
                            return null;
                        }
                        aliases.add(alias);
                    }
                }
            } else {
                // faster to iterate resolvedExpressions
                for (ResolvedExpression resolvedExpression : resolvedExpressions) {
                    AliasMetadata aliasMetadata = indexAliases.get(resolvedExpression.resource());
                    if (aliasMetadata != null) {
                        if (requiredAlias.test(aliasMetadata) == false) {
                            return null;
                        }
                        aliases.add(aliasMetadata.getAlias());
                    }
                }
            }
            return aliases.toArray(Strings.EMPTY_ARRAY);
        }
    }

    private static boolean resolvedExpressionsContainsAbstraction(
        Set<ResolvedExpression> resolvedExpressions,
        String abstractionName,
        boolean isData
    ) {
        if (isData) {
            return resolvedExpressions.contains(new ResolvedExpression(abstractionName))
                || resolvedExpressions.contains(new ResolvedExpression(abstractionName, IndexComponentSelector.DATA));
        }
        return resolvedExpressions.contains(new ResolvedExpression(abstractionName, IndexComponentSelector.FAILURES));
    }

    /**
     * Resolves the search routing if in the expression aliases are used. If expressions point to concrete indices
     * or aliases with no routing defined the specified routing is used.
     *
     * @return routing values grouped by concrete index
     */
    public Map<String, Set<String>> resolveSearchRouting(ClusterState state, @Nullable String routing, String... expressions) {
        return resolveSearchRouting(projectResolver.getProjectMetadata(state), routing, expressions);
    }

    /**
     * Resolves the search routing if in the expression aliases are used. If expressions point to concrete indices
     * or aliases with no routing defined the specified routing is used.
     *
     * @return routing values grouped by concrete index
     */
    public Map<String, Set<String>> resolveSearchRouting(ProjectMetadata project, @Nullable String routing, String... expressions) {
        Context context = new Context(
            project,
            IndicesOptions.lenientExpandOpen(),
            false,
            false,
            true,
            getSystemIndexAccessLevel(),
            getSystemIndexAccessPredicate(),
            getNetNewSystemIndexPredicate()
        );
        final Collection<ResolvedExpression> resolvedExpressions = resolveExpressionsToResources(context, expressions);

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();
        if (routing != null) {
            paramRouting = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
        }

        for (ResolvedExpression resolvedExpression : resolvedExpressions) {
            IndexAbstraction indexAbstraction = project.getIndicesLookup().get(resolvedExpression.resource());
            if (indexAbstraction != null && indexAbstraction.getType() == Type.ALIAS) {
                // Determine which set of indices to resolve for the alias based on the selector
                List<Index> aliasIndices = null;
                if (context.getOptions().allowSelectors()) {
                    IndexComponentSelector selector = resolvedExpression.selector();
                    if (shouldIncludeRegularIndices(context.getOptions(), selector)) {
                        aliasIndices = new ArrayList<>(indexAbstraction.getIndices().size());
                        aliasIndices.addAll(indexAbstraction.getIndices());
                    }
                    if (shouldIncludeFailureIndices(context.getOptions(), selector) && indexAbstraction.isDataStreamRelated()) {
                        List<Index> failureIndices = indexAbstraction.getFailureIndices(context.getProject());
                        if (failureIndices.isEmpty() == false) {
                            aliasIndices = aliasIndices == null ? new ArrayList<>(failureIndices.size()) : aliasIndices;
                            aliasIndices.addAll(failureIndices);
                        }
                    }
                    aliasIndices = aliasIndices == null ? List.of() : aliasIndices;
                } else {
                    aliasIndices = indexAbstraction.getIndices();
                }

                for (int i = 0, n = aliasIndices.size(); i < n; i++) {
                    Index index = aliasIndices.get(i);
                    String concreteIndex = index.getName();
                    if (norouting.contains(concreteIndex) == false) {
                        AliasMetadata aliasMetadata = project.index(concreteIndex).getAliases().get(indexAbstraction.getName());
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
                if (shouldIncludeRegularIndices(context.getOptions(), resolvedExpression.selector())) {
                    if (dataStream.getIndices() != null) {
                        for (int i = 0, n = dataStream.getIndices().size(); i < n; i++) {
                            Index index = dataStream.getIndices().get(i);
                            String concreteIndex = index.getName();
                            routings = collectRoutings(routings, paramRouting, norouting, concreteIndex);
                        }
                    }
                }
                if (shouldIncludeFailureIndices(context.getOptions(), resolvedExpression.selector())) {
                    if (dataStream.getFailureIndices().isEmpty() == false) {
                        for (Index failureIndex : dataStream.getFailureIndices()) {
                            String concreteIndex = failureIndex.getName();
                            routings = collectRoutings(routings, paramRouting, norouting, concreteIndex);
                        }
                    }
                }
            } else {
                // Index
                assert resolvedExpression.selector() == null || IndexComponentSelector.DATA.equals(resolvedExpression.selector())
                    : "Concrete index is being resolved with a selector other than [data] which is illegal";
                routings = collectRoutings(routings, paramRouting, norouting, resolvedExpression.resource());
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
    public static Map<String, Set<String>> resolveSearchRoutingAllIndices(ProjectMetadata project, String routing) {
        if (routing != null) {
            Set<String> r = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
            Map<String, Set<String>> routings = new HashMap<>();
            String[] concreteIndices = project.getConcreteAllIndices();
            for (int i = 0; i < concreteIndices.length; i++) {
                routings.put(concreteIndices[i], r);
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
    public static boolean isAllIndicesExpression(Collection<ResolvedExpression> aliasesOrIndices) {
        return isAllIndices(aliasesOrIndices, ResolvedExpression::resource);
    }

    /**
     * Identifies whether the array containing index names given as argument refers to all indices
     * The empty or null array identifies all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array maps to all indices, false otherwise
     */
    public static boolean isAllIndices(Collection<String> aliasesOrIndices) {
        return isAllIndices(aliasesOrIndices, Function.identity());
    }

    /**
     * Identifies whether the array containing objects with index names given as argument refers to all indices
     * The empty or null array identifies all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @param resourceGetter allows for obtaining the index name from a generic object that contains an index expression
     * @return true if the provided array maps to all indices, false otherwise
     * @param <T> any object that can contain an index expression in some form or another
     */
    public static <T> boolean isAllIndices(Collection<T> aliasesOrIndices, Function<T, String> resourceGetter) {
        return aliasesOrIndices == null || aliasesOrIndices.isEmpty() || isExplicitAllPattern(aliasesOrIndices, resourceGetter);
    }

    /**
     * Identifies whether the array containing index names given as argument explicitly refers to all indices
     * The empty or null array doesn't explicitly map to all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array explicitly maps to all indices, false otherwise
     */
    static boolean isExplicitAllPattern(Collection<String> aliasesOrIndices) {
        return isExplicitAllPattern(aliasesOrIndices, Function.identity());
    }

    /**
     * Identifies whether the array containing index names given as argument explicitly refers to all indices
     * The empty or null array doesn't explicitly map to all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @param resourceGetter function used to lazily convert the collection type into a string for checking equality with the ALL pattern
     * @return true if the provided array explicitly maps to all indices, false otherwise
     */
    static <T> boolean isExplicitAllPattern(Collection<T> aliasesOrIndices, Function<T, String> resourceGetter) {
        return aliasesOrIndices != null
            && aliasesOrIndices.size() == 1
            && Metadata.ALL.equals(resourceGetter.apply(aliasesOrIndices.iterator().next()));
    }

    /**
     * Identifies if this expression list is *,-* which effectively means a request that requests no indices.
     */
    public static boolean isNoneExpression(String[] expressions) {
        return expressions.length == 2 && "*".equals(expressions[0]) && "-*".equals(expressions[1]);
    }

    /**
     * @return the system access level that will be applied in this resolution. See {@link SystemIndexAccessLevel} for details.
     */
    public SystemIndexAccessLevel getSystemIndexAccessLevel() {
        final SystemIndexAccessLevel accessLevel = SystemIndices.getSystemIndexAccessLevel(threadContext);
        assert accessLevel != SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY
            : "BACKWARDS_COMPATIBLE_ONLY access level should never be used automatically, it should only be used in known special cases";
        return accessLevel;
    }

    /**
     * Determines the right predicate based on the {@link IndexNameExpressionResolver#getSystemIndexAccessLevel()}. Specifically:
     * - NONE implies no access to net-new system indices and data streams
     * - BACKWARDS_COMPATIBLE_ONLY allows access also to net-new system resources
     * - ALL allows access to everything
     * - otherwise we fall back to {@link SystemIndices#getProductSystemIndexNamePredicate(ThreadContext)}
     * @return the predicate that defines the access to system indices.
     */
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

    /**
     * This returns `true` if the given {@param name} is of a resource that exists.
     * Otherwise, it returns `false` if the `ignore_unvailable` option is `true`, or, if `false`, it throws a "not found" type of
     * exception.
     */
    @Nullable
    private static boolean ensureAliasOrIndexExists(Context context, String name, IndexComponentSelector selector) {
        boolean ignoreUnavailable = context.getOptions().ignoreUnavailable();
        IndexAbstraction indexAbstraction = context.getProject().getIndicesLookup().get(name);
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
            assert selector != null : "Earlier logic should have parsed selectors or added the default selectors already";
            // Check if ::failures has been explicitly requested
            if (IndexComponentSelector.FAILURES.equals(selector) && indexAbstraction.isDataStreamRelated() == false) {
                // If requested abstraction is not data stream related, then you cannot use ::failures
                if (ignoreUnavailable) {
                    return false;
                } else {
                    // Return the expression with the selector on it since the selector is the part that is incorrect
                    throw notFoundException(combineSelector(name, selector));
                }
            }
        }
        return true;
    }

    public static class Context {

        private final ProjectMetadata project;
        private final IndicesOptions options;
        private final long startTime;
        private final boolean preserveAliases;
        private final boolean resolveToWriteIndex;
        private final boolean includeDataStreams;
        private final boolean preserveDataStreams;
        private final SystemIndexAccessLevel systemIndexAccessLevel;
        private final Predicate<String> systemIndexAccessPredicate;
        private final Predicate<String> netNewSystemIndexPredicate;

        Context(ProjectMetadata project, IndicesOptions options, SystemIndexAccessLevel systemIndexAccessLevel) {
            this(project, options, systemIndexAccessLevel, Predicates.always(), Predicates.never());
        }

        Context(
            ProjectMetadata project,
            IndicesOptions options,
            SystemIndexAccessLevel systemIndexAccessLevel,
            Predicate<String> systemIndexAccessPredicate,
            Predicate<String> netNewSystemIndexPredicate
        ) {
            this(
                project,
                options,
                System.currentTimeMillis(),
                systemIndexAccessLevel,
                systemIndexAccessPredicate,
                netNewSystemIndexPredicate
            );
        }

        Context(
            ProjectMetadata project,
            IndicesOptions options,
            boolean preserveAliases,
            boolean resolveToWriteIndex,
            boolean includeDataStreams,
            SystemIndexAccessLevel systemIndexAccessLevel,
            Predicate<String> systemIndexAccessPredicate,
            Predicate<String> netNewSystemIndexPredicate
        ) {
            this(
                project,
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
            ProjectMetadata project,
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
                project,
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
            ProjectMetadata project,
            IndicesOptions options,
            long startTime,
            SystemIndexAccessLevel systemIndexAccessLevel,
            Predicate<String> systemIndexAccessPredicate,
            Predicate<String> netNewSystemIndexPredicate
        ) {
            this(
                project,
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
            ProjectMetadata project,
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
            this.project = project;
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

        public ProjectMetadata getProject() {
            return project;
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
         * by {@link #filteringAliases(ProjectMetadata, String, Set)} since it's the only one that needs aliases
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
     * Resolves name expressions with wildcards into the corresponding concrete indices/aliases/data streams
     */
    static final class WildcardExpressionResolver {

        private WildcardExpressionResolver() {
            // Utility class
        }

        /**
         * Returns all the indices, data streams, and aliases, considering the open/closed, system, and hidden context parameters.
         * Depending on the context, returns the names of the data streams themselves or their backing indices.
         */
        public static Collection<ResolvedExpression> resolveAll(Context context, IndexComponentSelector selector) {
            assert selector != null || context.options.allowSelectors() == false
                : "selectors are enabled in this context, but a selector was not provided";
            List<ResolvedExpression> concreteIndices = resolveEmptyOrTrivialWildcard(context, selector);

            if (context.includeDataStreams() == false && context.getOptions().ignoreAliases()) {
                return concreteIndices;
            }

            Set<ResolvedExpression> resolved = new HashSet<>(concreteIndices.size());
            context.getProject()
                .getIndicesLookup()
                .values()
                .stream()
                .filter(ia -> context.getOptions().expandWildcardsHidden() || ia.isHidden() == false)
                .filter(ia -> shouldIncludeIfDataStream(ia, context) || shouldIncludeIfAlias(ia, context))
                .filter(ia -> ia.isSystem() == false || context.systemIndexAccessPredicate.test(ia.getName()))
                .forEach(ia -> resolved.addAll(expandToOpenClosed(context, ia, selector)));

            resolved.addAll(concreteIndices);
            return resolved;
        }

        private static boolean shouldIncludeIfDataStream(IndexAbstraction ia, IndexNameExpressionResolver.Context context) {
            return context.includeDataStreams() && ia.getType() == Type.DATA_STREAM;
        }

        private static boolean shouldIncludeIfAlias(IndexAbstraction ia, IndexNameExpressionResolver.Context context) {
            return context.getOptions().ignoreAliases() == false && ia.getType() == Type.ALIAS;
        }

        private static IndexMetadata.State excludeState(IndicesOptions options) {
            final IndexMetadata.State excludeState;
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                excludeState = null;
            } else if (options.expandWildcardsOpen()) {
                excludeState = IndexMetadata.State.CLOSE;
            } else if (options.expandWildcardsClosed()) {
                excludeState = IndexMetadata.State.OPEN;
            } else {
                assert false : "this shouldn't get called if wildcards expand to none";
                excludeState = null;
            }
            return excludeState;
        }

        /**
         * Given a single wildcard {@param expression}, return a {@code Set} that contains all the resources (i.e. indices, aliases,
         * and data streams), that exist in the cluster at this moment in time, and that the wildcard "resolves" to (i.e. the resource's
         * name matches the {@param expression} wildcard).
         * The {@param context} provides the current time-snapshot view of cluster state, as well as conditions
         * on whether to consider alias, data stream, system, and hidden resources.
         */
        static Set<ResolvedExpression> matchWildcardToResources(
            Context context,
            String wildcardExpression,
            IndexComponentSelector selector
        ) {
            assert isWildcard(wildcardExpression);
            final SortedMap<String, IndexAbstraction> indicesLookup = context.getProject().getIndicesLookup();
            Set<ResolvedExpression> matchedResources = new HashSet<>();
            // this applies an initial pre-filtering in the case where the expression is a common suffix wildcard, eg "test*"
            if (Regex.isSuffixMatchPattern(wildcardExpression)) {
                for (IndexAbstraction ia : filterIndicesLookupForSuffixWildcard(indicesLookup, wildcardExpression).values()) {
                    maybeAddToResult(context, wildcardExpression, ia, selector, matchedResources);
                }
                return matchedResources;
            }
            // In case of match all it fetches all index abstractions
            if (Regex.isMatchAllPattern(wildcardExpression)) {
                for (IndexAbstraction ia : indicesLookup.values()) {
                    maybeAddToResult(context, wildcardExpression, ia, selector, matchedResources);
                }
                return matchedResources;
            }
            for (IndexAbstraction indexAbstraction : indicesLookup.values()) {
                if (Regex.simpleMatch(wildcardExpression, indexAbstraction.getName())) {
                    maybeAddToResult(context, wildcardExpression, indexAbstraction, selector, matchedResources);
                }
            }
            return matchedResources;
        }

        private static void maybeAddToResult(
            Context context,
            String wildcardExpression,
            IndexAbstraction indexAbstraction,
            @Nullable IndexComponentSelector selector,
            Set<ResolvedExpression> matchedResources
        ) {
            if (shouldExpandToIndexAbstraction(context, wildcardExpression, indexAbstraction)) {
                matchedResources.addAll(expandToOpenClosed(context, indexAbstraction, selector));
            }
        }

        /**
         * Checks if this index abstraction should be included because it matched the wildcard expression.
         * @param context the options of this request that influence the decision if this index abstraction should be included in the result
         * @param wildcardExpression the wildcard expression that matched this index abstraction
         * @param indexAbstraction the index abstraction in question
         * @return true, if the index abstraction should be included in the result
         */
        private static boolean shouldExpandToIndexAbstraction(
            Context context,
            String wildcardExpression,
            IndexAbstraction indexAbstraction
        ) {
            if (context.getOptions().ignoreAliases() && indexAbstraction.getType() == Type.ALIAS) {
                return false;
            }
            if (context.includeDataStreams() == false && indexAbstraction.isDataStreamRelated()) {
                return false;
            }

            if (indexAbstraction.isSystem()
                && SystemResourceAccess.shouldExpandToSystemIndexAbstraction(context, indexAbstraction) == false) {
                return false;
            }

            if (context.getOptions().expandWildcardsHidden() == false) {
                // there is this behavior that hidden indices that start with "." are not hidden if the wildcard expression also
                // starts with "."
                if (indexAbstraction.isHidden()
                    && (wildcardExpression.startsWith(".") && indexAbstraction.getName().startsWith(".")) == false) {
                    return false;
                }
            }
            return true;
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

        /**
         * Return the {@code Set} of open and/or closed index names for the given {@param resources}.
         * Data streams and aliases are interpreted to refer to multiple indices,
         * then all index resources are filtered by their open/closed status.
         */
        private static Set<ResolvedExpression> expandToOpenClosed(
            Context context,
            IndexAbstraction indexAbstraction,
            IndexComponentSelector selector
        ) {
            final IndexMetadata.State excludeState = excludeState(context.getOptions());
            Set<ResolvedExpression> resources = new HashSet<>();
            if (context.isPreserveAliases() && indexAbstraction.getType() == Type.ALIAS) {
                resources.add(new ResolvedExpression(indexAbstraction.getName(), selector));
            } else if (context.isPreserveDataStreams() && indexAbstraction.getType() == Type.DATA_STREAM) {
                resources.add(new ResolvedExpression(indexAbstraction.getName(), selector));
            } else {
                if (shouldIncludeRegularIndices(context.getOptions(), selector)) {
                    for (int i = 0, n = indexAbstraction.getIndices().size(); i < n; i++) {
                        Index index = indexAbstraction.getIndices().get(i);
                        IndexMetadata indexMetadata = context.project.index(index);
                        if (indexMetadata.getState() != excludeState) {
                            resources.add(new ResolvedExpression(index.getName(), context.getOptions()));
                        }
                    }
                }
                if (shouldIncludeFailureIndices(context.getOptions(), selector)) {
                    if (indexAbstraction.isDataStreamRelated()) {
                        List<Index> failureIndices = indexAbstraction.getFailureIndices(context.getProject());
                        for (int i = 0, n = failureIndices.size(); i < n; i++) {
                            Index index = failureIndices.get(i);
                            IndexMetadata indexMetadata = context.getProject().index(index);
                            if (indexMetadata.getState() != excludeState) {
                                resources.add(new ResolvedExpression(index.getName(), context.getOptions()));
                            }
                        }
                    }
                }
            }
            return resources;
        }

        private static List<ResolvedExpression> resolveEmptyOrTrivialWildcard(Context context, IndexComponentSelector selector) {
            final String[] allIndices = resolveEmptyOrTrivialWildcardToAllIndices(context.getOptions(), context.getProject(), selector);
            List<String> indices;
            if (context.systemIndexAccessLevel == SystemIndexAccessLevel.ALL) {
                indices = List.of(allIndices);
            } else {
                indices = resolveEmptyOrTrivialWildcardWithAllowedSystemIndices(context, allIndices);
            }
            List<ResolvedExpression> result = new ArrayList<>(indices.size());
            boolean allowSelectors = context.options.allowSelectors();
            for (int i = 0; i < indices.size(); i++) {
                if (allowSelectors) {
                    // These only have values if the ::data selector is applicable, and they only support the ::data selector
                    result.add(new ResolvedExpression(indices.get(i), IndexComponentSelector.DATA));
                } else {
                    result.add(new ResolvedExpression(indices.get(i)));
                }
            }
            return result;
        }

        private static List<String> resolveEmptyOrTrivialWildcardWithAllowedSystemIndices(Context context, String[] allIndices) {
            List<String> filteredIndices = new ArrayList<>(allIndices.length);
            for (int i = 0; i < allIndices.length; i++) {
                if (shouldIncludeIndexAbstraction(context, allIndices[i])) {
                    filteredIndices.add(allIndices[i]);
                }
            }
            return filteredIndices;
        }

        private static boolean shouldIncludeIndexAbstraction(Context context, String name) {
            if (name.startsWith(".") == false) {
                return true;
            }

            IndexAbstraction abstraction = context.getProject().getIndicesLookup().get(name);
            assert abstraction != null : "null abstraction for " + name + " but was in array of all indices";
            if (abstraction.isSystem() == false) {
                return true;
            }
            return SystemResourceAccess.isSystemIndexAbstractionAccessible(context, abstraction);
        }

        private static String[] resolveEmptyOrTrivialWildcardToAllIndices(
            IndicesOptions options,
            ProjectMetadata project,
            IndexComponentSelector selector
        ) {
            if (selector != null && selector.shouldIncludeData() == false) {
                return Strings.EMPTY_ARRAY;
            }
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
                return project.getConcreteAllIndices();
            } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                return project.getConcreteVisibleIndices();
            } else if (options.expandWildcardsOpen() && options.expandWildcardsHidden()) {
                return project.getConcreteAllOpenIndices();
            } else if (options.expandWildcardsOpen()) {
                return project.getConcreteVisibleOpenIndices();
            } else if (options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
                return project.getConcreteAllClosedIndices();
            } else if (options.expandWildcardsClosed()) {
                return project.getConcreteVisibleClosedIndices();
            } else {
                return Strings.EMPTY_ARRAY;
            }
        }

        static boolean isWildcard(String expression) {
            return Regex.isSimpleMatchPattern(expression);
        }

        static boolean hasWildcards(String[] expressions) {
            for (int i = 0; i < expressions.length; i++) {
                if (isWildcard(expressions[i])) {
                    return true;
                }
            }
            return false;
        }
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
     * Resolves a date math expression based on the requested time.
     */
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

        /**
         * Resolves a date math expression using the current time. This method recognises a date math expression iff when they start with
         * <code>%3C</code> and end with <code>%3E</code>. Otherwise, it returns the expression intact.
         */
        public static String resolveExpression(String expression) {
            return resolveExpression(expression, System::currentTimeMillis);
        }

        /**
         * Resolves a date math expression using the provided time. This method recognises a date math expression iff when they start with
         * <code>%3C</code> and end with <code>%3E</code>. Otherwise, it returns the expression intact.
         */
        public static String resolveExpression(String expression, LongSupplier getTime) {
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

    public static final class SelectorResolver {
        public static final String SELECTOR_SEPARATOR = "::";

        private SelectorResolver() {
            // Utility class
        }

        /**
         * Parses an index expression for a selector suffix. If a suffix is present and supported by the index options, the
         * expression and its suffix are split apart and returned. If a suffix is not present on the expression, the default
         * {@link IndexComponentSelector#DATA} selector will be returned. If suffixes are present but not supported by the
         * index options, this will throw {@link IndexNotFoundException}. When suffixes are not allowed by the context, the
         * selector returned will be null.
         * @param options IndicesOptions object
         * @param expression The expression to check for selectors
         * @return A resolved expression, optionally paired with a selector if present and supported.
         */
        public static ResolvedExpression parseExpression(String expression, IndicesOptions options) {
            return parseAndTransformSelector(expression, (baseExpression, selector) -> {
                if (options.allowSelectors()) {
                    IndexComponentSelector resolvedSelector = selector != null ? selector : IndexComponentSelector.DATA;
                    return new ResolvedExpression(baseExpression, resolvedSelector);
                } else {
                    // Ensure there is no selector if the API doesn't allow it.
                    ensureNoSelectorsProvided(expression, selector);
                    return new ResolvedExpression(baseExpression);
                }
            });
        }

        /**
         * Parses an index expression for selector suffixes. If a suffix is present and supported by the index options, the suffix is
         * returned. If a suffix is not present on the expression, the default selector defined in the context is returned. If selectors
         * are disabled in the options, this will return a null value. If a selector is provided when selectors are disabled in the options,
         * this will throw {@link IndexNotFoundException}.
         * @param context Context object
         * @param matchAllExpression The match all expression given to the index request (e.g. `*`, `*::failures`, `_all::data`)
         * @return The selector for this match all expression
         */
        public static IndexComponentSelector parseMatchAllToSelector(Context context, String matchAllExpression) {
            return parseAndTransformSelector(matchAllExpression, (baseExpression, selector) -> {
                if (context.options.allowSelectors()) {
                    // if selector was not present in the expression, default to ::data
                    return selector == null ? IndexComponentSelector.DATA : selector;
                } else {
                    // Ensure there is no selector if the API doesn't allow it.
                    ensureNoSelectorsProvided(matchAllExpression, selector);
                    return null;
                }
            });
        }

        /**
         * Determines if the given expression matches the provided predicate. If the predicate evaluates to false for the expression, the
         * expression is checked for selectors and the evaluation is repeated on just the index-part. If selectors are present when they
         * should not be (according to the context object) this method will throw an exception.
         * @param expression Index expression that may contain a selector suffix.
         * @param context Context object.
         * @param predicate Determines match criteria. May be called multiple times if expression contains a valid selector.
         * @return true if the expression matches the predicate, with or without its selector suffix if one is present.
         * @throws InvalidIndexNameException if the expression contains invalid selector syntax.
         * @throws IllegalArgumentException if selectors were present on the expression, but they are not allowed in this context.
         */
        private static boolean selectorsValidatedAndMatchesPredicate(String expression, Context context, Predicate<String> predicate) {
            if (expression == null) {
                return false;
            }
            // We need to check if there was a selector present, and validate if it was allowed
            return parseAndTransformSelector(expression, (base, selectors) -> {
                if (context.options.allowSelectors() == false) {
                    // Ensure there is no selector if the API doesn't allow it.
                    ensureNoSelectorsProvided(expression, selectors);
                }
                return predicate.test(base);
            });
        }

        /**
         * Splits off a selector suffix from the expression and converts it to the corresponding {@link IndexComponentSelector} value. A
         * provided function binds the results to the return type.
         * @param expression The expression to parse and split apart
         * @param bindFunction A function that is handed the remainder of the expression and any selector that was parsed off
         *                       the expression.
         * @return The result of the bind function
         * @param <V> The type returned from the binding function
         * @throws InvalidIndexNameException In the event that the selector syntax is used incorrectly.
         */
        private static <V> V parseAndTransformSelector(String expression, BiFunction<String, IndexComponentSelector, V> bindFunction) {
            return splitSelectorExpression(expression, (expressionBase, suffix) -> {
                if (suffix == null) {
                    return bindFunction.apply(expressionBase, null);
                } else {
                    return bindFunction.apply(expressionBase, IndexComponentSelector.getByKey(suffix));
                }
            });
        }

        /**
         * Splits off selector fragments from an index expression. Selectors are always expected to be the suffix of the expression, and
         * must equal one of the supported selector keys (data, failures) or be the match-all wildcard (*) which maps to all selectors. The
         * expression is then split into the base expression and the selector string. A provided function binds the results to the return
         * type.
         * @param expression The expression to parse and split apart
         * @param bindFunction A function that is handed the remainder of the expression and any selector suffix that was parsed off
         *                       the expression.
         * @return The result of the bind function
         * @param <V> The type returned from the binding function
         * @throws InvalidIndexNameException In the event that the selector syntax is used incorrectly.
         */
        private static <V> V splitSelectorExpression(String expression, BiFunction<String, String, V> bindFunction) {
            Objects.requireNonNull(expression, "expression cannot be null");
            int lastDoubleColon = expression.lastIndexOf(SELECTOR_SEPARATOR);
            if (lastDoubleColon >= 0) {
                String suffix = expression.substring(lastDoubleColon + SELECTOR_SEPARATOR.length());
                IndexComponentSelector selector = resolveAndValidateSelectorString(() -> expression, suffix);
                String expressionBase = expression.substring(0, lastDoubleColon);
                ensureNoMoreSelectorSeparators(expressionBase, expression);
                ensureNotMixingRemoteClusterExpressionWithSelectorSeparator(expressionBase, selector, expression);
                return bindFunction.apply(expressionBase, suffix);
            }
            // Otherwise accept the default
            return bindFunction.apply(expression, null);
        }

        public static void validateIndexSelectorString(String indexName, String suffix) {
            resolveAndValidateSelectorString(() -> indexName + SELECTOR_SEPARATOR + suffix, suffix);
        }

        private static IndexComponentSelector resolveAndValidateSelectorString(Supplier<String> expression, String suffix) {
            IndexComponentSelector selector = IndexComponentSelector.getByKey(suffix);
            if (selector == null) {
                throw new InvalidIndexNameException(
                    expression.get(),
                    "invalid usage of :: separator, [" + suffix + "] is not a recognized selector"
                );
            }
            return selector;
        }

        /**
         * Checks the selectors that have been returned from splitting an expression and throws an exception if any were present.
         * @param expression Original expression
         * @param selector Selectors to validate
         * @throws UnsupportedSelectorException if selectors are present
         */
        private static void ensureNoSelectorsProvided(String expression, IndexComponentSelector selector) {
            if (selector != null) {
                throw new UnsupportedSelectorException(expression);
            }
        }

        /**
         * Checks the remainder of an expression for any more selector separators and throws an exception if they are encountered.
         * @param remainingExpression Remaining expression
         * @param originalExpression Original expression to be used in the exception if invalid name is detected
         * @throws InvalidIndexNameException if there are any more :: separators
         */
        private static void ensureNoMoreSelectorSeparators(String remainingExpression, String originalExpression) {
            if (remainingExpression.contains(SELECTOR_SEPARATOR)) {
                throw new InvalidIndexNameException(
                    originalExpression,
                    "Invalid usage of :: separator, only one :: separator is allowed per expression"
                );
            }
        }

        /**
         * Checks the expression for remote cluster pattern and throws an exception if it is combined with :: selectors.
         * @throws InvalidIndexNameException if remote cluster pattern is detected after parsing the selector expression
         */
        private static void ensureNotMixingRemoteClusterExpressionWithSelectorSeparator(
            String expressionWithoutSelector,
            IndexComponentSelector selector,
            String originalExpression
        ) {
            if (selector != null) {
                if (RemoteClusterAware.isRemoteIndexName(expressionWithoutSelector)) {
                    throw new InvalidIndexNameException(originalExpression, "Selectors are not yet supported on remote cluster patterns");
                }
            }
        }
    }

    /**
     * In this class we collect the system access relevant code. The helper methods provide the following functionalities:
     * - determining the access to a system index abstraction
     * - verifying the access to system abstractions and adding the necessary warnings
     * - determining the access to a system index based on its name
     * WARNING: we have observed differences in how the access is determined. For now this behaviour is documented and preserved.
     */
    public static final class SystemResourceAccess {

        private SystemResourceAccess() {
            // Utility class
        }

        /**
         * Checks if this system index abstraction should be included when resolving via {@link
         * IndexNameExpressionResolver.WildcardExpressionResolver#resolveEmptyOrTrivialWildcardWithAllowedSystemIndices(Context, String[])}.
         * NOTE: it behaves differently than {@link SystemResourceAccess#shouldExpandToSystemIndexAbstraction(Context, IndexAbstraction)}
         * because in the case that the access level is BACKWARDS_COMPATIBLE_ONLY it does not include the net-new indices, this is
         * questionable.
         */
        public static boolean isSystemIndexAbstractionAccessible(Context context, IndexAbstraction abstraction) {
            assert abstraction.isSystem() : "We should only check this for system resources";
            if (context.netNewSystemIndexPredicate.test(abstraction.getName())) {
                if (SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY.equals(context.systemIndexAccessLevel)) {
                    return false;
                } else {
                    return context.systemIndexAccessPredicate.test(abstraction.getName());
                }
            } else if (abstraction.getType() == Type.DATA_STREAM || abstraction.getParentDataStream() != null) {
                return context.systemIndexAccessPredicate.test(abstraction.getName());
            }
            return true;
        }

        /**
         * Historic, i.e. not net-new, system indices are included irrespective of the system access predicate
         * the system access predicate is based on the endpoint kind and HTTP request headers that identify the stack feature.
         * A historic system resource, can only be an index since system data streams were added later.
         */
        private static boolean shouldExpandToSystemIndexAbstraction(Context context, IndexAbstraction indexAbstraction) {
            assert indexAbstraction.isSystem() : "We should only check this for system resources";
            boolean isHistoric = indexAbstraction.getType() != Type.DATA_STREAM
                && indexAbstraction.getParentDataStream() == null
                && context.netNewSystemIndexPredicate.test(indexAbstraction.getName()) == false;
            return isHistoric || context.systemIndexAccessPredicate.test(indexAbstraction.getName());
        }

        /**
         * Checks if any system indices that should not have been accessible according to the
         * {@link Context#getSystemIndexAccessPredicate()} are accessed, and it performs the following actions:
         * - if there are historic (aka not net-new) system indices, then it adds a deprecation warning
         * - if it contains net-new system indices or system data streams, it throws an exception.
         */
        private static void checkSystemIndexAccess(Context context, ThreadContext threadContext, Index... concreteIndices) {
            final Predicate<String> systemIndexAccessPredicate = context.getSystemIndexAccessPredicate();
            if (systemIndexAccessPredicate == Predicates.<String>always()) {
                return;
            }
            doCheckSystemIndexAccess(context, systemIndexAccessPredicate, threadContext, concreteIndices);
        }

        private static void doCheckSystemIndexAccess(
            Context context,
            Predicate<String> systemIndexAccessPredicate,
            ThreadContext threadContext,
            Index... concreteIndices
        ) {
            final ProjectMetadata projectMetadata = context.getProject();
            final List<String> resolvedSystemIndices = new ArrayList<>();
            final List<String> resolvedNetNewSystemIndices = new ArrayList<>();
            final Set<String> resolvedSystemDataStreams = new HashSet<>();
            final SortedMap<String, IndexAbstraction> indicesLookup = projectMetadata.getIndicesLookup();
            boolean matchedIndex = false;
            for (int i = 0; i < concreteIndices.length; i++) {
                Index concreteIndex = concreteIndices[i];
                IndexMetadata idxMetadata = projectMetadata.index(concreteIndex);
                String name = concreteIndex.getName();
                if (idxMetadata.isSystem() && systemIndexAccessPredicate.test(name) == false) {
                    matchedIndex = true;
                    IndexAbstraction indexAbstraction = indicesLookup.get(name);
                    if (indexAbstraction.getParentDataStream() != null) {
                        resolvedSystemDataStreams.add(indexAbstraction.getParentDataStream().getName());
                    } else if (context.netNewSystemIndexPredicate.test(name)) {
                        resolvedNetNewSystemIndices.add(name);
                    } else {
                        resolvedSystemIndices.add(name);
                    }
                }
            }
            if (matchedIndex) {
                handleMatchedSystemIndices(resolvedSystemIndices, resolvedSystemDataStreams, resolvedNetNewSystemIndices, threadContext);
            }
        }

        private static void handleMatchedSystemIndices(
            List<String> resolvedSystemIndices,
            Set<String> resolvedSystemDataStreams,
            List<String> resolvedNetNewSystemIndices,
            ThreadContext threadContext
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

        /**
         * Used in {@link IndexNameExpressionResolver#shouldTrackConcreteIndex(Context, Index)} to exclude net-new indices
         * when we are in backwards compatible only access level.
         * This also feels questionable as well.
         */
        private static boolean isNetNewInBackwardCompatibleMode(Context context, Index index) {
            return context.systemIndexAccessLevel == SystemIndexAccessLevel.BACKWARDS_COMPATIBLE_ONLY
                && context.netNewSystemIndexPredicate.test(index.getName());
        }
    }

}
