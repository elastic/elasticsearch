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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.InvalidIndexNameException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IndexNameExpressionResolver {

    private final DateMathExpressionResolver dateMathExpressionResolver = new DateMathExpressionResolver();
    private final List<ExpressionResolver> expressionResolvers = List.of(dateMathExpressionResolver, new WildcardExpressionResolver());

    /**
     * Same as {@link #concreteIndexNames(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
        return concreteIndexNames(state, request, false);
    }

    public String[] concreteIndexNames(ClusterState state, IndicesRequest request, boolean includeDataStreams) {
        Context context = new Context(state, request.indicesOptions(), false, false, includeDataStreams);
        return concreteIndexNames(context, request.indices());
    }

    /**
     * Same as {@link #concreteIndices(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
        return concreteIndices(state, request, false);
    }

    /**
     * Same as {@link #concreteIndices(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request and resolves data streams.
     */
    public Index[] concreteIndices(ClusterState state, IndicesRequest request, boolean includeDataStreams) {
        Context context = new Context(state, request.indicesOptions(), false, false, includeDataStreams);
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
     * indices options in the context don't allow such a case.
     */
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(state, options);
        return concreteIndexNames(context, indexExpressions);
    }

    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, boolean includeDataStreams,
                                       String... indexExpressions) {
        Context context = new Context(state, options, false, false, includeDataStreams);
        return concreteIndexNames(context, indexExpressions);
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
     * indices options in the context don't allow such a case.
     */
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, String... indexExpressions) {
        return concreteIndices(state, options, false, indexExpressions);
    }

    public Index[] concreteIndices(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... indexExpressions) {
        Context context = new Context(state, options, false, false, includeDataStreams);
        return concreteIndices(context, indexExpressions);
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param state             the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param startTime         The start of the request where concrete indices is being invoked for
     * @param indexExpressions  expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case.
     */
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, boolean includeDataStreams, long startTime,
                                   String... indexExpressions) {
        Context context = new Context(state, options, startTime, false, false, includeDataStreams);
        return concreteIndices(context, indexExpressions);
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
        if (indexExpressions == null || indexExpressions.length == 0) {
            indexExpressions = new String[]{Metadata.ALL};
        }
        Metadata metadata = context.getState().metadata();
        IndicesOptions options = context.getOptions();
        final boolean failClosed = options.forbidClosedIndices() && options.ignoreUnavailable() == false;
        // If only one index is specified then whether we fail a request if an index is missing depends on the allow_no_indices
        // option. At some point we should change this, because there shouldn't be a reason why whether a single index
        // or multiple indices are specified yield different behaviour.
        final boolean failNoIndices = indexExpressions.length == 1 ? !options.allowNoIndices() : !options.ignoreUnavailable();
        List<String> expressions = Arrays.asList(indexExpressions);
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            expressions = expressionResolver.resolve(context, expressions);
        }

        if (expressions.isEmpty()) {
            if (!options.allowNoIndices()) {
                IndexNotFoundException infe;
                if (indexExpressions.length == 1) {
                    if (indexExpressions[0].equals(Metadata.ALL)) {
                        infe = new IndexNotFoundException("no indices exist", (String)null);
                    } else {
                        infe = new IndexNotFoundException((String)null);
                    }
                } else {
                    infe = new IndexNotFoundException((String)null);
                }
                infe.setResources("index_expression", indexExpressions);
                throw infe;
            } else {
                return Index.EMPTY_ARRAY;
            }
        }

        final Set<Index> concreteIndices = new HashSet<>(expressions.size());
        for (String expression : expressions) {
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(expression);
            if (indexAbstraction == null ) {
                if (failNoIndices) {
                    IndexNotFoundException infe;
                    if (expression.equals(Metadata.ALL)) {
                        infe = new IndexNotFoundException("no indices exist", expression);
                    } else {
                        infe = new IndexNotFoundException(expression);
                    }
                    infe.setResources("index_expression", expression);
                    throw infe;
                } else {
                    continue;
                }
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS && context.getOptions().ignoreAliases()) {
                if (failNoIndices) {
                    throw aliasesNotSupportedException(expression);
                } else {
                    continue;
                }
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM &&
                        context.includeDataStreams() == false) {
                throw dataStreamsNotSupportedException(expression);
            }

            if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS && context.isResolveToWriteIndex()) {
                IndexMetadata writeIndex = indexAbstraction.getWriteIndex();
                if (writeIndex == null) {
                    throw new IllegalArgumentException("no write index is defined for alias [" + indexAbstraction.getName() + "]." +
                        " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple" +
                        " indices without one being designated as a write index");
                }
                if (addIndex(writeIndex, context)) {
                    concreteIndices.add(writeIndex.getIndex());
                }
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM && context.isResolveToWriteIndex()) {
                IndexMetadata writeIndex = indexAbstraction.getWriteIndex();
                if (addIndex(writeIndex, context)) {
                    concreteIndices.add(writeIndex.getIndex());
                }
            } else {
                if (indexAbstraction.getIndices().size() > 1 && !options.allowAliasesToMultipleIndices()) {
                    String[] indexNames = new String[indexAbstraction.getIndices().size()];
                    int i = 0;
                    for (IndexMetadata indexMetadata : indexAbstraction.getIndices()) {
                        indexNames[i++] = indexMetadata.getIndex().getName();
                    }
                    throw new IllegalArgumentException(indexAbstraction.getType().getDisplayName() + " [" + expression +
                        "] has more than one indices associated with it [" + Arrays.toString(indexNames) +
                        "], can't execute a single index op");
                }

                for (IndexMetadata index : indexAbstraction.getIndices()) {
                    if (index.getState() == IndexMetadata.State.CLOSE) {
                        if (failClosed) {
                            throw new IndexClosedException(index.getIndex());
                        } else {
                            if (options.forbidClosedIndices() == false && addIndex(index, context)) {
                                concreteIndices.add(index.getIndex());
                            }
                        }
                    } else if (index.getState() == IndexMetadata.State.OPEN) {
                        if (addIndex(index, context)) {
                            concreteIndices.add(index.getIndex());
                        }
                    } else {
                        throw new IllegalStateException("index state [" + index.getState() + "] not supported");
                    }
                }
            }
        }

        if (options.allowNoIndices() == false && concreteIndices.isEmpty()) {
            IndexNotFoundException infe = new IndexNotFoundException((String)null);
            infe.setResources("index_expression", indexExpressions);
            throw infe;
        }
        return concreteIndices.toArray(new Index[concreteIndices.size()]);
    }

    private static boolean addIndex(IndexMetadata metadata, Context context) {
        // This used to check the `index.search.throttled` setting, but we eventually decided that it was
        // trappy to hide throttled indices by default. In order to avoid breaking backward compatibility,
        // we changed it to look at the `index.frozen` setting instead, since frozen indices were the only
        // type of index to use the `search_throttled` threadpool at that time.
        // NOTE: We can't reference the Setting object, which is only defined and registered in x-pack.
        return (context.options.ignoreThrottled() && metadata.getSettings().getAsBoolean("index.frozen", false)) == false;
    }

    private static IllegalArgumentException aliasesNotSupportedException(String expression) {
        return new IllegalArgumentException("The provided expression [" + expression + "] matches an " +
                "alias, specify the corresponding concrete indices instead.");
    }

    private static IllegalArgumentException dataStreamsNotSupportedException(String expression) {
        return new IllegalArgumentException("The provided expression [" + expression + "] matches a " +
            "data stream, specify the corresponding concrete indices instead.");
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
     * @throws IllegalArgumentException if the index resolution lead to more than one index
     * @return the concrete index obtained as a result of the index resolution
     */
    public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
        String indexExpression = CollectionUtils.isEmpty(request.indices()) ? null : request.indices()[0];
        Index[] indices = concreteIndices(state, request.indicesOptions(), indexExpression);
        if (indices.length != 1) {
            throw new IllegalArgumentException("unable to return a single index as the index and options" +
                " provided got resolved to multiple indices");
        }
        return indices[0];
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single write index.
     *
     * @param state             the cluster state containing all the data to resolve to expression to a concrete index
     * @param request           The request that defines how the an alias or an index need to be resolved to a concrete index
     *                          and the expression that can be resolved to an alias or an index name.
     * @param includeDataStreams Whether data streams should be included in the evaluation.
     * @throws IllegalArgumentException if the index resolution does not lead to an index, or leads to more than one index
     * @return the write index obtained as a result of the index resolution
     */
    public Index concreteWriteIndex(ClusterState state, IndicesRequest request, boolean includeDataStreams) {
        if (request.indices() == null || (request.indices() != null && request.indices().length != 1)) {
            throw new IllegalArgumentException("indices request must specify a single index expression");
        }
        return concreteWriteIndex(state, request.indicesOptions(), request.indices()[0], false, includeDataStreams);
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single write index.
     *
     * @param state             the cluster state containing all the data to resolve to expression to a concrete index
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param index             index that can be resolved to alias or index name.
     * @param allowNoIndices    whether to allow resolve to no index
     * @param includeDataStreams Whether data streams should be included in the evaluation.
     * @throws IllegalArgumentException if the index resolution does not lead to an index, or leads to more than one index
     * @return the write index obtained as a result of the index resolution or null if no index
     */
    public Index concreteWriteIndex(ClusterState state, IndicesOptions options, String index, boolean allowNoIndices,
                                    boolean includeDataStreams) {
        Context context = new Context(state, options, false, true, includeDataStreams);
        Index[] indices = concreteIndices(context, index);
        if (allowNoIndices && indices.length == 0) {
            return null;
        }
        if (indices.length != 1) {
            throw new IllegalArgumentException("The index expression [" + index +
                "] and options provided did not point to a single write-index");
        }
        return indices[0];
    }

    /**
     * @return whether the specified index, data stream or alias exists.
     *         If the data stream, index or alias contains date math then that is resolved too.
     */
    public boolean hasIndexAbstraction(String indexAbstraction, ClusterState state) {
        Context context = new Context(state, IndicesOptions.lenientExpandOpen(), false, false, true);
        String resolvedAliasOrIndex = dateMathExpressionResolver.resolveExpression(indexAbstraction, context);
        return state.metadata().getIndicesLookup().containsKey(resolvedAliasOrIndex);
    }

    /**
     * @return If the specified string is data math expression then this method returns the resolved expression.
     */
    public String resolveDateMathExpression(String dateExpression) {
        // The data math expression resolver doesn't rely on cluster state or indices options, because
        // it just resolves the date math to an actual date.
        return dateMathExpressionResolver.resolveExpression(dateExpression, new Context(null, null));
    }

    /**
     * Resolve an array of expressions to the set of indices and aliases that these expressions match.
     */
    public Set<String> resolveExpressions(ClusterState state, String... expressions) {
        Context context = new Context(state, IndicesOptions.lenientExpandOpen(), true, false, true);
        List<String> resolvedExpressions = Arrays.asList(expressions);
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            resolvedExpressions = expressionResolver.resolve(context, resolvedExpressions);
        }
        return Set.copyOf(resolvedExpressions);
    }

    /**
     * Iterates through the list of indices and selects the effective list of filtering aliases for the
     * given index.
     * <p>Only aliases with filters are returned. If the indices list contains a non-filtering reference to
     * the index itself - null is returned. Returns {@code null} if no filtering is required.
     * <b>NOTE</b>: The provided expressions must have been resolved already via {@link #resolveExpressions}.
     */
    public String[] filteringAliases(ClusterState state, String index, Set<String> resolvedExpressions) {
        return indexAliases(state, index, AliasMetadata::filteringRequired, false, resolvedExpressions);
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
    public String[] indexAliases(ClusterState state, String index, Predicate<AliasMetadata> requiredAlias, boolean skipIdentity,
            Set<String> resolvedExpressions) {
        if (isAllIndices(resolvedExpressions)) {
            return null;
        }

        final IndexMetadata indexMetadata = state.metadata().getIndices().get(index);
        if (indexMetadata == null) {
            // Shouldn't happen
            throw new IndexNotFoundException(index);
        }

        if (skipIdentity == false && resolvedExpressions.contains(index)) {
            return null;
        }

        final ImmutableOpenMap<String, AliasMetadata> indexAliases = indexMetadata.getAliases();
        final AliasMetadata[] aliasCandidates;
        if (iterateIndexAliases(indexAliases.size(), resolvedExpressions.size())) {
            // faster to iterate indexAliases
            aliasCandidates = StreamSupport.stream(Spliterators.spliteratorUnknownSize(indexAliases.values().iterator(), 0), false)
                    .map(cursor -> cursor.value)
                    .filter(aliasMetadata -> resolvedExpressions.contains(aliasMetadata.alias()))
                    .toArray(AliasMetadata[]::new);
        } else {
            // faster to iterate resolvedExpressions
            aliasCandidates = resolvedExpressions.stream()
                    .map(indexAliases::get)
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
        return aliases.toArray(new String[aliases.size()]);
    }

    /**
     * Resolves the search routing if in the expression aliases are used. If expressions point to concrete indices
     * or aliases with no routing defined the specified routing is used.
     *
     * @return routing values grouped by concrete index
     */
    public Map<String, Set<String>> resolveSearchRouting(ClusterState state, @Nullable String routing, String... expressions) {
        List<String> resolvedExpressions = expressions != null ? Arrays.asList(expressions) : Collections.emptyList();
        Context context = new Context(state, IndicesOptions.lenientExpandOpen(), false, false, true);
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            resolvedExpressions = expressionResolver.resolve(context, resolvedExpressions);
        }

        // TODO: it appears that this can never be true?
        if (isAllIndices(resolvedExpressions)) {
            return resolveSearchRoutingAllIndices(state.metadata(), routing);
        }

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();
        if (routing != null) {
            paramRouting = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
        }

        for (String expression : resolvedExpressions) {
            IndexAbstraction indexAbstraction = state.metadata().getIndicesLookup().get(expression);
            if (indexAbstraction != null && indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
                IndexAbstraction.Alias alias = (IndexAbstraction.Alias) indexAbstraction;
                for (Tuple<String, AliasMetadata> item : alias.getConcreteIndexAndAliasMetadatas()) {
                    String concreteIndex = item.v1();
                    AliasMetadata aliasMetadata = item.v2();
                    if (!norouting.contains(concreteIndex)) {
                        if (!aliasMetadata.searchRoutingValues().isEmpty()) {
                            // Routing alias
                            if (routings == null) {
                                routings = new HashMap<>();
                            }
                            Set<String> r = routings.get(concreteIndex);
                            if (r == null) {
                                r = new HashSet<>();
                                routings.put(concreteIndex, r);
                            }
                            r.addAll(aliasMetadata.searchRoutingValues());
                            if (paramRouting != null) {
                                r.retainAll(paramRouting);
                            }
                            if (r.isEmpty()) {
                                routings.remove(concreteIndex);
                            }
                        } else {
                            // Non-routing alias
                            if (!norouting.contains(concreteIndex)) {
                                norouting.add(concreteIndex);
                                if (paramRouting != null) {
                                    Set<String> r = new HashSet<>(paramRouting);
                                    if (routings == null) {
                                        routings = new HashMap<>();
                                    }
                                    routings.put(concreteIndex, r);
                                } else {
                                    if (routings != null) {
                                        routings.remove(concreteIndex);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // Index
                if (!norouting.contains(expression)) {
                    norouting.add(expression);
                    if (paramRouting != null) {
                        Set<String> r = new HashSet<>(paramRouting);
                        if (routings == null) {
                            routings = new HashMap<>();
                        }
                        routings.put(expression, r);
                    } else {
                        if (routings != null) {
                            routings.remove(expression);
                        }
                    }
                }
            }

        }
        if (routings == null || routings.isEmpty()) {
            return null;
        }
        return routings;
    }

    /**
     * Sets the same routing for all indices
     */
    public Map<String, Set<String>> resolveSearchRoutingAllIndices(Metadata metadata, String routing) {
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
    public static boolean isAllIndices(Collection<String> aliasesOrIndices) {
        return aliasesOrIndices == null || aliasesOrIndices.isEmpty() || isExplicitAllPattern(aliasesOrIndices);
    }

    /**
     * Identifies whether the array containing index names given as argument explicitly refers to all indices
     * The empty or null array doesn't explicitly map to all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array explicitly maps to all indices, false otherwise
     */
    static boolean isExplicitAllPattern(Collection<String> aliasesOrIndices) {
        return aliasesOrIndices != null && aliasesOrIndices.size() == 1 && Metadata.ALL.equals(aliasesOrIndices.iterator().next());
    }

    /**
     * Identifies whether the first argument (an array containing index names) is a pattern that matches all indices
     *
     * @param indicesOrAliases the array containing index names
     * @param concreteIndices  array containing the concrete indices that the first argument refers to
     * @return true if the first argument is a pattern that maps to all available indices, false otherwise
     */
    boolean isPatternMatchingAllIndices(Metadata metadata, String[] indicesOrAliases, String[] concreteIndices) {
        // if we end up matching on all indices, check, if its a wildcard parameter, or a "-something" structure
        if (concreteIndices.length == metadata.getConcreteAllIndices().length && indicesOrAliases.length > 0) {

            //we might have something like /-test1,+test1 that would identify all indices
            //or something like /-test1 with test1 index missing and IndicesOptions.lenient()
            if (indicesOrAliases[0].charAt(0) == '-') {
                return true;
            }

            //otherwise we check if there's any simple regex
            for (String indexOrAlias : indicesOrAliases) {
                if (Regex.isSimpleMatchPattern(indexOrAlias)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static class Context {

        private final ClusterState state;
        private final IndicesOptions options;
        private final long startTime;
        private final boolean preserveAliases;
        private final boolean resolveToWriteIndex;
        private final boolean includeDataStreams;

        Context(ClusterState state, IndicesOptions options) {
            this(state, options, System.currentTimeMillis());
        }

        Context(ClusterState state, IndicesOptions options, boolean preserveAliases, boolean resolveToWriteIndex,
                boolean includeDataStreams) {
            this(state, options, System.currentTimeMillis(), preserveAliases, resolveToWriteIndex, includeDataStreams);
        }

        Context(ClusterState state, IndicesOptions options, long startTime) {
           this(state, options, startTime, false, false, false);
        }

        protected Context(ClusterState state, IndicesOptions options, long startTime,
                          boolean preserveAliases, boolean resolveToWriteIndex, boolean includeDataStreams) {
            this.state = state;
            this.options = options;
            this.startTime = startTime;
            this.preserveAliases = preserveAliases;
            this.resolveToWriteIndex = resolveToWriteIndex;
            this.includeDataStreams = includeDataStreams;
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
    }

    private interface ExpressionResolver {

        /**
         * Resolves the list of expressions into other expressions if possible (possible concrete indices and aliases, but
         * that isn't required). The provided implementations can also be left untouched.
         *
         * @return a new list with expressions based on the provided expressions
         */
        List<String> resolve(Context context, List<String> expressions);

    }

    /**
     * Resolves alias/index name expressions with wildcards into the corresponding concrete indices/aliases
     */
    static final class WildcardExpressionResolver implements ExpressionResolver {

        @Override
        public List<String> resolve(Context context, List<String> expressions) {
            IndicesOptions options = context.getOptions();
            Metadata metadata = context.getState().metadata();
            // only check open/closed since if we do not expand to open or closed it doesn't make sense to
            // expand to hidden
            if (options.expandWildcardsClosed() == false && options.expandWildcardsOpen() == false) {
                return expressions;
            }

            if (isEmptyOrTrivialWildcard(expressions)) {
                if (context.includeDataStreams() == false && metadata.dataStreams().isEmpty() == false) {
                    throw dataStreamsNotSupportedException(expressions.toString());
                }
                return resolveEmptyOrTrivialWildcard(options, metadata);
            }

            Set<String> result = innerResolve(context, expressions, options, metadata);

            if (result == null) {
                return expressions;
            }
            if (result.isEmpty() && !options.allowNoIndices()) {
                IndexNotFoundException infe = new IndexNotFoundException((String)null);
                infe.setResources("index_or_alias", expressions.toArray(new String[0]));
                throw infe;
            }
            return new ArrayList<>(result);
        }

        private Set<String> innerResolve(Context context, List<String> expressions, IndicesOptions options, Metadata metadata) {
            Set<String> result = null;
            boolean wildcardSeen = false;
            for (int i = 0; i < expressions.size(); i++) {
                String expression = expressions.get(i);
                if (Strings.isEmpty(expression)) {
                    throw indexNotFoundException(expression);
                }
                validateAliasOrIndex(expression);
                if (aliasOrIndexExists(context, options, metadata, expression)) {
                    if (result != null) {
                        result.add(expression);
                    }
                    continue;
                }
                final boolean add;
                if (expression.charAt(0) == '-' && wildcardSeen) {
                    add = false;
                    expression = expression.substring(1);
                } else {
                    add = true;
                }
                if (result == null) {
                    // add all the previous ones...
                    result = new HashSet<>(expressions.subList(0, i));
                }
                if (Regex.isSimpleMatchPattern(expression) == false) {
                    //TODO why does wildcard resolver throw exceptions regarding non wildcarded expressions? This should not be done here.
                    if (options.ignoreUnavailable() == false) {
                        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(expression);
                        if (indexAbstraction == null) {
                            throw indexNotFoundException(expression);
                        } else if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS && options.ignoreAliases()) {
                            throw aliasesNotSupportedException(expression);
                        } else if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM &&
                                    context.includeDataStreams() == false) {
                            throw dataStreamsNotSupportedException(expression);
                        }
                    }
                    if (add) {
                        result.add(expression);
                    } else {
                        result.remove(expression);
                    }
                    continue;
                }

                final IndexMetadata.State excludeState = excludeState(options);
                final Map<String, IndexAbstraction> matches = matches(context, metadata, expression);
                Set<String> expand = expand(context, excludeState, matches, expression, options.expandWildcardsHidden());
                if (add) {
                    result.addAll(expand);
                } else {
                    result.removeAll(expand);
                }
                if (options.allowNoIndices() == false && matches.isEmpty()) {
                    throw indexNotFoundException(expression);
                }
                if (Regex.isSimpleMatchPattern(expression)) {
                    wildcardSeen = true;
                }
            }
            return result;
        }

        private static void validateAliasOrIndex(String expression) {
            // Expressions can not start with an underscore. This is reserved for APIs. If the check gets here, the API
            // does not exist and the path is interpreted as an expression. If the expression begins with an underscore,
            // throw a specific error that is different from the [[IndexNotFoundException]], which is typically thrown
            // if the expression can't be found.
            if (expression.charAt(0) == '_') {
                throw new InvalidIndexNameException(expression, "must not start with '_'.");
            }
        }

        private static boolean aliasOrIndexExists(Context context, IndicesOptions options, Metadata metadata, String expression) {
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(expression);
            if (indexAbstraction == null) {
                return false;
            }

            //treat aliases as unavailable indices when ignoreAliases is set to true (e.g. delete index and update aliases api)
            if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS && options.ignoreAliases()) {
                return false;
            }

            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM && context.includeDataStreams() == false) {
                return false;
            }

            return true;
        }

        private static IndexNotFoundException indexNotFoundException(String expression) {
            IndexNotFoundException infe = new IndexNotFoundException(expression);
            infe.setResources("index_or_alias", expression);
            return infe;
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

        public static Map<String, IndexAbstraction> matches(Context context, Metadata metadata, String expression) {
            if (Regex.isMatchAllPattern(expression)) {
                return filterIndicesLookup(context, metadata.getIndicesLookup(), null, expression, context.getOptions());
            } else if (expression.indexOf("*") == expression.length() - 1) {
                return suffixWildcard(context, metadata, expression);
            } else {
                return otherWildcard(context, metadata, expression);
            }
        }

        private static Map<String, IndexAbstraction> suffixWildcard(Context context, Metadata metadata, String expression) {
            assert expression.length() >= 2 : "expression [" + expression + "] should have at least a length of 2";
            String fromPrefix = expression.substring(0, expression.length() - 1);
            char[] toPrefixCharArr = fromPrefix.toCharArray();
            toPrefixCharArr[toPrefixCharArr.length - 1]++;
            String toPrefix = new String(toPrefixCharArr);
            SortedMap<String, IndexAbstraction> subMap = metadata.getIndicesLookup().subMap(fromPrefix, toPrefix);
            return filterIndicesLookup(context, subMap, null, expression, context.getOptions());
        }

        private static Map<String, IndexAbstraction> otherWildcard(Context context, Metadata metadata, String expression) {
            final String pattern = expression;
            return filterIndicesLookup(context, metadata.getIndicesLookup(), e -> Regex.simpleMatch(pattern, e.getKey()),
                expression, context.getOptions());
        }

        private static Map<String, IndexAbstraction> filterIndicesLookup(Context context, SortedMap<String, IndexAbstraction> indicesLookup,
                                                                         Predicate<? super Map.Entry<String, IndexAbstraction>> filter,
                                                                         String expression,
                                                                         IndicesOptions options) {
            boolean shouldConsumeStream = false;
            Stream<Map.Entry<String, IndexAbstraction>> stream = indicesLookup.entrySet().stream();
            if (options.ignoreAliases()) {
                shouldConsumeStream = true;
                stream = stream.filter(e -> e.getValue().getType() != IndexAbstraction.Type.ALIAS);
            }
            if (filter != null) {
                shouldConsumeStream = true;
                stream = stream.filter(filter);
            }
            if (context.includeDataStreams() == false) {
                shouldConsumeStream = true;
                stream = stream.peek(e -> {
                    if (e.getValue().getType() == IndexAbstraction.Type.DATA_STREAM) {
                        throw dataStreamsNotSupportedException(expression);
                    }
                });
            }
            if (shouldConsumeStream) {
                return stream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            } else {
                return indicesLookup;
            }
        }

        private static Set<String> expand(Context context, IndexMetadata.State excludeState, Map<String, IndexAbstraction> matches,
                                          String expression, boolean includeHidden) {
            Set<String> expand = new HashSet<>();
            for (Map.Entry<String, IndexAbstraction> entry : matches.entrySet()) {
                String aliasOrIndexName = entry.getKey();
                IndexAbstraction indexAbstraction = entry.getValue();

                if (indexAbstraction.isHidden() == false || includeHidden || implicitHiddenMatch(aliasOrIndexName, expression)) {
                    if (context.isPreserveAliases() && indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
                        expand.add(aliasOrIndexName);
                    } else {
                        for (IndexMetadata meta : indexAbstraction.getIndices()) {
                            if (excludeState == null || meta.getState() != excludeState) {
                                expand.add(meta.getIndex().getName());
                            }
                        }

                    }
                }
            }
            return expand;
        }

        private static boolean implicitHiddenMatch(String itemName, String expression) {
            return itemName.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
        }

        private boolean isEmptyOrTrivialWildcard(List<String> expressions) {
            return expressions.isEmpty() || (expressions.size() == 1 && (Metadata.ALL.equals(expressions.get(0)) ||
                Regex.isMatchAllPattern(expressions.get(0))));
        }

        private static List<String> resolveEmptyOrTrivialWildcard(IndicesOptions options, Metadata metadata) {
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
                return Arrays.asList(metadata.getConcreteAllIndices());
            } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                return Arrays.asList(metadata.getConcreteVisibleIndices());
            } else if (options.expandWildcardsOpen() && options.expandWildcardsHidden()) {
                return Arrays.asList(metadata.getConcreteAllOpenIndices());
            } else if (options.expandWildcardsOpen()) {
                return Arrays.asList(metadata.getConcreteVisibleOpenIndices());
            } else if (options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
                return Arrays.asList(metadata.getConcreteAllClosedIndices());
            } else if (options.expandWildcardsClosed()) {
                return Arrays.asList(metadata.getConcreteVisibleClosedIndices());
            } else {
                return Collections.emptyList();
            }
        }
    }

    public static final class DateMathExpressionResolver implements ExpressionResolver {

        private static final DateFormatter DEFAULT_DATE_FORMATTER = DateFormatter.forPattern("uuuu.MM.dd");
        private static final String EXPRESSION_LEFT_BOUND = "<";
        private static final String EXPRESSION_RIGHT_BOUND = ">";
        private static final char LEFT_BOUND = '{';
        private static final char RIGHT_BOUND = '}';
        private static final char ESCAPE_CHAR = '\\';
        private static final char TIME_ZONE_BOUND = '|';

        @Override
        public List<String> resolve(final Context context, List<String> expressions) {
            List<String> result = new ArrayList<>(expressions.size());
            for (String expression : expressions) {
                result.add(resolveExpression(expression, context));
            }
            return result;
        }

        @SuppressWarnings("fallthrough")
        String resolveExpression(String expression, final Context context) {
            if (expression.startsWith(EXPRESSION_LEFT_BOUND) == false || expression.endsWith(EXPRESSION_RIGHT_BOUND) == false) {
                return expression;
            }

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
                            } else if (!inDateFormat) {
                                inDateFormat = true;
                                inPlaceHolderSb.append(c);
                            } else {
                                throw new ElasticsearchParseException("invalid dynamic name expression [{}]." +
                                    " invalid character in placeholder at position [{}]", new String(text, from, length), i);
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
                                        throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing closing `}`" +
                                            " for date math format", inPlaceHolderString);
                                    }
                                    if (dateTimeFormatLeftBoundIndex == inPlaceHolderString.length() - 2) {
                                        throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing date format",
                                            inPlaceHolderString);
                                    }
                                    mathExpression = inPlaceHolderString.substring(0, dateTimeFormatLeftBoundIndex);
                                    String patternAndTZid =
                                        inPlaceHolderString.substring(dateTimeFormatLeftBoundIndex + 1, inPlaceHolderString.length() - 1);
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
                                Instant instant = dateMathParser.parse(mathExpression, context::getStartTime, false, timeZone);

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
                            if (!escapedChar) {
                                throw new ElasticsearchParseException("invalid dynamic name expression [{}]." +
                                    " invalid character at position [{}]. `{` and `}` are reserved characters and" +
                                    " should be escaped when used as part of the index name using `\\` (e.g. `\\{text\\}`)",
                                    new String(text, from, length), i);
                            }
                        default:
                            beforePlaceHolderSb.append(c);
                    }
                }
            }

            if (inPlaceHolder) {
                throw new ElasticsearchParseException("invalid dynamic name expression [{}]. date math placeholder is open ended",
                    new String(text, from, length));
            }
            if (beforePlaceHolderSb.length() == 0) {
                throw new ElasticsearchParseException("nothing captured");
            }
            return beforePlaceHolderSb.toString();
        }
    }
}
