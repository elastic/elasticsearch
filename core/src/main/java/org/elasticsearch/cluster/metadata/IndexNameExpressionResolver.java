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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndexMissingException;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

public class IndexNameExpressionResolver {

    private final ImmutableList<ExpressionResolver> expressionResolvers;

    @Inject
    public IndexNameExpressionResolver() {
        expressionResolvers = ImmutableList.<ExpressionResolver>of(new WildcardExpressionResolver());
    }

    /**
     * Same as {@link #concreteIndices(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public String[] concreteIndices(ClusterState state, IndicesRequest request) throws IndexMissingException, IllegalArgumentException {
        Context context = new Context(state, request.indicesOptions());
        return concreteIndices(context, request.indices());
    }

    /**
     * Translates the provided index expression into actual concrete indices.
     *
     * @param state             the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options           defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions  expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexMissingException if one of the index expressions is pointing to a missing index or alias and the
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case.
     */
    public String[] concreteIndices(ClusterState state, IndicesOptions options, String... indexExpressions) throws IndexMissingException, IllegalArgumentException {
        Context context = new Context(state, options);
        return concreteIndices(context, indexExpressions);
    }

    String[] concreteIndices(Context context, String... indexExpressions) throws IndexMissingException, IllegalArgumentException {
        if (indexExpressions == null || indexExpressions.length == 0) {
            indexExpressions = new String[]{MetaData.ALL};
        }
        MetaData metaData = context.getState().metaData();
        IndicesOptions options = context.getOptions();
        boolean failClosed = options.forbidClosedIndices() && options.ignoreUnavailable() == false;
        boolean failNoIndices = options.ignoreUnavailable() == false;
        // If only one index is specified then whether we fail a request if an index is missing depends on the allow_no_indices
        // option. At some point we should change this, because there shouldn't be a reason why whether a single index
        // or multiple indices are specified yield different behaviour.
        if (indexExpressions.length == 1) {
            failNoIndices = options.allowNoIndices() == false;
        }

        List<String> expressions = Arrays.asList(indexExpressions);
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            expressions = expressionResolver.resolve(context, expressions);
        }

        if (expressions.isEmpty()) {
            if (!options.allowNoIndices()) {
                throw new IndexMissingException(new Index(Arrays.toString(indexExpressions)));
            } else {
                return Strings.EMPTY_ARRAY;
            }
        }

        List<String> concreteIndices = new ArrayList<>(expressions.size());
        for (String expression : expressions) {
            List<IndexMetaData> indexMetaDatas;
            IndexMetaData indexMetaData = metaData.getIndices().get(expression);
            if (indexMetaData == null) {
                ImmutableOpenMap<String, AliasMetaData> indexAliasMap = metaData.aliases().get(expression);
                if (indexAliasMap == null) {
                    if (failNoIndices) {
                        throw new IndexMissingException(new Index(expression));
                    } else {
                        continue;
                    }
                }
                if (indexAliasMap.size() > 1 && !options.allowAliasesToMultipleIndices()) {
                    throw new IllegalArgumentException("Alias [" + expression + "] has more than one indices associated with it [" + Arrays.toString(indexAliasMap.keys().toArray(String.class)) + "], can't execute a single index op");
                }
                indexMetaDatas = new ArrayList<>(indexAliasMap.size());
                for (ObjectObjectCursor<String, AliasMetaData> cursor : indexAliasMap) {
                    indexMetaDatas.add(metaData.getIndices().get(cursor.key));
                }
            } else {
                indexMetaDatas = Collections.singletonList(indexMetaData);
            }

            for (IndexMetaData found : indexMetaDatas) {
                if (found.getState() == IndexMetaData.State.CLOSE) {
                    if (failClosed) {
                        throw new IndexClosedException(new Index(found.getIndex()));
                    } else {
                        if (options.forbidClosedIndices() == false) {
                            concreteIndices.add(found.getIndex());
                        }
                    }
                } else if (found.getState() == IndexMetaData.State.OPEN) {
                    concreteIndices.add(found.getIndex());
                } else {
                    throw new IllegalStateException("index state [" + found.getState() + "] not supported");
                }
            }
        }

        if (options.allowNoIndices() == false && concreteIndices.isEmpty()) {
            throw new IndexMissingException(new Index(Arrays.toString(indexExpressions)));
        }
        return concreteIndices.toArray(new String[concreteIndices.size()]);
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single concrete index.
     * Callers should make sure they provide proper {@link org.elasticsearch.action.support.IndicesOptions}
     * that require a single index as a result. The indices resolution must in fact return a single index when
     * using this method, an {@link IllegalArgumentException} gets thrown otherwise.
     *
     * @param request   request containing the index or alias to be resolved to concrete index and
     *                  the indices options to be used for the index resolution
     * @throws IndexMissingException    if the resolved index or alias provided doesn't exist
     * @throws IllegalArgumentException if the index resolution lead to more than one index
     * @return the concrete index obtained as a result of the index resolution
     */
    public String concreteSingleIndex(ClusterState state, IndicesRequest request) throws IndexMissingException, IllegalArgumentException {
        String indexOrAlias = request.indices() != null && request.indices().length > 0 ? request.indices()[0] : null;
        String[] indices = concreteIndices(state, request.indicesOptions(), indexOrAlias);
        if (indices.length != 1) {
            throw new IllegalArgumentException("unable to return a single index as the index and options provided got resolved to multiple indices");
        }
        return indices[0];
    }

    /**
     * Iterates through the list of indices and selects the effective list of filtering aliases for the
     * given index.
     * <p/>
     * <p>Only aliases with filters are returned. If the indices list contains a non-filtering reference to
     * the index itself - null is returned. Returns <tt>null</tt> if no filtering is required.</p>
     */
    public String[] filteringAliases(ClusterState state, String index, String... expressions) {
        // expand the aliases wildcard
        List<String> resolvedExpressions = expressions != null ? Arrays.asList(expressions) : Collections.<String>emptyList();
        Context context = new Context(state, IndicesOptions.lenientExpandOpen());
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            resolvedExpressions = expressionResolver.resolve(context, resolvedExpressions);
        }

        if (isAllIndices(resolvedExpressions)) {
            return null;
        }
        // optimize for the most common single index/alias scenario
        if (resolvedExpressions.size() == 1) {
            String alias = resolvedExpressions.get(0);
            IndexMetaData indexMetaData = state.metaData().getIndices().get(index);
            if (indexMetaData == null) {
                // Shouldn't happen
                throw new IndexMissingException(new Index(index));
            }
            AliasMetaData aliasMetaData = indexMetaData.aliases().get(alias);
            boolean filteringRequired = aliasMetaData != null && aliasMetaData.filteringRequired();
            if (!filteringRequired) {
                return null;
            }
            return new String[]{alias};
        }
        List<String> filteringAliases = null;
        for (String alias : resolvedExpressions) {
            if (alias.equals(index)) {
                return null;
            }

            IndexMetaData indexMetaData = state.metaData().getIndices().get(index);
            if (indexMetaData == null) {
                // Shouldn't happen
                throw new IndexMissingException(new Index(index));
            }

            AliasMetaData aliasMetaData = indexMetaData.aliases().get(alias);
            // Check that this is an alias for the current index
            // Otherwise - skip it
            if (aliasMetaData != null) {
                boolean filteringRequired = aliasMetaData.filteringRequired();
                if (filteringRequired) {
                    // If filtering required - add it to the list of filters
                    if (filteringAliases == null) {
                        filteringAliases = newArrayList();
                    }
                    filteringAliases.add(alias);
                } else {
                    // If not, we have a non filtering alias for this index - no filtering needed
                    return null;
                }
            }
        }
        if (filteringAliases == null) {
            return null;
        }
        return filteringAliases.toArray(new String[filteringAliases.size()]);
    }

    /**
     * Resolves the search routing if in the expression aliases are used. If expressions point to concrete indices
     * or aliases with no routing defined the specified routing is used.
     *
     * @return routing values grouped by concrete index
     */
    public Map<String, Set<String>> resolveSearchRouting(ClusterState state, @Nullable String routing, String... expressions) {
        List<String> resolvedExpressions = expressions != null ? Arrays.asList(expressions) : Collections.<String>emptyList();
        Context context = new Context(state, IndicesOptions.lenientExpandOpen());
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            resolvedExpressions = expressionResolver.resolve(context, resolvedExpressions);
        }

        if (isAllIndices(resolvedExpressions)) {
            return resolveSearchRoutingAllIndices(state.metaData(), routing);
        }

        if (resolvedExpressions.size() == 1) {
            return resolveSearchRoutingSingleValue(state.metaData(), routing, resolvedExpressions.get(0));
        }

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();
        if (routing != null) {
            paramRouting = Strings.splitStringByCommaToSet(routing);
        }

        for (String expression : resolvedExpressions) {
            ImmutableOpenMap<String, AliasMetaData> indexToRoutingMap = state.metaData().getAliases().get(expression);
            if (indexToRoutingMap != null && !indexToRoutingMap.isEmpty()) {
                for (ObjectObjectCursor<String, AliasMetaData> indexRouting : indexToRoutingMap) {
                    if (!norouting.contains(indexRouting.key)) {
                        if (!indexRouting.value.searchRoutingValues().isEmpty()) {
                            // Routing alias
                            if (routings == null) {
                                routings = newHashMap();
                            }
                            Set<String> r = routings.get(indexRouting.key);
                            if (r == null) {
                                r = new HashSet<>();
                                routings.put(indexRouting.key, r);
                            }
                            r.addAll(indexRouting.value.searchRoutingValues());
                            if (paramRouting != null) {
                                r.retainAll(paramRouting);
                            }
                            if (r.isEmpty()) {
                                routings.remove(indexRouting.key);
                            }
                        } else {
                            // Non-routing alias
                            if (!norouting.contains(indexRouting.key)) {
                                norouting.add(indexRouting.key);
                                if (paramRouting != null) {
                                    Set<String> r = new HashSet<>(paramRouting);
                                    if (routings == null) {
                                        routings = newHashMap();
                                    }
                                    routings.put(indexRouting.key, r);
                                } else {
                                    if (routings != null) {
                                        routings.remove(indexRouting.key);
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
                            routings = newHashMap();
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

    private Map<String, Set<String>> resolveSearchRoutingSingleValue(MetaData metaData, @Nullable String routing, String aliasOrIndex) {
        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        if (routing != null) {
            paramRouting = Strings.splitStringByCommaToSet(routing);
        }

        ImmutableOpenMap<String, AliasMetaData> indexToRoutingMap = metaData.getAliases().get(aliasOrIndex);
        if (indexToRoutingMap != null && !indexToRoutingMap.isEmpty()) {
            // It's an alias
            for (ObjectObjectCursor<String, AliasMetaData> indexRouting : indexToRoutingMap) {
                if (!indexRouting.value.searchRoutingValues().isEmpty()) {
                    // Routing alias
                    Set<String> r = new HashSet<>(indexRouting.value.searchRoutingValues());
                    if (paramRouting != null) {
                        r.retainAll(paramRouting);
                    }
                    if (!r.isEmpty()) {
                        if (routings == null) {
                            routings = newHashMap();
                        }
                        routings.put(indexRouting.key, r);
                    }
                } else {
                    // Non-routing alias
                    if (paramRouting != null) {
                        Set<String> r = new HashSet<>(paramRouting);
                        if (routings == null) {
                            routings = newHashMap();
                        }
                        routings.put(indexRouting.key, r);
                    }
                }
            }
        } else {
            // It's an index
            if (paramRouting != null) {
                routings = ImmutableMap.of(aliasOrIndex, paramRouting);
            }
        }
        return routings;
    }

    /**
     * Sets the same routing for all indices
     */
    private Map<String, Set<String>> resolveSearchRoutingAllIndices(MetaData metaData, String routing) {
        if (routing != null) {
            Set<String> r = Strings.splitStringByCommaToSet(routing);
            Map<String, Set<String>> routings = newHashMap();
            String[] concreteIndices = metaData.concreteAllIndices();
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
    public static boolean isAllIndices(List<String> aliasesOrIndices) {
        return aliasesOrIndices == null || aliasesOrIndices.isEmpty() || isExplicitAllPattern(aliasesOrIndices);
    }

    /**
     * Identifies whether the array containing index names given as argument explicitly refers to all indices
     * The empty or null array doesn't explicitly map to all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array explicitly maps to all indices, false otherwise
     */
    static boolean isExplicitAllPattern(List<String> aliasesOrIndices) {
        return aliasesOrIndices != null && aliasesOrIndices.size() == 1 && MetaData.ALL.equals(aliasesOrIndices.get(0));
    }

    /**
     * Identifies whether the first argument (an array containing index names) is a pattern that matches all indices
     *
     * @param indicesOrAliases the array containing index names
     * @param concreteIndices  array containing the concrete indices that the first argument refers to
     * @return true if the first argument is a pattern that maps to all available indices, false otherwise
     */
    boolean isPatternMatchingAllIndices(MetaData metaData, String[] indicesOrAliases, String[] concreteIndices) {
        // if we end up matching on all indices, check, if its a wildcard parameter, or a "-something" structure
        if (concreteIndices.length == metaData.concreteAllIndices().length && indicesOrAliases.length > 0) {

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

    final static class Context {

        private final ClusterState state;
        private final IndicesOptions options;

        Context(ClusterState state, IndicesOptions options) {
            this.state = state;
            this.options = options;
        }

        public ClusterState getState() {
            return state;
        }

        public IndicesOptions getOptions() {
            return options;
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
    final static class WildcardExpressionResolver implements ExpressionResolver {

        @Override
        public List<String> resolve(Context context, List<String> expressions) {
            IndicesOptions options = context.getOptions();
            MetaData metaData = context.getState().metaData();
            if (options.expandWildcardsClosed() == false && options.expandWildcardsOpen() == false) {
                return expressions;
            }

            if (expressions.isEmpty() || (expressions.size() == 1 && MetaData.ALL.equals(expressions.get(0)))) {
                if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                    return Arrays.asList(metaData.concreteAllIndices());
                } else if (options.expandWildcardsOpen()) {
                    return Arrays.asList(metaData.concreteAllOpenIndices());
                } else if (options.expandWildcardsClosed()) {
                    return Arrays.asList(metaData.concreteAllClosedIndices());
                } else {
                    return Collections.emptyList();
                }
            }

            Set<String> result = null;
            for (int i = 0; i < expressions.size(); i++) {
                String aliasOrIndex = expressions.get(i);
                if (metaData.getAliasAndIndexMap().containsKey(aliasOrIndex)) {
                    if (result != null) {
                        result.add(aliasOrIndex);
                    }
                    continue;
                }
                boolean add = true;
                if (aliasOrIndex.charAt(0) == '+') {
                    // if its the first, add empty result set
                    if (i == 0) {
                        result = new HashSet<>();
                    }
                    add = true;
                    aliasOrIndex = aliasOrIndex.substring(1);
                } else if (aliasOrIndex.charAt(0) == '-') {
                    // if its the first, fill it with all the indices...
                    if (i == 0) {
                        String[] concreteIndices;
                        if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                            concreteIndices = metaData.concreteAllIndices();
                        } else if (options.expandWildcardsOpen()) {
                            concreteIndices = metaData.concreteAllOpenIndices();
                        } else if (options.expandWildcardsClosed()) {
                            concreteIndices = metaData.concreteAllClosedIndices();
                        } else {
                            assert false : "Shouldn't end up here";
                            concreteIndices = Strings.EMPTY_ARRAY;
                        }
                        result = new HashSet<>(Arrays.asList(concreteIndices));
                    }
                    add = false;
                    aliasOrIndex = aliasOrIndex.substring(1);
                }
                if (!Regex.isSimpleMatchPattern(aliasOrIndex)) {
                    if (!options.ignoreUnavailable() && !metaData.getAliasAndIndexMap().containsKey(aliasOrIndex)) {
                        throw new IndexMissingException(new Index(aliasOrIndex));
                    }
                    if (result != null) {
                        if (add) {
                            result.add(aliasOrIndex);
                        } else {
                            result.remove(aliasOrIndex);
                        }
                    }
                    continue;
                }
                if (result == null) {
                    // add all the previous ones...
                    result = new HashSet<>();
                    result.addAll(expressions.subList(0, i));
                }
                String[] indices;
                if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                    indices = metaData.concreteAllIndices();
                } else if (options.expandWildcardsOpen()) {
                    indices = metaData.concreteAllOpenIndices();
                } else if (options.expandWildcardsClosed()) {
                    indices = metaData.concreteAllClosedIndices();
                } else {
                    assert false : "this shouldn't get called if wildcards expand to none";
                    indices = Strings.EMPTY_ARRAY;
                }
                boolean found = false;
                // iterating over all concrete indices and see if there is a wildcard match
                for (String index : indices) {
                    if (Regex.simpleMatch(aliasOrIndex, index)) {
                        found = true;
                        if (add) {
                            result.add(index);
                        } else {
                            result.remove(index);
                        }
                    }
                }
                // iterating over all aliases and see if there is a wildcard match
                for (ObjectCursor<String> cursor : metaData.getAliases().keys()) {
                    String alias = cursor.value;
                    if (Regex.simpleMatch(aliasOrIndex, alias)) {
                        found = true;
                        if (add) {
                            result.add(alias);
                        } else {
                            result.remove(alias);
                        }
                    }
                }
                if (!found && !options.allowNoIndices()) {
                    throw new IndexMissingException(new Index(aliasOrIndex));
                }
            }
            if (result == null) {
                return expressions;
            }
            if (result.isEmpty() && !options.allowNoIndices()) {
                throw new IndexMissingException(new Index(StringUtils.join(expressions.iterator(), ',')));
            }
            return new ArrayList<>(result);
        }
    }

}
