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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class IndexNameExpressionResolver extends AbstractComponent {

    private final List<ExpressionResolver> expressionResolvers;
    private final DateMathExpressionResolver dateMathExpressionResolver;

    public IndexNameExpressionResolver(Settings settings) {
        super(settings);
        expressionResolvers = Arrays.asList(
                dateMathExpressionResolver = new DateMathExpressionResolver(settings),
                new WildcardExpressionResolver()
        );
    }

    /**
     * Same as {@link #concreteIndexNames(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
        Context context = new Context(state, request.indicesOptions());
        return concreteIndexNames(context, request.indices());
    }

    /**
     * Same as {@link #concreteIndices(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
        Context context = new Context(state, request.indicesOptions());
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
        Context context = new Context(state, options);
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
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, long startTime, String... indexExpressions) {
        Context context = new Context(state, options, startTime);
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
            indexExpressions = new String[]{MetaData.ALL};
        }
        MetaData metaData = context.getState().metaData();
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
                IndexNotFoundException infe = new IndexNotFoundException((String)null);
                infe.setResources("index_expression", indexExpressions);
                throw infe;
            } else {
                return Index.EMPTY_ARRAY;
            }
        }

        final Set<Index> concreteIndices = new HashSet<>(expressions.size());
        for (String expression : expressions) {
            AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(expression);
            if (aliasOrIndex == null ) {
                if (failNoIndices) {
                    IndexNotFoundException infe = new IndexNotFoundException(expression);
                    infe.setResources("index_expression", expression);
                    throw infe;
                } else {
                    continue;
                }
            } else if (aliasOrIndex.isAlias() && context.getOptions().ignoreAliases()) {
                if (failNoIndices) {
                    throw aliasesNotSupportedException(expression);
                } else {
                    continue;
                }
            }

            Collection<IndexMetaData> resolvedIndices = aliasOrIndex.getIndices();
            if (resolvedIndices.size() > 1 && !options.allowAliasesToMultipleIndices()) {
                String[] indexNames = new String[resolvedIndices.size()];
                int i = 0;
                for (IndexMetaData indexMetaData : resolvedIndices) {
                    indexNames[i++] = indexMetaData.getIndex().getName();
                }
                throw new IllegalArgumentException("Alias [" + expression + "] has more than one indices associated with it [" +
                        Arrays.toString(indexNames) + "], can't execute a single index op");
            }

            for (IndexMetaData index : resolvedIndices) {
                if (index.getState() == IndexMetaData.State.CLOSE) {
                    if (failClosed) {
                        throw new IndexClosedException(index.getIndex());
                    } else {
                        if (options.forbidClosedIndices() == false) {
                            concreteIndices.add(index.getIndex());
                        }
                    }
                } else if (index.getState() == IndexMetaData.State.OPEN) {
                    concreteIndices.add(index.getIndex());
                } else {
                    throw new IllegalStateException("index state [" + index.getState() + "] not supported");
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

    private static IllegalArgumentException aliasesNotSupportedException(String expression) {
        return new IllegalArgumentException("The provided expression [" + expression + "] matches an " +
                "alias, specify the corresponding concrete indices instead.");
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
        String indexExpression = request.indices() != null && request.indices().length > 0 ? request.indices()[0] : null;
        Index[] indices = concreteIndices(state, request.indicesOptions(), indexExpression);
        if (indices.length != 1) {
            throw new IllegalArgumentException("unable to return a single index as the index and options provided got resolved to multiple indices");
        }
        return indices[0];
    }

    /**
     * @return whether the specified alias or index exists. If the alias or index contains datemath then that is resolved too.
     */
    public boolean hasIndexOrAlias(String aliasOrIndex, ClusterState state) {
        Context context = new Context(state, IndicesOptions.lenientExpandOpen());
        String resolvedAliasOrIndex = dateMathExpressionResolver.resolveExpression(aliasOrIndex, context);
        return state.metaData().getAliasAndIndexLookup().containsKey(resolvedAliasOrIndex);
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
     * Iterates through the list of indices and selects the effective list of filtering aliases for the
     * given index.
     * <p>Only aliases with filters are returned. If the indices list contains a non-filtering reference to
     * the index itself - null is returned. Returns <tt>null</tt> if no filtering is required.
     */
    public String[] filteringAliases(ClusterState state, String index, String... expressions) {
        return indexAliases(state, index, AliasMetaData::filteringRequired, false, expressions);
    }

    /**
     * Iterates through the list of indices and selects the effective list of required aliases for the given index.
     * <p>Only aliases where the given predicate tests successfully are returned. If the indices list contains a non-required reference to
     * the index itself - null is returned. Returns <tt>null</tt> if no filtering is required.
     */
    public String[] indexAliases(ClusterState state, String index, Predicate<AliasMetaData> requiredAlias, boolean skipIdentity,
                                 String... expressions) {
        // expand the aliases wildcard
        List<String> resolvedExpressions = expressions != null ? Arrays.asList(expressions) : Collections.emptyList();
        Context context = new Context(state, IndicesOptions.lenientExpandOpen(), true);
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            resolvedExpressions = expressionResolver.resolve(context, resolvedExpressions);
        }

        if (isAllIndices(resolvedExpressions)) {
            return null;
        }
        final IndexMetaData indexMetaData = state.metaData().getIndices().get(index);
        if (indexMetaData == null) {
            // Shouldn't happen
            throw new IndexNotFoundException(index);
        }
        // optimize for the most common single index/alias scenario
        if (resolvedExpressions.size() == 1) {
            String alias = resolvedExpressions.get(0);

            AliasMetaData aliasMetaData = indexMetaData.getAliases().get(alias);
            if (aliasMetaData == null || requiredAlias.test(aliasMetaData) == false) {
                return null;
            }
            return new String[]{alias};
        }
        List<String> aliases = null;
        for (String alias : resolvedExpressions) {
            if (alias.equals(index)) {
                if (skipIdentity) {
                    continue;
                } else {
                    return null;
                }
            }
            AliasMetaData aliasMetaData = indexMetaData.getAliases().get(alias);
            // Check that this is an alias for the current index
            // Otherwise - skip it
            if (aliasMetaData != null) {
                if (requiredAlias.test(aliasMetaData)) {
                    // If required - add it to the list of aliases
                    if (aliases == null) {
                        aliases = new ArrayList<>();
                    }
                    aliases.add(alias);
                } else {
                    // If not, we have a non required alias for this index - no futher checking needed
                    return null;
                }
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
        List<String> resolvedExpressions = expressions != null ? Arrays.asList(expressions) : Collections.<String>emptyList();
        Context context = new Context(state, IndicesOptions.lenientExpandOpen());
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            resolvedExpressions = expressionResolver.resolve(context, resolvedExpressions);
        }

        // TODO: it appears that this can never be true?
        if (isAllIndices(resolvedExpressions)) {
            return resolveSearchRoutingAllIndices(state.metaData(), routing);
        }

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();
        if (routing != null) {
            paramRouting = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
        }

        for (String expression : resolvedExpressions) {
            AliasOrIndex aliasOrIndex = state.metaData().getAliasAndIndexLookup().get(expression);
            if (aliasOrIndex != null && aliasOrIndex.isAlias()) {
                AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
                for (Tuple<String, AliasMetaData> item : alias.getConcreteIndexAndAliasMetaDatas()) {
                    String concreteIndex = item.v1();
                    AliasMetaData aliasMetaData = item.v2();
                    if (!norouting.contains(concreteIndex)) {
                        if (!aliasMetaData.searchRoutingValues().isEmpty()) {
                            // Routing alias
                            if (routings == null) {
                                routings = new HashMap<>();
                            }
                            Set<String> r = routings.get(concreteIndex);
                            if (r == null) {
                                r = new HashSet<>();
                                routings.put(concreteIndex, r);
                            }
                            r.addAll(aliasMetaData.searchRoutingValues());
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
    public Map<String, Set<String>> resolveSearchRoutingAllIndices(MetaData metaData, String routing) {
        if (routing != null) {
            Set<String> r = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
            Map<String, Set<String>> routings = new HashMap<>();
            String[] concreteIndices = metaData.getConcreteAllIndices();
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
        if (concreteIndices.length == metaData.getConcreteAllIndices().length && indicesOrAliases.length > 0) {

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

    static final class Context {

        private final ClusterState state;
        private final IndicesOptions options;
        private final long startTime;
        private final boolean preserveAliases;

        Context(ClusterState state, IndicesOptions options) {
            this(state, options, System.currentTimeMillis());
        }

        Context(ClusterState state, IndicesOptions options, boolean preserveAliases) {
            this(state, options, System.currentTimeMillis(), preserveAliases);
        }

        Context(ClusterState state, IndicesOptions options, long startTime) {
           this(state, options, startTime, false);
        }

        Context(ClusterState state, IndicesOptions options, long startTime, boolean preserveAliases) {
            this.state = state;
            this.options = options;
            this.startTime = startTime;
            this.preserveAliases = preserveAliases;
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
         * by {@link #filteringAliases(ClusterState, String, String...)} since it's the only one that needs aliases
         */
        boolean isPreserveAliases() {
            return preserveAliases;
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
            MetaData metaData = context.getState().metaData();
            if (options.expandWildcardsClosed() == false && options.expandWildcardsOpen() == false) {
                return expressions;
            }

            if (isEmptyOrTrivialWildcard(expressions)) {
                return resolveEmptyOrTrivialWildcard(options, metaData);
            }

            Set<String> result = innerResolve(context, expressions, options, metaData);

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

        private Set<String> innerResolve(Context context, List<String> expressions, IndicesOptions options, MetaData metaData) {
            Set<String> result = null;
            boolean wildcardSeen = false;
            for (int i = 0; i < expressions.size(); i++) {
                String expression = expressions.get(i);
                if (Strings.isEmpty(expression)) {
                    throw indexNotFoundException(expression);
                }
                validateAliasOrIndex(expression);
                if (aliasOrIndexExists(options, metaData, expression)) {
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
                if (!Regex.isSimpleMatchPattern(expression)) {
                    //TODO why does wildcard resolver throw exceptions regarding non wildcarded expressions? This should not be done here.
                    if (options.ignoreUnavailable() == false) {
                        AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(expression);
                        if (aliasOrIndex == null) {
                            throw indexNotFoundException(expression);
                        } else if (aliasOrIndex.isAlias() && options.ignoreAliases()) {
                            throw aliasesNotSupportedException(expression);
                        }
                    }
                    if (add) {
                        result.add(expression);
                    } else {
                        result.remove(expression);
                    }
                    continue;
                }

                final IndexMetaData.State excludeState = excludeState(options);
                final Map<String, AliasOrIndex> matches = matches(context, metaData, expression);
                Set<String> expand = expand(context, excludeState, matches);
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

        private static boolean aliasOrIndexExists(IndicesOptions options, MetaData metaData, String expression) {
            AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(expression);
            //treat aliases as unavailable indices when ignoreAliases is set to true (e.g. delete index and update aliases api)
            return aliasOrIndex != null && (options.ignoreAliases() == false || aliasOrIndex.isAlias() == false);
        }

        private static IndexNotFoundException indexNotFoundException(String expression) {
            IndexNotFoundException infe = new IndexNotFoundException(expression);
            infe.setResources("index_or_alias", expression);
            return infe;
        }

        private static IndexMetaData.State excludeState(IndicesOptions options) {
            final IndexMetaData.State excludeState;
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                excludeState = null;
            } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed() == false) {
                excludeState = IndexMetaData.State.CLOSE;
            } else if (options.expandWildcardsClosed() && options.expandWildcardsOpen() == false) {
                excludeState = IndexMetaData.State.OPEN;
            } else {
                assert false : "this shouldn't get called if wildcards expand to none";
                excludeState = null;
            }
            return excludeState;
        }

        public static Map<String, AliasOrIndex> matches(Context context, MetaData metaData, String expression) {
            if (Regex.isMatchAllPattern(expression)) {
                // Can only happen if the expressions was initially: '-*'
                if (context.getOptions().ignoreAliases()) {
                    return metaData.getAliasAndIndexLookup().entrySet().stream()
                            .filter(e -> e.getValue().isAlias() == false)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                } else {
                    return metaData.getAliasAndIndexLookup();
                }
            } else if (expression.indexOf("*") == expression.length() - 1) {
                return suffixWildcard(context, metaData, expression);
            } else {
                return otherWildcard(context, metaData, expression);
            }
        }

        private static Map<String, AliasOrIndex> suffixWildcard(Context context, MetaData metaData, String expression) {
            assert expression.length() >= 2 : "expression [" + expression + "] should have at least a length of 2";
            String fromPrefix = expression.substring(0, expression.length() - 1);
            char[] toPrefixCharArr = fromPrefix.toCharArray();
            toPrefixCharArr[toPrefixCharArr.length - 1]++;
            String toPrefix = new String(toPrefixCharArr);
            SortedMap<String,AliasOrIndex> subMap = metaData.getAliasAndIndexLookup().subMap(fromPrefix, toPrefix);
            if (context.getOptions().ignoreAliases()) {
                 return subMap.entrySet().stream()
                        .filter(entry -> entry.getValue().isAlias() == false)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            return subMap;
        }

        private static Map<String, AliasOrIndex> otherWildcard(Context context, MetaData metaData, String expression) {
            final String pattern = expression;
            return metaData.getAliasAndIndexLookup()
                .entrySet()
                .stream()
                .filter(e -> context.getOptions().ignoreAliases() == false || e.getValue().isAlias() == false)
                .filter(e -> Regex.simpleMatch(pattern, e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private static Set<String> expand(Context context, IndexMetaData.State excludeState, Map<String, AliasOrIndex> matches) {
            Set<String> expand = new HashSet<>();
            for (Map.Entry<String, AliasOrIndex> entry : matches.entrySet()) {
                AliasOrIndex aliasOrIndex = entry.getValue();
                if (context.isPreserveAliases() && aliasOrIndex.isAlias()) {
                    expand.add(entry.getKey());
                } else {
                    for (IndexMetaData meta : aliasOrIndex.getIndices()) {
                        if (excludeState == null || meta.getState() != excludeState) {
                            expand.add(meta.getIndex().getName());
                        }
                    }
                }
            }
            return expand;
        }

        private boolean isEmptyOrTrivialWildcard(List<String> expressions) {
            return expressions.isEmpty() || (expressions.size() == 1 && (MetaData.ALL.equals(expressions.get(0)) || Regex.isMatchAllPattern(expressions.get(0))));
        }

        private static List<String> resolveEmptyOrTrivialWildcard(IndicesOptions options, MetaData metaData) {
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                return Arrays.asList(metaData.getConcreteAllIndices());
            } else if (options.expandWildcardsOpen()) {
                return Arrays.asList(metaData.getConcreteAllOpenIndices());
            } else if (options.expandWildcardsClosed()) {
                return Arrays.asList(metaData.getConcreteAllClosedIndices());
            } else {
                return Collections.emptyList();
            }
        }
    }

    static final class DateMathExpressionResolver implements ExpressionResolver {

        private static final String EXPRESSION_LEFT_BOUND = "<";
        private static final String EXPRESSION_RIGHT_BOUND = ">";
        private static final char LEFT_BOUND = '{';
        private static final char RIGHT_BOUND = '}';
        private static final char ESCAPE_CHAR = '\\';
        private static final char TIME_ZONE_BOUND = '|';

        private final DateTimeZone defaultTimeZone;
        private final String defaultDateFormatterPattern;
        private final DateTimeFormatter defaultDateFormatter;

        DateMathExpressionResolver(Settings settings) {
            String defaultTimeZoneId = settings.get("date_math_expression_resolver.default_time_zone", "UTC");
            this.defaultTimeZone = DateTimeZone.forID(defaultTimeZoneId);
            defaultDateFormatterPattern = settings.get("date_math_expression_resolver.default_date_format", "YYYY.MM.dd");
            this.defaultDateFormatter = DateTimeFormat.forPattern(defaultDateFormatterPattern);
        }

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
                                throw new ElasticsearchParseException("invalid dynamic name expression [{}]. invalid character in placeholder at position [{}]", new String(text, from, length), i);
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
                                DateTimeFormatter dateFormatter;
                                final DateTimeZone timeZone;
                                if (dateTimeFormatLeftBoundIndex < 0) {
                                    mathExpression = inPlaceHolderString;
                                    dateFormatterPattern = defaultDateFormatterPattern;
                                    dateFormatter = defaultDateFormatter;
                                    timeZone = defaultTimeZone;
                                } else {
                                    if (inPlaceHolderString.lastIndexOf(RIGHT_BOUND) != inPlaceHolderString.length() - 1) {
                                        throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing closing `}` for date math format", inPlaceHolderString);
                                    }
                                    if (dateTimeFormatLeftBoundIndex == inPlaceHolderString.length() - 2) {
                                        throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing date format", inPlaceHolderString);
                                    }
                                    mathExpression = inPlaceHolderString.substring(0, dateTimeFormatLeftBoundIndex);
                                    String dateFormatterPatternAndTimeZoneId = inPlaceHolderString.substring(dateTimeFormatLeftBoundIndex + 1, inPlaceHolderString.length() - 1);
                                    int formatPatternTimeZoneSeparatorIndex = dateFormatterPatternAndTimeZoneId.indexOf(TIME_ZONE_BOUND);
                                    if (formatPatternTimeZoneSeparatorIndex != -1) {
                                        dateFormatterPattern = dateFormatterPatternAndTimeZoneId.substring(0, formatPatternTimeZoneSeparatorIndex);
                                        timeZone = DateTimeZone.forID(dateFormatterPatternAndTimeZoneId.substring(formatPatternTimeZoneSeparatorIndex + 1));
                                    } else {
                                        dateFormatterPattern = dateFormatterPatternAndTimeZoneId;
                                        timeZone = defaultTimeZone;
                                    }
                                    dateFormatter = DateTimeFormat.forPattern(dateFormatterPattern);
                                }
                                DateTimeFormatter parser = dateFormatter.withZone(timeZone);
                                FormatDateTimeFormatter formatter = new FormatDateTimeFormatter(dateFormatterPattern, parser, Locale.ROOT);
                                DateMathParser dateMathParser = new DateMathParser(formatter);
                            long millis = dateMathParser.parse(mathExpression, context::getStartTime, false, timeZone);

                                String time = formatter.printer().print(millis);
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
                                throw new ElasticsearchParseException("invalid dynamic name expression [{}]. invalid character at position [{}]. " +
                                        "`{` and `}` are reserved characters and should be escaped when used as part of the index name using `\\` (e.g. `\\{text\\}`)", new String(text, from, length), i);
                            }
                        default:
                            beforePlaceHolderSb.append(c);
                    }
                }
            }

            if (inPlaceHolder) {
                throw new ElasticsearchParseException("invalid dynamic name expression [{}]. date math placeholder is open ended", new String(text, from, length));
            }
            if (beforePlaceHolderSb.length() == 0) {
                throw new ElasticsearchParseException("nothing captured");
            }
            return beforePlaceHolderSb.toString();
        }
    }

    /**
     * Returns <code>true</code> iff the given expression resolves to the given index name otherwise <code>false</code>
     */
    public final boolean matchesIndex(String indexName, String expression, ClusterState state) {
        final String[] concreteIndices = concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), expression);
        for (String index : concreteIndices) {
            if (Regex.simpleMatch(index, indexName)) {
                return true;
            }
        }
        return indexName.equals(expression);
    }

}
