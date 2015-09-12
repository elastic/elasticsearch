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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
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
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class IndexNameExpressionResolver extends AbstractComponent {

    private final List<ExpressionResolver> expressionResolvers;
    private final DateMathExpressionResolver dateMathExpressionResolver;

    @Inject
    public IndexNameExpressionResolver(Settings settings) {
        super(settings);
        expressionResolvers = Arrays.asList(
                dateMathExpressionResolver = new DateMathExpressionResolver(settings),
                new WildcardExpressionResolver()
        );
    }

    /**
     * Same as {@link #concreteIndices(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public String[] concreteIndices(ClusterState state, IndicesRequest request) {
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
     * @throws IndexNotFoundException if one of the index expressions is pointing to a missing index or alias and the
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options in the context don't allow such a case.
     */
    public String[] concreteIndices(ClusterState state, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(state, options);
        return concreteIndices(context, indexExpressions);
    }

    /**
     * Translates the provided index expression into actual concrete indices.
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
    public String[] concreteIndices(ClusterState state, IndicesOptions options, long startTime, String... indexExpressions) {
        Context context = new Context(state, options, startTime);
        return concreteIndices(context, indexExpressions);
    }

    String[] concreteIndices(Context context, String... indexExpressions) {
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
                IndexNotFoundException infe = new IndexNotFoundException((String)null);
                infe.setResources("index_expression", indexExpressions);
                throw infe;
            } else {
                return Strings.EMPTY_ARRAY;
            }
        }

        List<String> concreteIndices = new ArrayList<>(expressions.size());
        for (String expression : expressions) {
            AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(expression);
            if (aliasOrIndex == null) {
                if (failNoIndices) {
                    IndexNotFoundException infe = new IndexNotFoundException(expression);
                    infe.setResources("index_expression", expression);
                    throw infe;
                } else {
                    continue;
                }
            }

            Collection<IndexMetaData> resolvedIndices = aliasOrIndex.getIndices();
            if (resolvedIndices.size() > 1 && !options.allowAliasesToMultipleIndices()) {
                String[] indexNames = new String[resolvedIndices.size()];
                int i = 0;
                for (IndexMetaData indexMetaData : resolvedIndices) {
                    indexNames[i++] = indexMetaData.getIndex();
                }
                throw new IllegalArgumentException("Alias [" + expression + "] has more than one indices associated with it [" + Arrays.toString(indexNames) + "], can't execute a single index op");
            }

            for (IndexMetaData index : resolvedIndices) {
                if (index.getState() == IndexMetaData.State.CLOSE) {
                    if (failClosed) {
                        throw new IndexClosedException(new Index(index.getIndex()));
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
        return concreteIndices.toArray(new String[concreteIndices.size()]);
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
    public String concreteSingleIndex(ClusterState state, IndicesRequest request) {
        String indexExpression = request.indices() != null && request.indices().length > 0 ? request.indices()[0] : null;
        String[] indices = concreteIndices(state, request.indicesOptions(), indexExpression);
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
                throw new IndexNotFoundException(index);
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
                throw new IndexNotFoundException(index);
            }

            AliasMetaData aliasMetaData = indexMetaData.aliases().get(alias);
            // Check that this is an alias for the current index
            // Otherwise - skip it
            if (aliasMetaData != null) {
                boolean filteringRequired = aliasMetaData.filteringRequired();
                if (filteringRequired) {
                    // If filtering required - add it to the list of filters
                    if (filteringAliases == null) {
                        filteringAliases = new ArrayList<>();
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

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();
        if (routing != null) {
            paramRouting = Strings.splitStringByCommaToSet(routing);
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
    private Map<String, Set<String>> resolveSearchRoutingAllIndices(MetaData metaData, String routing) {
        if (routing != null) {
            Set<String> r = Strings.splitStringByCommaToSet(routing);
            Map<String, Set<String>> routings = new HashMap<>();
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
        private final long startTime;

        Context(ClusterState state, IndicesOptions options) {
            this.state = state;
            this.options = options;
            startTime = System.currentTimeMillis();
        }

        public Context(ClusterState state, IndicesOptions options, long startTime) {
            this.state = state;
            this.options = options;
            this.startTime = startTime;
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

            if (expressions.isEmpty() || (expressions.size() == 1 && (MetaData.ALL.equals(expressions.get(0))) || Regex.isMatchAllPattern(expressions.get(0)))) {
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
                String expression = expressions.get(i);
                if (metaData.getAliasAndIndexLookup().containsKey(expression)) {
                    if (result != null) {
                        result.add(expression);
                    }
                    continue;
                }
                boolean add = true;
                if (expression.charAt(0) == '+') {
                    // if its the first, add empty result set
                    if (i == 0) {
                        result = new HashSet<>();
                    }
                    add = true;
                    expression = expression.substring(1);
                } else if (expression.charAt(0) == '-') {
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
                    expression = expression.substring(1);
                }
                if (!Regex.isSimpleMatchPattern(expression)) {
                    if (!options.ignoreUnavailable() && !metaData.getAliasAndIndexLookup().containsKey(expression)) {
                        IndexNotFoundException infe = new IndexNotFoundException(expression);
                        infe.setResources("index_or_alias", expression);
                        throw infe;
                    }
                    if (result != null) {
                        if (add) {
                            result.add(expression);
                        } else {
                            result.remove(expression);
                        }
                    }
                    continue;
                }
                if (result == null) {
                    // add all the previous ones...
                    result = new HashSet<>();
                    result.addAll(expressions.subList(0, i));
                }

                final IndexMetaData.State excludeState;
                if (options.expandWildcardsOpen() && options.expandWildcardsClosed()){
                    excludeState = null;
                } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed() == false) {
                    excludeState = IndexMetaData.State.CLOSE;
                } else if (options.expandWildcardsClosed() && options.expandWildcardsOpen() == false) {
                    excludeState = IndexMetaData.State.OPEN;
                } else {
                    assert false : "this shouldn't get called if wildcards expand to none";
                    excludeState = null;
                }

                final Map<String, AliasOrIndex> matches;
                if (Regex.isMatchAllPattern(expression)) {
                    // Can only happen if the expressions was initially: '-*'
                    matches = metaData.getAliasAndIndexLookup();
                } else if (expression.endsWith("*")) {
                    // Suffix wildcard:
                    assert expression.length() >= 2 : "expression [" + expression + "] should have at least a length of 2";
                    String fromPrefix = expression.substring(0, expression.length() - 1);
                    char[] toPrefixCharArr = fromPrefix.toCharArray();
                    toPrefixCharArr[toPrefixCharArr.length - 1]++;
                    String toPrefix = new String(toPrefixCharArr);
                    matches = metaData.getAliasAndIndexLookup().subMap(fromPrefix, toPrefix);
                } else {
                    // Other wildcard expressions:
                    final String pattern = expression;
                    matches = metaData.getAliasAndIndexLookup()
                            .entrySet()
                            .stream()
                            .filter(e -> Regex.simpleMatch(pattern, e.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                }
                for (Map.Entry<String, AliasOrIndex> entry : matches.entrySet()) {
                    AliasOrIndex aliasOrIndex = entry.getValue();
                    if (aliasOrIndex.isAlias() == false) {
                        AliasOrIndex.Index index = (AliasOrIndex.Index) aliasOrIndex;
                        if (excludeState != null && index.getIndex().getState() == excludeState) {
                            continue;
                        }
                    }

                    if (add) {
                        result.add(entry.getKey());
                    } else {
                        result.remove(entry.getKey());
                    }
                }

                if (matches.isEmpty() && options.allowNoIndices() == false) {
                    IndexNotFoundException infe = new IndexNotFoundException(expression);
                    infe.setResources("index_or_alias", expression);
                    throw infe;
                }
            }
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
    }

    final static class DateMathExpressionResolver implements ExpressionResolver {

        private static final String EXPRESSION_LEFT_BOUND = "<";
        private static final String EXPRESSION_RIGHT_BOUND = ">";
        private static final char LEFT_BOUND = '{';
        private static final char RIGHT_BOUND = '}';
        private static final char ESCAPE_CHAR = '\\';
        private static final char TIME_ZONE_BOUND = '|';

        private final DateTimeZone defaultTimeZone;
        private final String defaultDateFormatterPattern;
        private final DateTimeFormatter defaultDateFormatter;

        public DateMathExpressionResolver(Settings settings) {
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
                                long millis = dateMathParser.parse(mathExpression, new Callable<Long>() {
                                    @Override
                                    public Long call() throws Exception {
                                        return context.getStartTime();
                                    }
                                }, false, timeZone);

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

}
