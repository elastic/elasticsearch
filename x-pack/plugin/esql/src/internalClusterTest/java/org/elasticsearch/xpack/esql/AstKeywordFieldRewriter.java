/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Rewrites a csv-spec ES|QL query so that every reference to a keyword field that has been
 * converted to {@code flattened} is wrapped in a call to
 * {@code field_extract(<field>, "<wrapperSubKey>")}.
 * <p>
 * The rewriter parses the query into the unresolved {@link LogicalPlan} (via {@link EsqlTestUtils#TEST_PARSER})
 * and uses the tree as a structural oracle: every {@link UnresolvedAttribute} carries a {@link Source} whose
 * line/column position and verbatim text map back to an absolute character span in the original
 * query string. The walker decides whether each attribute sits in an expression position (where
 * {@code field_extract(...)} is legal) or in a project-mode / attribute-only position (where the
 * grammar accepts only a bare identifier), then splices the wrapper into the original text at the
 * collected spans &mdash; working right-to-left so earlier offsets stay valid. Because the parser
 * understands the full grammar (including {@code IN (subquery)}, {@code FORK}, {@code LOOKUP JOIN}),
 * the rewriter never injects a function call into a slot the lexer would reject, which the
 * text-only rewriter could not guarantee.
 *
 * <h2>Pipeline-aware scope tracking</h2>
 * The query is walked in pipeline order (source first). A running scope tracks which
 * converted-keyword fields are still bound to a {@code flattened} value at the current point.
 * Each command updates that scope the same way the analyzer would change the schema:
 * <ul>
 *   <li>{@code EVAL} wraps each assignment's right-hand side and then removes the assigned names
 *       from scope (they are now bound to the EVAL result, not the flattened field).</li>
 *   <li>{@code KEEP}/{@code DROP}/{@code RENAME} are left untouched in the text but their schema
 *       effect is tracked (narrow / remove / rename). Wildcards over-approximate.</li>
 *   <li>{@code STATS} / {@code INLINE STATS} wrap aggregate and grouping expressions; a bare
 *       in-scope grouping key is aliased through the {@link #BY_ALIAS_PREFIX} synthetic name so the
 *       wrapped grouping does not self-shadow, with a follow-up {@code RENAME} (and, for
 *       {@code INLINE STATS}, a {@code DROP}) restoring the test-expected column name. Plain
 *       {@code STATS} clears the scope; {@code INLINE STATS} preserves it minus rebound names.</li>
 *   <li>{@code DISSECT}/{@code GROK} wrap the input expression and drop the pattern-bound names
 *       from scope.</li>
 *   <li>{@code MV_EXPAND <field>} accepts only a bare attribute and would see a flattened field
 *       as a single wrapper object (nothing to expand), so an in-scope target is hoisted into an
 *       {@code EVAL <field> = field_extract(<field>, "v")} before the command &mdash; rebinding it
 *       to the multi-value {@code keyword} that {@code MV_EXPAND} can split into rows &mdash; and
 *       then leaves scope.</li>
 *   <li>{@code ENRICH} and {@code LOOKUP JOIN ... ON ...} accept only bare attributes in their
 *       bodies; in-scope references there are recorded as {@link SkipEvent}s instead of being
 *       wrapped.</li>
 *   <li>The left-hand side of the match operator {@code :} ({@link MatchOperator}) accepts only a
 *       bare attribute; in-scope references there are recorded as
 *       {@link SkipSite#MATCH_OPERATOR_LHS} skip events.</li>
 *   <li>{@code field IN (subquery)} hoists a bare in-scope left-hand side into an
 *       {@code EVAL field = field_extract(field, "v")} inserted before the {@code WHERE} (the
 *       IN-subquery resolver accepts only a bare attribute or constant on that side, rejecting a
 *       {@code field_extract(...)} call as a "Complicated IN subquery"), and recursively rewrites
 *       the subquery with a scope resolved from the subquery's own {@code FROM}, appending a
 *       tail-end {@code EVAL} so its projected column arrives as {@code keyword} too.</li>
 *   <li>{@code FORK} rewrites each branch as its own pipeline sharing the pre-fork scope; the
 *       shared input pipeline is rewritten once thanks to span de-duplication.</li>
 *   <li>{@code FROM a, (subquery), ...} (a subquery in the {@code FROM} command, parsed as a
 *       {@code UnionAll}) rewrites each branch as an independent source pipeline; the post-union scope
 *       is the union of the branch-end scopes. No cross-branch recovery is needed because the scope
 *       already excludes cross-dataset non-keyword conflicts, so every still-flattened field has a
 *       consistent type across the branches that produce it.</li>
 * </ul>
 *
 * <h2>Tail-end recovery</h2>
 * A converted-keyword field projected directly (via {@code KEEP}/{@code SORT}/etc. or by surviving
 * to the end with no terminal projection) reaches the runner typed as {@code flattened} while the
 * csv-spec declares it as {@code keyword}. The top-level walk therefore appends
 * {@code | EVAL name = field_extract(name, "v"), ...} for every field still in scope at
 * end-of-pipeline, and (when an expected column order is supplied) a trailing
 * {@code | KEEP <expectedColumnOrder>} so the affected columns return to their expected positions.
 */
public final class AstKeywordFieldRewriter {

    /** Name of the ES|QL function used to extract a sub-field from a {@code flattened} value. */
    public static final String FIELD_EXTRACT_FUNCTION = "field_extract";

    /**
     * Prefix for the synthetic alias names introduced for bare in-scope grouping keys in a
     * {@code STATS}/{@code INLINE STATS BY} clause, so the wrapped grouping does not collide with
     * (and self-shadow) the input column of the same name within the same segment.
     */
    private static final String BY_ALIAS_PREFIX = "__kw_";

    /**
     * Resolves the initial flattened-field scope for an arbitrary (sub-)query text. Invoked once
     * for the top-level query and once again for every recursive subquery so that a nested
     * {@code (FROM b | ...)} is rewritten with the keyword paths of {@code b} alone.
     */
    @FunctionalInterface
    public interface ScopeResolver {
        Set<String> resolveScope(String query);
    }

    /**
     * Identifies a syntactic site at which the rewriter declined to wrap an in-scope keyword
     * reference in {@code field_extract(...)} because the grammar slot accepts only a bare
     * attribute. Used by callers to inventory exactly which positions were left unwrapped.
     */
    public enum SkipSite {
        /** Body of an {@code MV_EXPAND <field>} command. */
        MV_EXPAND_ARG,
        /** Body of an {@code ENRICH <policy> [ON <field>] [WITH ...]} command (any sub-position). */
        ENRICH_BODY,
        /** Identifier on the left-hand side of a single-colon match operator ({@code field : "value"}). */
        MATCH_OPERATOR_LHS,
        /** Body of a {@code LOOKUP JOIN <index> ON <field>[, <field>]*} command. */
        LOOKUP_JOIN_ON,
        /** Identifier inside an ES|QL qualified-name reference of the form {@code [<index>].[<field>]}. */
        QUALIFIED_NAME_BRACKETS
    }

    /**
     * One occurrence of an in-scope keyword field that the rewriter intentionally did not wrap.
     *
     * @param site  the syntactic context that forced the skip; see {@link SkipSite}
     * @param field the field name that was left unwrapped
     */
    public record SkipEvent(SkipSite site, String field) {}

    /**
     * Result of {@link #rewrite(String, ScopeResolver, String, List)}.
     *
     * @param rewrittenQuery      the rewritten query string, or the original query if nothing was rewritten
     * @param modified            {@code true} iff the rewritten query differs from the original
     * @param rewrittenFieldNames the set of keyword field names that were actually wrapped at least once
     * @param skipEvents          every in-scope keyword reference that was left unwrapped, in pipeline order
     * @param wrappedMatchFunctionArg {@code true} iff a {@code MATCH(...)} function argument was wrapped in
     *                            {@code field_extract(...)}, so the caller can enable runtime lexical search
     * @param coveredArguments    the {@code function:argIndex} contexts in which an in-scope reference was wrapped
     */
    public record RewriteResult(
        String rewrittenQuery,
        boolean modified,
        Set<String> rewrittenFieldNames,
        List<SkipEvent> skipEvents,
        boolean wrappedMatchFunctionArg,
        Set<String> coveredArguments
    ) {}

    private AstKeywordFieldRewriter() {}

    private static final Map<String, String> CLASS_NAME_TO_OPERATOR_NAME = new java.util.HashMap<>();
    static {
        for (DocsV3Support.OperatorConfig config : DocsV3Support.OPERATORS.values()) {
            CLASS_NAME_TO_OPERATOR_NAME.put(config.clazz().getName(), config.name().toUpperCase(Locale.ROOT));
        }
    }

    private static String getExpressionName(Expression expression) {
        if (expression instanceof org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction unf) {
            String name = unf.name().toUpperCase(Locale.ROOT);
            if (name.equals("TO_INT")) {
                // TODO remove the weird hack for TO_INT.
                return "TO_INTEGER";
            }
            return name;
        }

        String opName = CLASS_NAME_TO_OPERATOR_NAME.get(expression.getClass().getName());
        if (opName != null) {
            return opName;
        }

        if (expression instanceof Function function) {
            return function.functionName().toUpperCase(Locale.ROOT);
        }

        return null;
    }

    /**
     * Rewrites {@code query} by parsing it, walking its pipeline commands, and wrapping every
     * reference to a field in scope as {@code field_extract(field, "wrapperSubKey")}. The supplied
     * {@link ScopeResolver} computes the initial scope for the top-level query and for each
     * recursive subquery. When the resolved initial scope is empty the original query is returned
     * with {@link RewriteResult#modified()} set to {@code false}. If the query cannot be parsed the
     * original query is also returned unchanged so the unmodified spec runs as-is.
     */
    public static RewriteResult rewrite(String query, ScopeResolver scopeResolver, String wrapperSubKey, List<String> expectedColumnOrder) {
        Set<String> initialScope = scopeResolver.resolveScope(query);
        if (initialScope.isEmpty()) {
            return new RewriteResult(query, false, Set.of(), List.of(), false, Set.of());
        }
        LogicalPlan plan;
        try {
            plan = EsqlTestUtils.TEST_PARSER.parseQuery(query);
        } catch (Exception e) {
            // The query uses grammar this parser configuration rejects (or relies on bound params);
            // leave it unmodified so the unmodified spec runs and any failure is attributable to the
            // engine rather than to a malformed rewrite.
            return new RewriteResult(query, false, Set.of(), List.of(), false, Set.of());
        }
        Walker walker = new Walker(query, scopeResolver, wrapperSubKey);
        Set<String> endScope = walker.processPipeline(plan, initialScope);
        String body = walker.applyEdits();
        String rewritten = appendTopLevelTailRecovery(body, endScope, expectedColumnOrder, wrapperSubKey, walker.rewrittenNames);
        boolean modified = rewritten.equals(query) == false;
        return new RewriteResult(
            rewritten,
            modified,
            Set.copyOf(walker.rewrittenNames),
            List.copyOf(walker.skipEvents),
            walker.wrappedMatchFunctionArg,
            Set.copyOf(walker.coveredArguments)
        );
    }

    /**
     * Appends the top-level tail-end recovery to {@code body}: an {@code | EVAL ...} that rebinds
     * every field still in scope (intersected with {@code expectedColumnOrder} when supplied) back
     * to its keyword value, optionally followed by a {@code | KEEP <expectedColumnOrder>} to
     * restore the test-expected column positions.
     */
    private static String appendTopLevelTailRecovery(
        String body,
        Set<String> endScope,
        List<String> expectedColumnOrder,
        String wrapperSubKey,
        Set<String> rewrittenNamesSink
    ) {
        List<String> recoverable = recoverableFields(endScope, expectedColumnOrder);
        if (recoverable.isEmpty()) {
            return body;
        }
        StringBuilder sb = new StringBuilder(body);
        sb.append(evalRecovery(recoverable, wrapperSubKey));
        rewrittenNamesSink.addAll(recoverable);
        if (expectedColumnOrder.isEmpty() == false) {
            sb.append("\n| KEEP ");
            for (int i = 0; i < expectedColumnOrder.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(quoteIdentifierIfNeeded(expectedColumnOrder.get(i)));
            }
        }
        return sb.toString();
    }

    /**
     * The fields that need tail-end recovery: every field in {@code endScope}, intersected with
     * {@code expectedColumnOrder} when that order is known so a conservative wildcard
     * {@code KEEP}/{@code DROP} earlier in the pipeline does not produce an EVAL referencing a
     * column the schema no longer has.
     */
    private static List<String> recoverableFields(Set<String> endScope, List<String> expectedColumnOrder) {
        if (endScope.isEmpty()) {
            return List.of();
        }
        List<String> recoverable = new ArrayList<>();
        if (expectedColumnOrder.isEmpty()) {
            recoverable.addAll(endScope);
        } else {
            Set<String> expectedSet = new HashSet<>(expectedColumnOrder);
            for (String name : endScope) {
                if (expectedSet.contains(name)) {
                    recoverable.add(name);
                }
            }
        }
        recoverable.sort(Comparator.naturalOrder());
        return recoverable;
    }

    /** Builds a {@code \n| EVAL a = field_extract(a, "v"), b = ...} clause for the given fields (already ordered). */
    private static String evalRecovery(List<String> fields, String wrapperSubKey) {
        StringBuilder sb = new StringBuilder("\n| EVAL ");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            String name = fields.get(i);
            sb.append(name).append(" = ").append(extractCall(name, wrapperSubKey));
        }
        return sb.toString();
    }

    /** Returns {@code field_extract(<inner>, "<wrapperSubKey>")}. */
    private static String extractCall(String inner, String wrapperSubKey) {
        return FIELD_EXTRACT_FUNCTION + "(" + inner + ", \"" + wrapperSubKey + "\")";
    }

    /** Returns {@code name} unchanged when it is a valid bare identifier, otherwise wrapped in backticks. */
    private static String quoteIdentifierIfNeeded(String name) {
        if (name.isEmpty()) {
            return "``";
        }
        char first = name.charAt(0);
        if ((Character.isLetter(first) || first == '_') == false) {
            return "`" + name + "`";
        }
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isLetterOrDigit(c) == false && c != '_' && c != '.') {
                return "`" + name + "`";
            }
        }
        return name;
    }

    /** A single text edit: replace {@code [start, end)} of the original query with {@code replacement}. */
    private record Edit(int start, int end, String replacement) {}

    /**
     * Stateful walker over a parsed pipeline. Collects {@link Edit}s against the original query
     * string and tracks the set of fields actually wrapped and the skip events emitted. The walker
     * is reused for recursive subqueries and FORK branches so all edits land in a single list that
     * is applied once to the original text.
     */
    private static final class Walker {
        private final String query;
        private final ScopeResolver scopeResolver;
        private final String wrapperSubKey;
        private final int[] lineStarts;
        private final Map<String, Edit> editsByKey = new LinkedHashMap<>();
        final Set<String> rewrittenNames = new HashSet<>();
        final List<SkipEvent> skipEvents = new ArrayList<>();
        boolean wrappedMatchFunctionArg = false;
        final Set<String> coveredArguments = new HashSet<>();

        Walker(String query, ScopeResolver scopeResolver, String wrapperSubKey) {
            this.query = query;
            this.scopeResolver = scopeResolver;
            this.wrapperSubKey = wrapperSubKey;
            this.lineStarts = computeLineStarts(query);
        }

        private static int[] computeLineStarts(String query) {
            List<Integer> starts = new ArrayList<>();
            starts.add(0);
            for (int i = 0; i < query.length(); i++) {
                if (query.charAt(i) == '\n') {
                    starts.add(i + 1);
                }
            }
            int[] result = new int[starts.size()];
            for (int i = 0; i < starts.size(); i++) {
                result[i] = starts.get(i);
            }
            return result;
        }

        /** Absolute start offset of a {@link Source}, or {@code -1} if the source has no position. */
        private int startOffset(Source source) {
            int line = source.lineNumber();
            int col = source.columnNumber();
            if (line <= 0 || col <= 0 || line > lineStarts.length) {
                return -1;
            }
            return lineStarts[line - 1] + (col - 1);
        }

        /** Whether the original query substring at this source's offset matches the source text verbatim. */
        private boolean spanMatches(Source source) {
            int start = startOffset(source);
            if (start < 0) {
                return false;
            }
            String text = source.text();
            return start + text.length() <= query.length() && query.regionMatches(start, text, 0, text.length());
        }

        private void addEdit(int start, int end, String replacement) {
            Edit edit = new Edit(start, end, replacement);
            // De-duplicate identical edits so a pipeline shared across FORK branches (each branch
            // re-walks the same input plan) contributes each edit only once.
            editsByKey.putIfAbsent(start + ":" + end + ":" + replacement, edit);
        }

        /** Applies all collected edits to the original query, right-to-left, skipping any overlap. */
        String applyEdits() {
            List<Edit> edits = new ArrayList<>(editsByKey.values());
            edits.sort(Comparator.comparingInt(Edit::start).reversed().thenComparing(Comparator.comparingInt(Edit::end).reversed()));
            StringBuilder sb = new StringBuilder(query);
            int prevStart = query.length();
            for (Edit edit : edits) {
                if (edit.end() > prevStart) {
                    // Overlaps an already-applied (further-right) edit; skip defensively.
                    continue;
                }
                sb.replace(edit.start(), edit.end(), edit.replacement());
                prevStart = edit.start();
            }
            return sb.toString();
        }

        /**
         * Walks {@code plan} in pipeline order starting from {@code incomingScope} and returns the
         * scope at end-of-pipeline. Edits are accumulated in {@link #editsByKey}.
         */
        Set<String> processPipeline(LogicalPlan plan, Set<String> incomingScope) {
            List<LogicalPlan> chain = linearize(plan);
            Set<String> scope = new HashSet<>(incomingScope);
            for (LogicalPlan node : chain) {
                scope = processNode(node, scope);
            }
            return scope;
        }

        /** The downstream input of a command node, or {@code null} at a leaf/fork/union/subquery source. */
        private static LogicalPlan pipelineChild(LogicalPlan plan) {
            if (plan instanceof InlineStats is && is.child() instanceof Aggregate agg) {
                // Skip the wrapped Aggregate; the InlineStats node carries the STATS body.
                return agg.child();
            }
            if (plan instanceof Join join) {
                return join.left();
            }
            if (plan instanceof UnaryPlan unary) {
                return unary.child();
            }
            return null;
        }

        /** Returns the unary command chain of {@code plan} in pipeline (source-first) order. */
        private static List<LogicalPlan> linearize(LogicalPlan plan) {
            List<LogicalPlan> chain = new ArrayList<>();
            LogicalPlan cur = plan;
            while (cur != null) {
                chain.add(cur);
                cur = pipelineChild(cur);
            }
            Collections.reverse(chain);
            return chain;
        }

        private Set<String> processNode(LogicalPlan node, Set<String> scope) {
            if (node instanceof Eval eval) {
                for (Alias field : eval.fields()) {
                    wrapExpression(field, scope, null);
                }
                return removeAll(scope, aliasNames(eval.fields()));
            }
            if (node instanceof Filter filter) {
                return processFilter(filter, scope);
            }
            if (node instanceof OrderBy orderBy) {
                for (Expression order : orderBy.order()) {
                    wrapExpression(order, scope, null);
                }
                return scope;
            }
            if (node instanceof Keep keep) {
                return narrowScopeForKeep(scope, keep.projections());
            }
            if (node instanceof Drop drop) {
                return dropScope(scope, drop.removals());
            }
            if (node instanceof Rename rename) {
                return renameScope(scope, rename.renamings());
            }
            if (node instanceof MvExpand mvExpand) {
                return processMvExpand(mvExpand, scope);
            }
            if (node instanceof Enrich enrich) {
                recordSkips(enrich.expressions(), scope, SkipSite.ENRICH_BODY);
                return removeAll(scope, namedExpressionNames(enrich.enrichFields()));
            }
            if (node instanceof InlineStats inlineStats && inlineStats.child() instanceof Aggregate agg) {
                return processAggregate(agg, scope, true);
            }
            if (node instanceof Aggregate aggregate) {
                return processAggregate(aggregate, scope, false);
            }
            if (node instanceof RegexExtract regexExtract) {
                wrapExpression(regexExtract.input(), scope, null);
                return removeAll(scope, attributeNames(regexExtract.extractedFields()));
            }
            if (node instanceof Join join) {
                recordSkips(join.expressions(), scope, SkipSite.LOOKUP_JOIN_ON);
                return scope;
            }
            if (node instanceof UnionAll unionAll) {
                // UnionAll (a Fork subclass) backs `FROM a, (subquery), ...`; it must be matched
                // before Fork so the union-of-independent-sources semantics apply rather than the
                // shared-input FORK ones.
                return processUnionAll(unionAll, scope);
            }
            if (node instanceof Fork fork) {
                return processFork(fork, scope);
            }
            // Source commands (FROM/TS/ROW), subquery sources and any other command: wrap whatever
            // own expressions the node exposes (a no-op for leaf sources) and preserve scope.
            for (Expression expression : node.expressions()) {
                wrapExpression(expression, scope, null);
            }
            return scope;
        }

        /**
         * Processes a {@code WHERE} command. The body is wrapped normally, except for any field used
         * as the left-hand side of an {@code IN (subquery)}: the {@code InSubquery} resolver accepts
         * only a bare attribute (or a constant) on that side, so a {@code field_extract(...)} call
         * there is rejected as a "Complicated IN subquery". Such fields are therefore hoisted into an
         * {@code EVAL <field> = field_extract(<field>, "v")} inserted immediately before this
         * {@code WHERE}, which rebinds them to {@code keyword} so the in-place reference stays a bare
         * attribute. Those fields are removed from the scope used to wrap the rest of the condition
         * (so they are not also wrapped in place) and from the returned downstream scope.
         */
        private Set<String> processFilter(Filter filter, Set<String> scope) {
            Set<String> hoist = collectInSubqueryLhsHoist(filter.condition(), scope);
            Set<String> wrapScope = removeAll(scope, hoist);
            wrapExpression(filter.condition(), wrapScope, null);
            hoistBeforeCommand(filter.source(), hoist);
            return wrapScope;
        }

        /**
         * Processes an {@code MV_EXPAND <field>}. The command accepts only a bare attribute, and a
         * {@code flattened} field reaches it as a single wrapper object so nothing expands. When the
         * expanded target is in scope it is hoisted into an {@code EVAL <field> = field_extract(<field>,
         * "v")} inserted before the command, rebinding it to the multi-value {@code keyword} that
         * {@code MV_EXPAND} can split into rows; the field then leaves scope (it is {@code keyword}
         * downstream). A not-in-scope target is left untouched.
         */
        private Set<String> processMvExpand(MvExpand mvExpand, Set<String> scope) {
            if (mvExpand.target() instanceof UnresolvedAttribute attr && scope.contains(attr.name())) {
                Set<String> hoist = Set.of(attr.name());
                hoistBeforeCommand(mvExpand.source(), hoist);
                return removeAll(scope, hoist);
            }
            return scope;
        }

        /**
         * Returns the names of the bare, in-scope attributes that appear as the left-hand side of an
         * {@code IN (subquery)} anywhere in {@code condition}. Only bare attributes qualify: a
         * non-attribute LHS is already rejected by the engine independently of the keyword variant,
         * so it is left untouched.
         */
        private Set<String> collectInSubqueryLhsHoist(Expression condition, Set<String> scope) {
            Set<String> hoist = new HashSet<>();
            condition.forEachDown(InSubquery.class, in -> {
                if (in.value() instanceof UnresolvedAttribute attr && scope.contains(attr.name())) {
                    hoist.add(attr.name());
                }
            });
            return hoist;
        }

        /**
         * Inserts {@code EVAL <field> = field_extract(<field>, "v"), ...\n| } immediately before the
         * command whose {@link Source} is {@code commandSource} (a command source begins at its
         * keyword), rebinding each field to {@code keyword}. Used to satisfy grammar slots that accept
         * only a bare attribute &mdash; the {@code WHERE} carrying an IN-subquery left-hand side and
         * {@code MV_EXPAND}. No-op when {@code fields} is empty or the command source cannot be located.
         */
        private void hoistBeforeCommand(Source commandSource, Set<String> fields) {
            if (fields.isEmpty() || spanMatches(commandSource) == false) {
                return;
            }
            List<String> ordered = new ArrayList<>(fields);
            ordered.sort(Comparator.naturalOrder());
            StringBuilder sb = new StringBuilder("EVAL ");
            for (int i = 0; i < ordered.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                String name = ordered.get(i);
                sb.append(name).append(" = ").append(extractCall(name, wrapperSubKey));
                rewrittenNames.add(name);
            }
            sb.append("\n| ");
            int at = startOffset(commandSource);
            addEdit(at, at, sb.toString());
        }

        /**
         * Processes a {@code STATS}/{@code INLINE STATS} body: wraps aggregate and grouping
         * expressions, aliases bare/in-scope grouping keys through {@link #BY_ALIAS_PREFIX}, and
         * inserts the follow-up {@code RENAME} (and {@code DROP} for {@code INLINE STATS}) after the
         * segment. Returns the empty scope for plain {@code STATS} (row replaced) or the scope minus
         * rebound names for {@code INLINE STATS} (row preserved).
         */
        private Set<String> processAggregate(Aggregate aggregate, Set<String> scope, boolean preserveInput) {
            List<String> aliasedBy = new ArrayList<>();
            Set<String> shadowed = new HashSet<>();
            for (Expression grouping : aggregate.groupings()) {
                classifyStatsPiece(grouping, scope, true, aliasedBy, shadowed);
            }
            // The parser appends one attribute reference per grouping key to the end of the
            // aggregates list; only the leading user-written aggregates need processing.
            List<? extends NamedExpression> aggregates = aggregate.aggregates();
            int userAggCount = aggregates.size() - aggregate.groupings().size();
            for (int i = 0; i < userAggCount; i++) {
                classifyStatsPiece(aggregates.get(i), scope, false, aliasedBy, shadowed);
            }
            insertStatsRenameBack(aggregate, aliasedBy, preserveInput);
            if (preserveInput) {
                return removeAll(scope, shadowed);
            }
            return new HashSet<>();
        }

        /**
         * Classifies one grouping or aggregate piece of a STATS body and applies the matching
         * rewrite. Bare in-scope grouping keys are replaced by {@code __kw_<name> = field_extract(...)}
         * (recorded for a follow-up rename); explicit aliases whose name is in scope get the
         * {@link #BY_ALIAS_PREFIX} prefix (BY pieces) or are recorded as rebound (aggregate pieces);
         * unaliased expressions that mention an in-scope field are wrapped in an auto-name-preserving
         * backtick alias; everything else just has its in-scope references wrapped.
         */
        private void classifyStatsPiece(Expression piece, Set<String> scope, boolean isBy, List<String> aliasedBy, Set<String> shadowed) {
            if (isBy && piece instanceof UnresolvedAttribute attr && scope.contains(attr.name()) && spanMatches(attr.source())) {
                int start = startOffset(attr.source());
                String text = attr.source().text();
                addEdit(start, start + text.length(), BY_ALIAS_PREFIX + attr.name() + " = " + extractCall(text, wrapperSubKey));
                aliasedBy.add(attr.name());
                shadowed.add(attr.name());
                rewrittenNames.add(attr.name());
                return;
            }
            if (piece instanceof Alias alias) {
                if (isImplicitlyNamed(alias) && mentionsInScope(alias.child(), scope)) {
                    // Auto-named aggregate or grouping expression (e.g. {@code SUM(LENGTH(last_name))}
                    // or {@code BY LEFT(last_name, 1)}): the column name is derived from the source text,
                    // so wrapping the in-scope field inside would rename the column (and break any
                    // downstream reference to the original name). Pin the original name with an explicit
                    // backtick alias before wrapping.
                    int start = startOffset(alias.source());
                    addEdit(start, start, "`" + alias.source().text() + "` = ");
                    wrapExpression(alias.child(), scope, null);
                    return;
                }
                if (scope.contains(alias.name())) {
                    if (isBy && spanMatches(alias.source())) {
                        int start = startOffset(alias.source());
                        addEdit(start, start, BY_ALIAS_PREFIX);
                        aliasedBy.add(alias.name());
                    }
                    shadowed.add(alias.name());
                }
                wrapExpression(alias.child(), scope, null);
                return;
            }
            if (mentionsInScope(piece, scope) && spanMatches(piece.source())) {
                int start = startOffset(piece.source());
                addEdit(start, start, "`" + piece.source().text() + "` = ");
                wrapExpression(piece, scope, null);
            }
        }

        /**
         * True when {@code alias} was auto-named by the parser from its source text (e.g.
         * {@code SUM(x)} or {@code BY LEFT(x, 1)}) rather than written explicitly as {@code name = expr}.
         * The parser sets an implicit alias's name to the verbatim field text (see
         * {@code ExpressionBuilder#visitField}), so the name equals the source span exactly.
         */
        private boolean isImplicitlyNamed(Alias alias) {
            return spanMatches(alias.source()) && alias.name().equals(alias.source().text());
        }

        /** Inserts the {@code RENAME}/{@code DROP} that restores aliased grouping-key names after a STATS segment. */
        private void insertStatsRenameBack(Aggregate aggregate, List<String> aliasedBy, boolean preserveInput) {
            if (aliasedBy.isEmpty() || spanMatches(aggregate.source()) == false) {
                return;
            }
            List<String> ordered = new TreeSet<>(aliasedBy).stream().toList();
            StringBuilder sb = new StringBuilder();
            if (preserveInput) {
                sb.append("\n| DROP ");
                for (int i = 0; i < ordered.size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(ordered.get(i));
                }
            }
            sb.append("\n| RENAME ");
            for (int i = 0; i < ordered.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(BY_ALIAS_PREFIX).append(ordered.get(i)).append(" AS ").append(ordered.get(i));
            }
            int at = startOffset(aggregate.source()) + aggregate.source().text().length();
            addEdit(at, at, sb.toString());
        }

        /**
         * Processes a {@code UnionAll} &mdash; the node that backs {@code FROM a, (subquery), ...} (a
         * subquery in the {@code FROM} command). Unlike {@code FORK}, the branches are independent
         * source pipelines with their own schemas, so each is walked independently for its in-branch
         * edits (a subquery's {@code WHERE keyword == "x"} is still wrapped) and the post-union scope
         * is the union of the branch-end scopes &mdash; a field is flattened in the union output when it
         * is flattened in any branch that produces it.
         * <p>
         * No cross-branch recovery is emitted: the initial scope already excludes any field that is
         * non-keyword in <em>any</em> referenced dataset (the cross-dataset intersection in the test's
         * scope resolver), so a field that remains in scope is keyword in every dataset that has it and
         * therefore flattens to the same type in every branch. A column present in only some branches is
         * null-filled in the others, which {@code UnionAll}'s type check treats as compatible, so the
         * union never sees the flattened-vs-keyword conflict that {@code FORK}'s shared input can create.
         */
        private Set<String> processUnionAll(UnionAll unionAll, Set<String> scope) {
            Set<String> postUnion = new HashSet<>();
            for (LogicalPlan branch : unionAll.children()) {
                postUnion.addAll(processPipeline(branch, scope));
            }
            return postUnion;
        }

        /**
         * Processes a {@code FORK}: each branch is a full pipeline (the shared pre-fork input plus
         * branch-specific commands), so it is walked independently from the same {@code scope}; edits
         * for the shared input are de-duplicated by span.
         * <p>
         * A field flattened at the end of <em>every</em> branch stays flattened with a consistent type,
         * so it survives into the post-fork scope (the intersection). A field flattened in some branches
         * but rebound to {@code keyword} in others would make FORK reject the column as having
         * conflicting branch types, so for those conflicting fields (union minus intersection) the
         * branches where the field is still flattened recover it to {@code keyword} &mdash; matching the
         * branches that already produce {@code keyword}.
         */
        private Set<String> processFork(Fork fork, Set<String> scope) {
            List<LogicalPlan> branches = fork.children();
            List<Set<String>> branchEnds = new ArrayList<>(branches.size());
            for (LogicalPlan branch : branches) {
                branchEnds.add(processPipeline(branch, scope));
            }
            if (branchEnds.isEmpty()) {
                return scope;
            }
            Set<String> intersection = new HashSet<>(branchEnds.get(0));
            Set<String> union = new HashSet<>();
            for (Set<String> branchEnd : branchEnds) {
                intersection.retainAll(branchEnd);
                union.addAll(branchEnd);
            }
            Set<String> conflicting = new HashSet<>(union);
            conflicting.removeAll(intersection);
            if (conflicting.isEmpty() == false) {
                for (int i = 0; i < branches.size(); i++) {
                    Set<String> recover = new HashSet<>(branchEnds.get(i));
                    recover.retainAll(conflicting);
                    appendForkBranchTailRecovery(branches.get(i), recover);
                }
            }
            return intersection;
        }

        /**
         * Appends {@code | EVAL <field> = field_extract(<field>, "v"), ...} to the end of a FORK
         * branch (just before its closing parenthesis) for every field still flattened at the
         * branch's end, so the branch emits {@code keyword} for them. The branch plan's outermost
         * node is the synthetic {@code _fork} {@link Eval} the parser appends; its source spans the
         * whole {@code FORK}, so the insertion offset is taken from the branch's last real command.
         */
        private void appendForkBranchTailRecovery(LogicalPlan branch, Set<String> branchEnd) {
            if (branchEnd.isEmpty()) {
                return;
            }
            LogicalPlan lastCommand = forkBranchLastCommand(branch);
            if (spanMatches(lastCommand.source()) == false) {
                return;
            }
            List<String> recoverable = new ArrayList<>(branchEnd);
            recoverable.sort(Comparator.naturalOrder());
            int at = startOffset(lastCommand.source()) + lastCommand.source().text().length();
            addEdit(at, at, evalRecovery(recoverable, wrapperSubKey));
            rewrittenNames.addAll(recoverable);
        }

        /**
         * Returns the last real command of a FORK branch, unwrapping the synthetic {@code _fork}
         * {@link Eval} the parser wraps around every branch (its source spans the whole {@code FORK}
         * rather than the branch body).
         */
        private static LogicalPlan forkBranchLastCommand(LogicalPlan branch) {
            if (branch instanceof Eval eval && eval.fields().size() == 1 && Fork.FORK_FIELD.equals(eval.fields().get(0).name())) {
                return eval.child();
            }
            return branch;
        }

        /**
         * Recursively wraps in-scope references inside {@code expression}. Stops at attribute leaves
         * (wrapping in-scope ones), protects the LHS of the match operator {@code :}, and recurses
         * into {@code IN (subquery)} by wrapping the left-hand side and rewriting the subquery.
         *
         * @param trackingContext the tracking context for function arguments, or {@code null}
         *                        when the expression is not directly inside a function call
         */
        private void wrapExpression(Expression expression, Set<String> scope, String trackingContext) {
            if (expression instanceof UnresolvedAttribute attr) {
                if (trackingContext != null && scope.contains(attr.name()) && spanMatches(attr.source())) {
                    coveredArguments.add(trackingContext);
                }
                wrapAttribute(attr, scope);
                return;
            }
            if (expression instanceof InSubquery inSubquery) {
                // The left-hand side is left to processFilter/insertPreFilterEval: a bare in-scope
                // attribute there is hoisted into a preceding EVAL (the IN-subquery resolver rejects
                // a field_extract(...) LHS), and is excluded from this scope so it is not wrapped in
                // place. A non-attribute LHS (constant/expression) is wrapped here as usual.
                wrapExpression(inSubquery.value(), scope, null);
                processInSubquery(inSubquery);
                return;
            }
            if (expression instanceof MatchOperator matchOperator) {
                // The match operator's left-hand side accepts only a bare attribute; record any
                // in-scope reference there as a skip and leave the whole operator untouched (its
                // right-hand side is a constant in every corpus case).
                recordSkips(List.of(matchOperator), scope, SkipSite.MATCH_OPERATOR_LHS);
                return;
            }

            String expressionName = getExpressionName(expression);
            if ("FIELD_EXTRACT".equals(expressionName)) {
                expressionName = null;
            }

            int i = 0;
            for (Expression child : expression.children()) {
                // A MATCH(field, "...") whose in-scope keyword field argument is wrapped in
                // field_extract(...) is pushed to Lucene as a synthetic field attribute, which only
                // the runtime lexical search path can score; record it so the caller enables that pragma.
                if (expression instanceof UnresolvedFunction uf
                    && "match".equalsIgnoreCase(uf.name())
                    && child instanceof UnresolvedAttribute attr
                    && scope.contains(attr.name())
                    && spanMatches(attr.source())) {
                    wrappedMatchFunctionArg = true;
                }
                // Heuristic to map variadic args to the last named param, or just skip if we don't care
                // but IN and CONCAT can just cap at 1.
                // TODO use a `kind` marker for varargs somehow.
                int argIndex = i;
                if (expressionName != null) {
                    if (expressionName.equals("CONCAT") && argIndex > 1) {
                        argIndex = 1;
                    }
                    if (expressionName.equals("IN") && argIndex > 1) {
                        argIndex = 1;
                    }
                }
                String nextTrackingContext = expressionName != null ? expressionName + ":" + argIndex : trackingContext;
                wrapExpression(child, scope, nextTrackingContext);
                i++;
            }
        }

        private void wrapAttribute(UnresolvedAttribute attr, Set<String> scope) {
            if (scope.contains(attr.name()) == false || spanMatches(attr.source()) == false) {
                return;
            }
            int start = startOffset(attr.source());
            String text = attr.source().text();
            addEdit(start, start + text.length(), extractCall(text, wrapperSubKey));
            rewrittenNames.add(attr.name());
        }

        /**
         * Rewrites the subquery of an {@code IN (subquery)} expression with a scope freshly resolved
         * from the subquery's own text (its {@code FROM} may reference a different dataset than the
         * outer query) and appends a tail-end {@code EVAL} (no {@code KEEP}) so its projected column
         * reaches the outer comparison as {@code keyword}. The outer left-hand side is rebound to
         * {@code keyword} separately by {@link #hoistBeforeCommand}, so both sides agree on type.
         */
        private void processInSubquery(InSubquery inSubquery) {
            LogicalPlan subquery = inSubquery.subquery();
            String subText = subqueryText(subquery);
            if (subText == null) {
                return;
            }
            Set<String> subScope = scopeResolver.resolveScope(subText);
            if (subScope.isEmpty()) {
                return;
            }
            Set<String> endScope = processPipeline(subquery, subScope);
            if (endScope.isEmpty() || spanMatches(subquery.source()) == false) {
                return;
            }
            List<String> recoverable = new ArrayList<>(endScope);
            recoverable.sort(Comparator.naturalOrder());
            int at = startOffset(subquery.source()) + subquery.source().text().length();
            addEdit(at, at, evalRecovery(recoverable, wrapperSubKey));
            rewrittenNames.addAll(recoverable);
        }

        /**
         * Returns the verbatim text of a subquery plan: from the leftmost source command (its
         * {@code FROM}/{@code ROW}/{@code TS}) through the end of the outermost processing command,
         * or {@code null} if either span cannot be located in the original query. The subquery plan
         * carries the source of only its last command, so the start is taken from the leftmost leaf.
         */
        private String subqueryText(LogicalPlan subquery) {
            LogicalPlan leaf = subquery;
            while (leaf.children().isEmpty() == false) {
                leaf = leaf.children().get(0);
            }
            if (spanMatches(leaf.source()) == false || spanMatches(subquery.source()) == false) {
                return null;
            }
            int start = startOffset(leaf.source());
            int end = startOffset(subquery.source()) + subquery.source().text().length();
            if (start < 0 || start >= end || end > query.length()) {
                return null;
            }
            return query.substring(start, end);
        }

        /** Records a skip event for every in-scope attribute appearing anywhere in the given expressions. */
        private void recordSkips(List<? extends Expression> expressions, Set<String> scope, SkipSite site) {
            for (Expression expression : expressions) {
                expression.forEachDown(UnresolvedAttribute.class, attr -> {
                    if (scope.contains(attr.name())) {
                        skipEvents.add(new SkipEvent(site, attr.name()));
                    }
                });
            }
        }

        /** True when any in-scope attribute appears in {@code expression}. */
        private static boolean mentionsInScope(Expression expression, Set<String> scope) {
            boolean[] found = { false };
            expression.forEachDown(UnresolvedAttribute.class, attr -> {
                if (scope.contains(attr.name())) {
                    found[0] = true;
                }
            });
            return found[0];
        }

        private static Set<String> narrowScopeForKeep(Set<String> scope, List<? extends NamedExpression> projections) {
            Set<String> kept = new HashSet<>();
            for (NamedExpression projection : projections) {
                if (projection instanceof UnresolvedAttribute attr) {
                    kept.add(attr.name());
                } else {
                    // A wildcard or pattern projection: over-approximate by keeping the whole scope.
                    return scope;
                }
            }
            Set<String> next = new HashSet<>(scope);
            next.retainAll(kept);
            return next;
        }

        private static Set<String> dropScope(Set<String> scope, List<? extends NamedExpression> removals) {
            Set<String> next = new HashSet<>(scope);
            for (NamedExpression removal : removals) {
                if (removal instanceof UnresolvedAttribute attr) {
                    next.remove(attr.name());
                }
                // Pattern removals are left as an over-approximation (nothing removed).
            }
            return next;
        }

        private static Set<String> renameScope(Set<String> scope, List<Alias> renamings) {
            Set<String> next = new HashSet<>(scope);
            for (Alias renaming : renamings) {
                String newName = renaming.name();
                String oldName = renaming.child() instanceof UnresolvedAttribute attr ? attr.name() : null;
                next.remove(newName);
                if (oldName != null) {
                    boolean oldWasFlattened = next.remove(oldName);
                    if (oldWasFlattened) {
                        next.add(newName);
                    }
                }
            }
            return next;
        }

        private static Set<String> removeAll(Set<String> scope, Set<String> names) {
            if (names.isEmpty()) {
                return scope;
            }
            Set<String> next = new HashSet<>(scope);
            next.removeAll(names);
            return next;
        }

        private static Set<String> aliasNames(List<Alias> aliases) {
            Set<String> names = new HashSet<>();
            for (Alias alias : aliases) {
                names.add(alias.name());
            }
            return names;
        }

        private static Set<String> namedExpressionNames(List<? extends NamedExpression> expressions) {
            Set<String> names = new HashSet<>();
            for (NamedExpression expression : expressions) {
                names.add(expression.name());
            }
            return names;
        }

        private static Set<String> attributeNames(List<? extends NamedExpression> attributes) {
            return namedExpressionNames(attributes);
        }
    }
}
