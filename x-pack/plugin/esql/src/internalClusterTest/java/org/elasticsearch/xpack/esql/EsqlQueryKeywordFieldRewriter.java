/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rewrites a csv-spec ES|QL query so that every reference to a keyword field that has been
 * converted to {@code flattened} is wrapped in a call to
 * {@code field_extract(<field>, "<wrapperSubKey>")}.
 * <p>
 * The companion {@link KeywordToFlattenedTransformer} rewrites the mapping and document source so
 * the field is stored as a {@code flattened} object {@code {"v": <original keyword value>}}; this
 * rewriter inserts the matching {@code field_extract(field, "v")} on the query side wherever the
 * field is referenced as an expression.
 *
 * <h2>Pipeline-aware rewrite</h2>
 * The query is split into top-level pipeline commands and walked in order. A running scope
 * tracks which converted-keyword fields are still bound to a {@code flattened} value at the
 * current point in the pipeline. Each command updates that scope:
 * <ul>
 *   <li>Source commands ({@code FROM}, {@code TS}, {@code ROW}, {@code SHOW}) leave their own
 *       body untouched but recursively rewrite every parenthesised sub-pipeline whose body starts
 *       with {@code FROM} or {@code TS} (the csv-spec subquery form
 *       {@code FROM a, (FROM b | ... | LIMIT N), ...}). Each sub-pipeline is rewritten with a
 *       freshly-resolved scope &mdash; the rewriter invokes the caller-supplied
 *       {@link ScopeResolver} again on the inner query text so the scope contains only the
 *       keyword paths from the datasets the subquery's own FROM/TS touches, not the union scope
 *       of the outer FROM. Inline {@code field_extract} wrapping is still applied inside the
 *       subquery (for instance to a converted-keyword field referenced in a {@code WHERE} or
 *       {@code EVAL}), but tail-end recovery is deliberately disabled there so the subquery's
 *       output schema arrives at the outer FROM with the same {@code flattened} type as the
 *       sibling-index branches; the outer pipeline's tail-end recovery then runs once over the
 *       merged schema. The initial scope at the top level is supplied by the caller, typically
 *       the per-query keyword set resolved by {@link EsqlQueryDatasetResolver}.</li>
 *   <li>Name-only commands ({@code KEEP}, {@code DROP}, {@code RENAME}) are left untouched in the
 *       text, but their effect on the schema is tracked: {@code KEEP} narrows the scope to the
 *       kept names, {@code DROP} removes the dropped names, and {@code RENAME old AS new} swaps
 *       {@code old} for {@code new} in the scope. Wildcard forms ({@code KEEP *_name}) are
 *       conservatively left to over-approximate the scope.</li>
 *   <li>{@code EVAL} rewrites expressions on the right-hand side of each assignment and then
 *       removes the LHS names from the scope &mdash; after the assignment the column is bound to
 *       the EVAL expression, not to the original {@code flattened} field, so subsequent commands
 *       must not re-wrap it.</li>
 *   <li>{@code STATS} rewrites both aggregate and {@code BY} expressions. Bare-name groupings
 *       that are in scope are first expanded to {@code name = name} so the subsequent regex wrap
 *       produces {@code name = field_extract(name, "v")} &mdash; this keeps the output column
 *       named {@code name} (of type keyword) rather than the auto-derived
 *       {@code field_extract(name, "v")}, so downstream references resolve cleanly. The scope is
 *       reset to empty after {@code STATS}: every column produced by it is either an aggregate of
 *       unknown type or an extracted keyword, never the original flattened field.</li>
 *   <li>{@code INLINE STATS} (alias {@code INLINESTATS}) is rewritten exactly like {@code STATS}
 *       but its scope effect is the opposite: {@code INLINE STATS} <em>adds</em> aggregate
 *       columns alongside every existing input column (it does not replace them), so any field
 *       that was in the flattened scope before the command is still in scope after it. Without
 *       this distinction, tail-end recovery would skip the still-flattened input columns and the
 *       test would fail with a column-type mismatch.</li>
 *   <li>{@code DISSECT} / {@code GROK} rewrites the command body (the source field reference and
 *       the literal pattern; the pattern itself is a string literal and so is masked from
 *       wrapping) and then removes every column the pattern binds from the scope. After the
 *       command those columns hold the extracted keyword tokens, not the original flattened
 *       value, so subsequent commands and the tail-end recovery must not re-wrap them. The names
 *       bound by a pattern are recognised as {@code %{[modifier]name}} for {@code DISSECT}, and
 *       as both {@code (?<name>...)} and {@code %{PATTERN:name[:type]}} for {@code GROK}.</li>
 *   <li>{@code MV_EXPAND} and {@code ENRICH} have name-only bodies (a single field reference for
 *       MV_EXPAND, and {@code <policy> [ON <field>] [WITH <new>=<src>, ...]} for ENRICH). Wrapping
 *       any of those names in {@code field_extract(...)} produces a parser error because those
 *       grammar slots accept only attribute references, not arbitrary expressions. The bodies are
 *       therefore left untouched and the scope is preserved across the command.</li>
 *   <li>Every other command (WHERE, SORT, LIMIT, LOOKUP JOIN, FORK, &hellip;) rewrites its
 *       expression body in scope and leaves the scope unchanged.</li>
 * </ul>
 *
 * <h2>Match-operator (<code>:</code>) LHS protection</h2>
 * The match operator <code>:</code> (used as <code>field : "value"</code> inside a {@code WHERE}
 * clause) accepts only a directly-mapped attribute on its left-hand side; an expression like
 * {@code field_extract(field, "v") : "value"} is rejected by the parser. Inside any expression
 * body, the rewriter therefore protects the identifier (and any subsequent {@code ::cast}
 * suffix) immediately preceding a single {@code :} that is not part of the {@code ::} cast
 * operator. Identifiers appearing elsewhere in the same expression are still wrapped normally.
 * <p>
 * The protection is applied at the field-reference rewrite level rather than at the segment
 * level because the {@code :} operator can appear anywhere a comparison is allowed, including
 * inside conjunctions, disjunctions, and nested function arguments. Per-segment skipping would
 * have to enumerate every command that admits a boolean expression body; per-reference protection
 * captures all of them with a single check.
 *
 * <h2>Tail-end recovery</h2>
 * Per-segment wrapping handles every expression-context reference, but the final output of the
 * pipeline still reaches the test runner typed as {@code flattened} whenever a converted-keyword
 * field is projected directly (e.g. through {@code KEEP}, {@code RENAME}, {@code SORT}, or simply
 * by surviving to the end of a pipeline with no terminal projection). The csv-spec corpus
 * declares those columns as {@code keyword}, so the test would fail with a column-type mismatch.
 * <p>
 * To restore {@code keyword} semantics at the projection boundary <em>without</em> sacrificing any
 * earlier in-line wrapping, the walker appends one final segment of the form
 * {@code | EVAL name = field_extract(name, "v"), ...} for every field still in the flattened
 * scope at end-of-pipeline.
 * <p>
 * ES|QL's {@code EVAL name = expr} semantics shadow an existing column by removing it from its
 * current position and appending a new column of the same name at the end of the schema; the
 * test asserts on column order, so a bare tail-end EVAL would break any test in which the
 * affected field is not already at the end. To preserve the expected column order the rewriter
 * also accepts the expected output column names (in their expected order) and, when that list is
 * supplied, appends {@code | KEEP <col1>, <col2>, ...} immediately after the tail-end EVAL. The
 * KEEP places every column back where the test expects it, and the EVAL's type promotion of the
 * affected columns is the only schema change relative to the unmodified query.
 * <p>
 * Importantly this is a strict <em>increase</em> in {@code field_extract} coverage: every wrap
 * the per-segment walker would otherwise emit is still emitted (the tail-end recovery does not
 * touch any earlier segment), and one additional wrap is emitted per surviving flattened field,
 * which itself sits in a new expression context (an {@code EVAL} after a projection /
 * {@code SORT} / {@code LIMIT} / {@code ENRICH} / etc.). Nothing is injected when the scope is
 * empty &mdash; that is, when every flattened field has been dropped, renamed, or rebound by an
 * earlier {@code EVAL} or {@code STATS} command. When no expected column order is supplied the
 * EVAL is still appended but the column-reordering KEEP is skipped; callers that care about
 * column order should always pass the expected order.
 *
 * <h2>What is intentionally not rewritten</h2>
 * <ul>
 *   <li>Identifiers inside string literals ({@code "..."} and {@code """..."""}).</li>
 *   <li>Identifiers inside line comments ({@code // ...}).</li>
 *   <li>Bodies of name-only commands (KEEP / DROP / RENAME / MV_EXPAND / ENRICH) and source
 *       commands.</li>
 *   <li>The left-hand side of an assignment ({@code name = expr}, distinguished from the
 *       comparison operator {@code ==}).</li>
 *   <li>The left-hand side of a match-operator comparison ({@code name : "value"} or
 *       {@code name::cast : "value"}), regardless of which command's body it appears in.</li>
 *   <li>Names that have already been dropped, renamed, or rebound by an earlier command in the
 *       pipeline.</li>
 * </ul>
 *
 * <h2>Known limitations</h2>
 * The rewriter is regex-driven and does not parse the ES|QL grammar. In particular it does not
 * handle backtick-quoted identifiers. It also leaves intact every grammar slot that admits only
 * an attribute and not an arbitrary expression but is not in {@link #NAME_ONLY_BODY_COMMANDS} or
 * otherwise covered above &mdash; the canonical example being
 * {@code LOOKUP JOIN ... ON &lt;field&gt;}, where wrapping the join key would surface as a
 * parser error. Queries that hit those cases will surface as failures, which is the intended way
 * to inventory ES|QL surfaces that {@code field_extract} cannot currently substitute for.
 */
public final class EsqlQueryKeywordFieldRewriter {

    /** Name of the ES|QL function used to extract a sub-field from a {@code flattened} value. */
    public static final String FIELD_EXTRACT_FUNCTION = "field_extract";

    /**
     * Resolves the initial flattened-field scope for an arbitrary (sub-)query text. Implementations
     * typically inspect the source command(s) of {@code query} (via
     * {@link EsqlQueryDatasetResolver}) and return the keyword paths declared by the datasets the
     * query actually touches, intersected with whatever cross-dataset conflict policy the caller
     * applies.
     * <p>
     * The walker invokes this resolver once at the top level and again once for every recursive
     * sub-pipeline so that a parenthesised {@code (FROM b | ...)} inside an outer
     * {@code FROM a, ...} is rewritten with the keyword paths of {@code b} alone &mdash; not with
     * the outer union, which can otherwise produce tail-end {@code EVAL} statements that
     * reference columns that do not exist in {@code b}'s schema.
     */
    @FunctionalInterface
    public interface ScopeResolver {
        Set<String> resolveScope(String query);
    }

    /**
     * Identifies a syntactic site at which the rewriter declined to wrap an in-scope keyword
     * reference in {@code field_extract(...)}. Each variant corresponds to a grammar slot or
     * operator position whose ES|QL syntax accepts only an attribute reference, not an arbitrary
     * expression; wrapping there would surface as a parser error rather than as a useful
     * {@code field_extract} finding. Used by callers (typically the test variant's logger) to
     * inventory exactly which positions were left unwrapped and why, so that the missing
     * coverage is visible in run output instead of being silently absorbed.
     */
    public enum SkipSite {
        /** Body of an {@code MV_EXPAND <field>} command. */
        MV_EXPAND_ARG,
        /** Body of an {@code ENRICH <policy> [ON <field>] [WITH ...]} command (any sub-position). */
        ENRICH_BODY,
        /** Identifier on the left-hand side of a single-colon match operator ({@code field : "value"}). */
        MATCH_OPERATOR_LHS
    }

    /**
     * One occurrence of an in-scope keyword field that the rewriter intentionally did not wrap.
     *
     * @param site   the syntactic context that forced the skip; see {@link SkipSite}
     * @param field  the field name that was left unwrapped
     */
    public record SkipEvent(SkipSite site, String field) {}

    /**
     * Result of {@link #rewrite(String, Set, String)}.
     *
     * @param rewrittenQuery       the rewritten query string, or the original query if no field references were rewritten
     * @param modified             {@code true} iff at least one keyword field reference was rewritten
     * @param rewrittenFieldNames  the set of keyword field names that were actually wrapped at least once
     * @param skipEvents           every in-scope keyword reference that was left unwrapped, in
     *                             pipeline order, including events emitted from recursive
     *                             rewrites of parenthesised sub-pipelines
     */
    public record RewriteResult(String rewrittenQuery, boolean modified, Set<String> rewrittenFieldNames, List<SkipEvent> skipEvents) {}

    /** Character class used in lookarounds to detect identifier boundaries. */
    private static final String IDENTIFIER_BOUNDARY_CLASS = "[a-zA-Z0-9_.]";

    /** Plain identifier pattern (no leading dot). */
    private static final String IDENTIFIER_PATTERN = "[a-zA-Z_][a-zA-Z0-9_.]*";

    /**
     * Pattern matching the left-hand side of an assignment: an identifier (possibly dotted)
     * followed by optional whitespace, a single {@code =}, and a negative lookahead to exclude the
     * comparison operator {@code ==}.
     */
    private static final Pattern ASSIGNMENT_LHS = Pattern.compile("(?<![a-zA-Z0-9_.])(" + IDENTIFIER_PATTERN + ")\\s*=(?!=)");

    /** Pattern matching a {@code RENAME old AS new} pair (used after splitting on top-level commas). */
    private static final Pattern RENAME_PAIR = Pattern.compile(
        "^\\s*(" + IDENTIFIER_PATTERN + ")\\s+AS\\s+(" + IDENTIFIER_PATTERN + ")\\s*$",
        Pattern.CASE_INSENSITIVE
    );

    /** Pattern matching a bare identifier with optional surrounding whitespace. */
    private static final Pattern BARE_IDENTIFIER = Pattern.compile("^\\s*(" + IDENTIFIER_PATTERN + ")\\s*$");

    /**
     * Pattern matching the left-hand side of an ES|QL match-operator comparison: an identifier
     * (possibly dotted), optionally followed by a {@code ::type} cast suffix, then optional
     * whitespace, then a single {@code :} that is NOT part of the {@code ::} cast operator
     * (negative lookahead for a second colon).
     * <p>
     * Group 1 is the identifier itself; the cast suffix and trailing {@code :} are matched but
     * not captured, so the protected range is exactly the identifier even when a cast is present
     * (the cast keyword is not a field name and does not need protection).
     */
    private static final Pattern MATCH_OPERATOR_LHS = Pattern.compile(
        "(?<!" + IDENTIFIER_BOUNDARY_CLASS + ")(" + IDENTIFIER_PATTERN + ")(?:\\s*::\\s*[a-zA-Z_][a-zA-Z0-9_]*)?\\s*:(?!:)"
    );

    /** Pattern matching the standalone {@code BY} keyword. */
    private static final Pattern BY_KEYWORD = Pattern.compile("\\bBY\\b", Pattern.CASE_INSENSITIVE);

    /** Source commands; their segment text is preserved verbatim. */
    private static final Set<String> SOURCE_COMMANDS = Set.of("FROM", "TS", "ROW", "SHOW");

    /** Name-only commands; their bodies are preserved verbatim and their schema effect is tracked. */
    private static final Set<String> NAME_ONLY_COMMANDS = Set.of("KEEP", "DROP", "RENAME");

    /**
     * STATS-family commands that <em>replace</em> the row schema with their grouping and aggregate
     * outputs. Their body is rewritten and BY-clause bare names are aliased back; the scope is
     * cleared after the command because no original input column survives the row reduction.
     */
    private static final Set<String> STATS_REPLACE_COMMANDS = Set.of("STATS");

    /**
     * STATS-family commands that <em>extend</em> each input row with aggregate columns. Their body
     * is rewritten in the same way as {@link #STATS_REPLACE_COMMANDS}, but the scope is preserved
     * because every previously-bound input column still passes through the command.
     */
    private static final Set<String> STATS_PRESERVE_COMMANDS = Set.of("INLINE STATS", "INLINESTATS");

    /** Commands that rebind columns from a literal pattern: the bound names leave the scope. */
    private static final Set<String> DISSECT_GROK_COMMANDS = Set.of("DISSECT", "GROK");

    /**
     * Commands whose entire body is a list of bare field references (no expressions are accepted
     * by the grammar), so the body must be passed through verbatim. The scope is preserved across
     * these commands because they do not rebind or shadow any pre-existing column &mdash;
     * {@code MV_EXPAND} unflattens a multi-value column without changing its type, and
     * {@code ENRICH} appends new columns from the policy's enrich index without touching any
     * existing column.
     */
    private static final Set<String> NAME_ONLY_BODY_COMMANDS = Set.of("MV_EXPAND", "ENRICH");

    /** Names bound by a {@code DISSECT} pattern as {@code %{[modifier]name}}. */
    private static final Pattern DISSECT_NAME_BINDING = Pattern.compile("%\\{[?+&*-]?\\s*(" + IDENTIFIER_PATTERN + ")");

    /** Names bound by a {@code GROK} pattern as {@code %{PATTERN:name[:type]}}. */
    private static final Pattern GROK_PERCENT_NAME_BINDING = Pattern.compile("%\\{[A-Za-z_][A-Za-z0-9_]*:(" + IDENTIFIER_PATTERN + ")");

    /** Names bound by a {@code GROK} pattern as a Java named-capture group {@code (?<name>...)}. */
    private static final Pattern GROK_NAMED_CAPTURE_BINDING = Pattern.compile("\\(\\?<(" + IDENTIFIER_PATTERN + ")>");

    /** Two-word commands recognized when parsing a segment's leading keyword. */
    private static final List<String> TWO_WORD_COMMANDS = List.of("INLINE STATS", "LOOKUP JOIN");

    private EsqlQueryKeywordFieldRewriter() {}

    /**
     * Convenience overload that performs tail-end recovery without restoring a particular column
     * order. See {@link #rewrite(String, Set, String, List)} for the full contract.
     */
    public static RewriteResult rewrite(String query, Set<String> initialKeywordScope, String wrapperSubKey) {
        return rewrite(query, initialKeywordScope, wrapperSubKey, List.of());
    }

    /**
     * Convenience overload for callers that have already resolved a flat scope for the top-level
     * query and do not need per-subquery scope re-resolution. Every sub-pipeline encountered
     * during recursion will see the same {@code initialKeywordScope}; if the corpus contains
     * subqueries whose FROM list does not include some of those fields, the tail-end recovery
     * inside the subquery will emit references to columns the subquery does not produce. Prefer
     * {@link #rewrite(String, ScopeResolver, String, List)} whenever subqueries are possible.
     */
    public static RewriteResult rewrite(
        String query,
        Set<String> initialKeywordScope,
        String wrapperSubKey,
        List<String> expectedColumnOrder
    ) {
        return rewrite(query, q -> initialKeywordScope, wrapperSubKey, expectedColumnOrder);
    }

    /**
     * Rewrites {@code query} by walking its pipeline commands top to bottom and wrapping every
     * reference to a field in scope as {@code field_extract(field, "wrapperSubKey")}. The supplied
     * {@link ScopeResolver} computes the initial scope at every source-command boundary &mdash;
     * once for the top-level query and once again for each parenthesised sub-pipeline encountered
     * during recursion. Subsequent commands narrow that scope according to their schema effect.
     * <p>
     * Per-subquery scope re-resolution is what keeps the tail-end recovery inside a subquery from
     * referencing columns that do not belong to its inner FROM. For example, in
     * {@code FROM a, (FROM b | ...) | ...}, the outer rewrite sees the union of {@code a} and
     * {@code b}'s keyword paths, but the inner rewrite is invoked with {@code (FROM b | ...)} as
     * its query and must therefore see only {@code b}'s keyword paths so the trailing
     * {@code EVAL} it emits references only columns present in {@code b}.
     * <p>
     * After the per-segment walk, if any field is still in the flattened scope, the rewriter
     * appends a tail-end {@code | EVAL ...} that rebinds each one back to its keyword form (see
     * the class-level "Tail-end recovery" section). When {@code expectedColumnOrder} is non-empty,
     * the rewriter also appends a {@code | KEEP <expectedColumnOrder>} so that the affected
     * columns return to the positions the unmodified query would have placed them in.
     * {@code expectedColumnOrder} should list every output column of the unmodified query in the
     * exact order the test asserts on; pass an empty list when that order is unknown or
     * irrelevant. The trailing KEEP is only emitted at the top level &mdash; recursive subquery
     * rewrites pass an empty {@code expectedColumnOrder} because the outer FROM merges subquery
     * columns by name, not by position.
     * <p>
     * If the resolved initial scope is empty, the original query is returned and
     * {@link RewriteResult#modified()} is {@code false}.
     */
    public static RewriteResult rewrite(String query, ScopeResolver scopeResolver, String wrapperSubKey, List<String> expectedColumnOrder) {
        return rewriteInternal(query, scopeResolver, wrapperSubKey, expectedColumnOrder, true);
    }

    /**
     * Internal entry point used both by the public {@link #rewrite(String, ScopeResolver, String, List)}
     * API and by the subquery-recursive code path inside the {@link PipelineWalker}. The public API
     * always asks for tail-end recovery; recursive subquery rewrites disable it because emitting a
     * trailing {@code EVAL field = field_extract(...)} inside a subquery would coerce that field
     * from {@code flattened} to {@code keyword} only on the subquery's branch of the merge, while
     * the outer FROM's other (sibling-index) branches would still produce a {@code flattened}
     * value for the same field. The outer rewriter would then encounter the field as
     * {@code keyword} from one shard and {@code flattened} from another, and the per-source
     * tail-end EVAL it emits would fail with "first argument of [field_extract(...)] must be
     * [flattened], found value [...] type [keyword]". Letting the outer tail-end recovery run once
     * over the merged schema gives a single, consistent conversion point.
     */
    private static RewriteResult rewriteInternal(
        String query,
        ScopeResolver scopeResolver,
        String wrapperSubKey,
        List<String> expectedColumnOrder,
        boolean applyTailEndRecovery
    ) {
        Set<String> initialScope = scopeResolver.resolveScope(query);
        if (initialScope.isEmpty()) {
            return new RewriteResult(query, false, Set.of(), List.of());
        }
        return new PipelineWalker(query, initialScope, scopeResolver, wrapperSubKey, expectedColumnOrder, applyTailEndRecovery).walk();
    }

    /**
     * Stateful walker over the top-level pipeline commands of a query. Each {@link #processSegment}
     * call appends the (possibly rewritten) text of one command to {@link #out} and updates
     * {@link #scope} according to that command's schema effect.
     */
    private static final class PipelineWalker {
        private final String query;
        private final ScopeResolver scopeResolver;
        private final String wrapperSubKey;
        private final List<String> expectedColumnOrder;
        /**
         * Whether to append a tail-end {@code | EVAL ...} (and optional {@code | KEEP ...}) that
         * converts every surviving flattened field back to keyword at the end of the walked
         * pipeline. Top-level rewrites set this to {@code true}; recursive subquery rewrites set
         * it to {@code false} so the outer pipeline's tail-end recovery runs once over the merged
         * schema. See {@link EsqlQueryKeywordFieldRewriter#rewriteInternal} for the rationale.
         */
        private final boolean applyTailEndRecovery;
        private final StringBuilder out = new StringBuilder();
        private final Set<String> rewrittenNames = new HashSet<>();
        private final List<SkipEvent> skipEvents = new ArrayList<>();
        private Set<String> scope;

        PipelineWalker(
            String query,
            Set<String> initialScope,
            ScopeResolver scopeResolver,
            String wrapperSubKey,
            List<String> expectedColumnOrder,
            boolean applyTailEndRecovery
        ) {
            this.query = query;
            this.scopeResolver = scopeResolver;
            this.wrapperSubKey = wrapperSubKey;
            this.expectedColumnOrder = List.copyOf(expectedColumnOrder);
            this.applyTailEndRecovery = applyTailEndRecovery;
            this.scope = new HashSet<>(initialScope);
        }

        RewriteResult walk() {
            for (int[] range : splitIntoSegmentRanges(query)) {
                processSegment(query.substring(range[0], range[1]));
            }
            if (applyTailEndRecovery) {
                appendTailEndRecovery();
            }
            String result = out.toString();
            return new RewriteResult(result, result.equals(query) == false, Set.copyOf(rewrittenNames), List.copyOf(skipEvents));
        }

        /**
         * Appends a trailing {@code | EVAL ...} (and, when an expected column order is known, a
         * trailing {@code | KEEP ...} after it) that rebinds every field still in the flattened
         * scope back to its keyword value while keeping the test's expected column order intact.
         * See the class-level "Tail-end recovery" section for the rationale and ES|QL semantics
         * that motivate the trailing KEEP.
         * <p>
         * When {@link #expectedColumnOrder} is non-empty, the EVAL's field list is intersected
         * with the expected output columns. Without this intersection a wildcard {@code KEEP} or
         * {@code DROP} earlier in the pipeline (which the conservative scope tracker leaves
         * over-approximated) would cause the EVAL to reference fields that no longer exist in
         * the schema at end-of-pipeline, producing a spurious {@code Unknown column} verification
         * error. When the expected column order is empty the EVAL falls back to the full scope.
         * <p>
         * The EVAL's assignments are emitted in sorted order so that the rewritten query is
         * deterministic across runs (the scope is a {@code HashSet} whose iteration order is
         * otherwise unspecified). The KEEP's column list is emitted verbatim in the order given
         * by {@link #expectedColumnOrder}; the rewriter does not validate the list against the
         * query's schema, since the test runner asserts on it directly and any mismatch will
         * surface as a clear test failure.
         * <p>
         * Nothing is appended when no field needs recovery &mdash; that is the normal outcome
         * when every converted-keyword field has been dropped, renamed, or rebound by an earlier
         * {@code EVAL}, {@code STATS}, {@code DISSECT}, or {@code GROK} command, or when none of
         * the in-scope fields appear in the expected output.
         */
        private void appendTailEndRecovery() {
            if (scope.isEmpty()) {
                return;
            }
            List<String> recoverable = new ArrayList<>();
            if (expectedColumnOrder.isEmpty()) {
                recoverable.addAll(scope);
            } else {
                Set<String> expectedSet = new HashSet<>(expectedColumnOrder);
                for (String name : scope) {
                    if (expectedSet.contains(name)) {
                        recoverable.add(name);
                    }
                }
            }
            if (recoverable.isEmpty()) {
                return;
            }
            recoverable.sort(Comparator.naturalOrder());
            StringBuilder sb = new StringBuilder();
            sb.append("\n| EVAL ");
            for (int i = 0; i < recoverable.size(); i++) {
                String name = recoverable.get(i);
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(name)
                    .append(" = ")
                    .append(FIELD_EXTRACT_FUNCTION)
                    .append('(')
                    .append(name)
                    .append(", \"")
                    .append(wrapperSubKey)
                    .append("\")");
                rewrittenNames.add(name);
            }
            if (expectedColumnOrder.isEmpty() == false) {
                sb.append("\n| KEEP ");
                for (int i = 0; i < expectedColumnOrder.size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(expectedColumnOrder.get(i));
                }
            }
            out.append(sb);
        }

        private void processSegment(String segment) {
            CommandPosition pos = parseCommandPosition(segment);
            String cmd = pos.keyword();

            if (SOURCE_COMMANDS.contains(cmd)) {
                out.append(rewriteSubqueriesInSourceSegment(segment));
                return;
            }
            if (NAME_ONLY_COMMANDS.contains(cmd)) {
                out.append(segment);
                scope = updateScopeForNameOnlyCommand(cmd, segment, pos, scope);
                return;
            }
            if (cmd.equals("EVAL")) {
                out.append(rewriteFieldRefs(segment, scope, wrapperSubKey, rewrittenNames, skipEvents));
                scope = updateScopeForEvalCommand(segment, pos, scope);
                return;
            }
            if (STATS_REPLACE_COMMANDS.contains(cmd)) {
                String preprocessed = preprocessStatsByAliasing(segment, pos, scope);
                out.append(rewriteFieldRefs(preprocessed, scope, wrapperSubKey, rewrittenNames, skipEvents));
                scope = new HashSet<>();
                return;
            }
            if (STATS_PRESERVE_COMMANDS.contains(cmd)) {
                String preprocessed = preprocessStatsByAliasing(segment, pos, scope);
                out.append(rewriteFieldRefs(preprocessed, scope, wrapperSubKey, rewrittenNames, skipEvents));
                return;
            }
            if (DISSECT_GROK_COMMANDS.contains(cmd)) {
                out.append(rewriteFieldRefs(segment, scope, wrapperSubKey, rewrittenNames, skipEvents));
                scope = updateScopeForDissectOrGrokCommand(cmd, segment, pos, scope);
                return;
            }
            if (NAME_ONLY_BODY_COMMANDS.contains(cmd)) {
                out.append(segment);
                recordNameOnlyBodySkips(cmd, segment, pos, scope, skipEvents);
                return;
            }
            out.append(rewriteFieldRefs(segment, scope, wrapperSubKey, rewrittenNames, skipEvents));
        }

        /**
         * Returns {@code segment} with every parenthesised sub-pipeline whose body begins with
         * {@code FROM} or {@code TS} replaced by its recursively rewritten form. The recursive
         * call uses the same rewrite scope and wrapper sub-key as the outer walker; it does not
         * attempt to restore a particular column order inside the sub-pipeline because the outer
         * query merges by name (UNION-by-name) and the order of columns within a sub-pipeline is
         * not asserted on by the test. Tail-end recovery is also deliberately disabled for the
         * inner walker: see {@link EsqlQueryKeywordFieldRewriter#rewriteInternal} for the
         * rationale.
         * <p>
         * The motivating shape is the csv-spec subquery form
         * {@code FROM a, (FROM b | EVAL ... | LIMIT N), (FROM c | ...)} that appears in the
         * {@code subquery} csv-spec file. Without this recursion every converted-keyword field
         * referenced inside a sub-pipeline would surface as {@code flattened} when ES|QL went to
         * compare or aggregate it (because the inline rewriter would not have wrapped it), so the
         * subquery would either fail to parse or produce wrong values. With recursion, each
         * sub-pipeline receives inline {@code field_extract} wrapping for every reference to a
         * keyword path of its own datasets, leaving the resulting column type as
         * {@code flattened} so the outer FROM can merge with sibling indices without a type
         * conflict and the outer tail-end recovery can convert everything back to {@code keyword}
         * once at the projection boundary.
         * <p>
         * Parentheses inside string literals or comments are not treated as sub-pipeline
         * delimiters; the standard {@link #maskStringsAndComments} mask is applied before the
         * scan. The scan is regex-free: it walks the masked segment one character at a time,
         * tracks a parenthesis-depth counter, and recurses on every depth-1 balanced
         * {@code (...)} block whose body's leading word is {@code FROM} or {@code TS}.
         */
        private String rewriteSubqueriesInSourceSegment(String segment) {
            String masked = maskStringsAndComments(segment);
            int n = segment.length();
            StringBuilder result = new StringBuilder(n);
            int i = 0;
            while (i < n) {
                if (masked.charAt(i) != '(') {
                    result.append(segment.charAt(i));
                    i++;
                    continue;
                }
                int closing = findMatchingParen(masked, i);
                if (closing < 0) {
                    result.append(segment.charAt(i));
                    i++;
                    continue;
                }
                String innerText = segment.substring(i + 1, closing);
                if (looksLikeSourceSubPipeline(innerText)) {
                    // Inline wrapping inside the subquery is still useful (e.g. WHERE
                    // client_ip == "..." over a converted-keyword field), but we deliberately
                    // disable tail-end recovery for the inner walker: see
                    // EsqlQueryKeywordFieldRewriter#rewriteInternal for why a per-subquery
                    // tail-end EVAL conflicts with the outer FROM's sibling-index branches.
                    RewriteResult inner = rewriteInternal(innerText, scopeResolver, wrapperSubKey, List.of(), false);
                    rewrittenNames.addAll(inner.rewrittenFieldNames());
                    skipEvents.addAll(inner.skipEvents());
                    result.append('(').append(inner.rewrittenQuery()).append(')');
                } else {
                    result.append(segment, i, closing + 1);
                }
                i = closing + 1;
            }
            return result.toString();
        }
    }

    /**
     * Returns the index of the {@code )} that closes the {@code (} at {@code openIdx} in
     * {@code masked}, or {@code -1} if the parenthesis is unbalanced. Only the masked form is
     * inspected, so parentheses inside string literals or comments do not influence the result.
     */
    private static int findMatchingParen(String masked, int openIdx) {
        int depth = 0;
        int n = masked.length();
        for (int i = openIdx; i < n; i++) {
            char c = masked.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Returns {@code true} if {@code innerText} (the content of a parenthesised block, without
     * the surrounding parens) starts with the {@code FROM} or {@code TS} keyword as a whole word.
     * Whitespace before the keyword is skipped. Used by
     * {@link PipelineWalker#rewriteSubqueriesInSourceSegment} to decide whether to recurse into a
     * parenthesised block; non-source parenthesised blocks (function arguments, expression
     * groupings) are left alone.
     */
    private static boolean looksLikeSourceSubPipeline(String innerText) {
        int n = innerText.length();
        int i = 0;
        while (i < n && Character.isWhitespace(innerText.charAt(i))) {
            i++;
        }
        for (String cmd : SOURCE_COMMANDS) {
            int candidateEnd = i + cmd.length();
            if (candidateEnd <= n
                && innerText.substring(i, candidateEnd).equalsIgnoreCase(cmd)
                && (candidateEnd == n || isWordBoundary(innerText.charAt(candidateEnd)))) {
                return true;
            }
        }
        return false;
    }

    /** Parsed leading command keyword of a segment together with the offset where its body starts. */
    private record CommandPosition(String keyword, int bodyStart) {}

    /**
     * Splits {@code query} into top-level pipeline segment ranges. A pipe character starts a new
     * segment only when it is not inside a string literal, line comment, or parenthesized
     * sub-expression. Every returned range is non-empty except possibly the last one (a trailing
     * empty range is preserved so concatenating the substrings reproduces {@code query} exactly).
     */
    private static List<int[]> splitIntoSegmentRanges(String query) {
        String masked = maskStringsAndComments(query);
        List<int[]> ranges = new ArrayList<>();
        int n = masked.length();
        int start = 0;
        int depth = 0;
        for (int i = 0; i < n; i++) {
            char c = masked.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == '|' && depth == 0) {
                if (i > start) {
                    ranges.add(new int[] { start, i });
                }
                start = i;
            }
        }
        ranges.add(new int[] { start, n });
        return ranges;
    }

    /**
     * Parses the leading command keyword of {@code segment}, returning its uppercase canonical
     * form (e.g. {@code "INLINE STATS"} as a single token) and the offset within {@code segment}
     * where the body starts &mdash; right after the keyword token, before any leading whitespace
     * of the body.
     */
    private static CommandPosition parseCommandPosition(String segment) {
        int n = segment.length();
        int i = 0;
        if (i < n && segment.charAt(i) == '|') {
            i++;
        }
        while (i < n && Character.isWhitespace(segment.charAt(i))) {
            i++;
        }
        for (String cmd : TWO_WORD_COMMANDS) {
            int candidateEnd = i + cmd.length();
            if (candidateEnd <= n && segment.substring(i, candidateEnd).equalsIgnoreCase(cmd)) {
                if (candidateEnd == n || isWordBoundary(segment.charAt(candidateEnd))) {
                    return new CommandPosition(cmd.toUpperCase(Locale.ROOT), candidateEnd);
                }
            }
        }
        int j = i;
        while (j < n && (Character.isLetterOrDigit(segment.charAt(j)) || segment.charAt(j) == '_')) {
            j++;
        }
        if (j == i) {
            return new CommandPosition("", i);
        }
        return new CommandPosition(segment.substring(i, j).toUpperCase(Locale.ROOT), j);
    }

    private static boolean isWordBoundary(char c) {
        return Character.isLetterOrDigit(c) == false && c != '_';
    }

    /**
     * Returns the scope after a {@code KEEP}, {@code DROP}, or {@code RENAME} command body.
     * {@code KEEP} intersects the scope with the kept-list, {@code DROP} removes the dropped
     * names, and {@code RENAME} swaps each {@code old} for the corresponding {@code new}. If
     * either body contains a wildcard, the scope is preserved as-is &mdash; an over-approximation
     * keeps the rewriter conservative (it may attempt to wrap a name that the original query has
     * since dropped, but such references would already be invalid in the original query and so
     * the failure mode is unchanged).
     */
    private static Set<String> updateScopeForNameOnlyCommand(String cmd, String segment, CommandPosition pos, Set<String> scope) {
        Set<String> next = new HashSet<>(scope);
        String body = segment.substring(pos.bodyStart());
        List<String> parts = splitTopLevelCommaPieces(body);
        if (parts.stream().anyMatch(s -> s.contains("*"))) {
            return next;
        }
        switch (cmd) {
            case "KEEP":
                next.retainAll(parts.stream().map(String::trim).toList());
                return next;
            case "DROP":
                next.removeAll(parts.stream().map(String::trim).toList());
                return next;
            case "RENAME":
                for (String piece : parts) {
                    Matcher m = RENAME_PAIR.matcher(piece);
                    if (m.matches()) {
                        String oldName = m.group(1);
                        String newName = m.group(2);
                        if (next.remove(oldName)) {
                            next.add(newName);
                        }
                    }
                }
                return next;
            default:
                throw new AssertionError("unexpected name-only command [" + cmd + "]");
        }
    }

    /**
     * Removes every name bound by a {@code DISSECT} or {@code GROK} pattern from the scope. After
     * the command, those columns hold the keyword tokens extracted from the source value, not the
     * original {@code flattened} field, so neither subsequent commands nor the tail-end recovery
     * should re-wrap them. The source field name (the attribute being matched against) is left
     * untouched here: if it was already in scope it remains in scope, because the command does
     * not rebind it.
     * <p>
     * Names are taken from the first string literal in the body, which is the literal pattern.
     * For {@code DISSECT}, names are recognised as {@code %{[modifier]name}}. For {@code GROK},
     * names are recognised both as Java named-capture groups {@code (?<name>...)} and as the
     * grok-library shorthand {@code %{PATTERN:name[:type]}}. Pattern fragments that bind no name
     * (for example a bare {@code %{IP}} in {@code GROK}, which would bind the column {@code IP})
     * are intentionally not handled here because the csv-spec corpus consistently uses the
     * {@code :name} form, and matching bare {@code %{PATTERN}} would risk over-aggressive scope
     * mutation against arbitrary user identifiers.
     */
    private static Set<String> updateScopeForDissectOrGrokCommand(String cmd, String segment, CommandPosition pos, Set<String> scope) {
        Set<String> next = new HashSet<>(scope);
        String body = segment.substring(pos.bodyStart());
        String pattern = extractFirstStringLiteralContent(body);
        if (pattern == null) {
            return next;
        }
        if (cmd.equals("DISSECT")) {
            removeMatches(pattern, DISSECT_NAME_BINDING, next);
        } else {
            removeMatches(pattern, GROK_PERCENT_NAME_BINDING, next);
            removeMatches(pattern, GROK_NAMED_CAPTURE_BINDING, next);
        }
        return next;
    }

    /** Removes from {@code target} every capture-group-1 match of {@code pattern} against {@code input}. */
    private static void removeMatches(String input, Pattern pattern, Set<String> target) {
        Matcher m = pattern.matcher(input);
        while (m.find()) {
            target.remove(m.group(1));
        }
    }

    /**
     * Returns the contents (without the surrounding quotes) of the first string literal that
     * appears in {@code text}, or {@code null} if {@code text} has no string literal. Recognises
     * both the standard {@code "..."} form and the triple-quoted {@code """..."""} form.
     * Backslash escapes inside a standard string consume the next character; the triple-quoted
     * form has no escape processing.
     */
    private static String extractFirstStringLiteralContent(String text) {
        int n = text.length();
        for (int i = 0; i < n; i++) {
            if (text.charAt(i) != '"') {
                continue;
            }
            if (i + 2 < n && text.charAt(i + 1) == '"' && text.charAt(i + 2) == '"') {
                int start = i + 3;
                int j = start;
                while (j + 2 < n && (text.charAt(j) != '"' || text.charAt(j + 1) != '"' || text.charAt(j + 2) != '"')) {
                    j++;
                }
                return j + 2 < n ? text.substring(start, j) : text.substring(start);
            }
            int start = i + 1;
            int j = start;
            while (j < n && text.charAt(j) != '"') {
                if (text.charAt(j) == '\\' && j + 1 < n) {
                    j++;
                }
                j++;
            }
            return j < n ? text.substring(start, j) : text.substring(start);
        }
        return null;
    }

    /**
     * Removes the left-hand side of each top-level assignment in an {@code EVAL} body from the
     * scope. After an EVAL the column referenced by that name is whatever the right-hand side
     * produced, not the original {@code flattened} field, so subsequent commands must not re-wrap
     * it. Names introduced for the first time (not previously in scope) are removed harmlessly.
     */
    private static Set<String> updateScopeForEvalCommand(String segment, CommandPosition pos, Set<String> scope) {
        Set<String> next = new HashSet<>(scope);
        String body = segment.substring(pos.bodyStart());
        String masked = maskStringsAndComments(body);
        Matcher m = ASSIGNMENT_LHS.matcher(masked);
        while (m.find()) {
            if (topLevelInBody(masked, m.start())) {
                next.remove(m.group(1));
            }
        }
        return next;
    }

    /**
     * For each bare-name grouping in the {@code BY} clause of a STATS body that is currently in
     * scope, expands {@code BY ..., field, ...} to {@code BY ..., field = field, ...}. The
     * subsequent regex rewrite then turns the right-hand side into
     * {@code field = field_extract(field, "v")}, which preserves the output column name as
     * {@code field} (of type keyword) and lets downstream commands reference it normally.
     */
    private static String preprocessStatsByAliasing(String segment, CommandPosition pos, Set<String> scope) {
        int byOffset = findTopLevelBy(segment, pos.bodyStart());
        if (byOffset < 0) {
            return segment;
        }
        int byBodyStart = byOffset + "BY".length();
        List<int[]> groupRanges = splitTopLevelCommaPiecesAsRanges(segment, byBodyStart);
        if (groupRanges.isEmpty()) {
            return segment;
        }
        StringBuilder sb = new StringBuilder(segment);
        for (int i = groupRanges.size() - 1; i >= 0; i--) {
            int[] r = groupRanges.get(i);
            String groupText = segment.substring(r[0], r[1]);
            Matcher m = BARE_IDENTIFIER.matcher(groupText);
            if (m.matches() && scope.contains(m.group(1))) {
                int absStart = r[0] + m.start(1);
                int absEnd = r[0] + m.end(1);
                String name = m.group(1);
                sb.replace(absStart, absEnd, name + " = " + name);
            }
        }
        return sb.toString();
    }

    /**
     * Finds the offset of the {@code BY} keyword that lives at the top level of a STATS body
     * (parenthesis depth 0 relative to {@code bodyStart}), or {@code -1} if none exists. Used to
     * locate the BY clause without being fooled by a {@code BY} substring inside a function call
     * argument or a string literal.
     */
    private static int findTopLevelBy(String segment, int bodyStart) {
        String masked = maskStringsAndComments(segment);
        Matcher m = BY_KEYWORD.matcher(masked);
        if (m.find(bodyStart) == false) {
            return -1;
        }
        do {
            if (parenDepthBetween(masked, bodyStart, m.start()) == 0) {
                return m.start();
            }
        } while (m.find());
        return -1;
    }

    /** Returns true when {@code masked.charAt(pos)} is at parenthesis depth 0 relative to body start. */
    private static boolean topLevelInBody(String masked, int pos) {
        int depth = 0;
        for (int i = 0; i < pos; i++) {
            char c = masked.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
        }
        return depth == 0;
    }

    /** Returns the parenthesis depth at {@code targetPos} starting from {@code from} (inclusive). */
    private static int parenDepthBetween(String masked, int from, int targetPos) {
        int depth = 0;
        for (int i = from; i < targetPos; i++) {
            char c = masked.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
        }
        return depth;
    }

    /**
     * Splits {@code body} into top-level comma-separated pieces, where "top-level" means outside
     * any parenthesized sub-expression and outside string literals or comments. Empty pieces at
     * the start or end are preserved as trimmed empty strings.
     */
    private static List<String> splitTopLevelCommaPieces(String body) {
        String masked = maskStringsAndComments(body);
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        int n = masked.length();
        for (int i = 0; i < n; i++) {
            char c = masked.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(body.substring(start, i).trim());
                start = i + 1;
            }
        }
        parts.add(body.substring(start).trim());
        return parts;
    }

    /**
     * Returns the [start, end) ranges (within {@code segment}, indexing into the original text)
     * of each top-level comma-separated piece that begins at offset {@code from} and continues
     * until the segment's end or a top-level pipe character.
     */
    private static List<int[]> splitTopLevelCommaPiecesAsRanges(String segment, int from) {
        String masked = maskStringsAndComments(segment);
        List<int[]> ranges = new ArrayList<>();
        int depth = 0;
        int n = masked.length();
        int start = from;
        for (int i = from; i < n; i++) {
            char c = masked.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                ranges.add(new int[] { start, i });
                start = i + 1;
            } else if (c == '|' && depth == 0) {
                ranges.add(new int[] { start, i });
                return ranges;
            }
        }
        ranges.add(new int[] { start, n });
        return ranges;
    }

    /**
     * Rewrites every standalone occurrence of a name in {@code scope} within {@code segment} as
     * {@code field_extract(name, "wrapperSubKey")}, skipping string literals, line comments, the
     * LHS of any assignment, and the LHS of any match-operator ({@code :}) comparison. Field
     * names actually wrapped are added to {@code rewrittenSink}; in-scope identifiers that fell
     * inside a match-operator LHS protection range &mdash; and so were intentionally not wrapped
     * &mdash; are reported via {@code skipSink} as {@link SkipSite#MATCH_OPERATOR_LHS} events so
     * the caller can inventory them.
     * <p>
     * The other two protection categories (string/comment ranges and assignment LHS ranges) are
     * not reported as skip events because their occurrences are not field references in the
     * first place: an identifier inside a string literal is part of the literal text, and an
     * identifier on the LHS of {@code name = expr} is being defined, not read. Only the
     * match-operator LHS represents a reference whose position the rewriter would otherwise have
     * wrapped but cannot.
     */
    private static String rewriteFieldRefs(
        String segment,
        Set<String> scope,
        String wrapperSubKey,
        Set<String> rewrittenSink,
        List<SkipEvent> skipSink
    ) {
        if (scope.isEmpty()) {
            return segment;
        }
        List<int[]> nonReferenceRanges = new ArrayList<>();
        addStringAndCommentRanges(segment, nonReferenceRanges);
        addAssignmentTargetRanges(segment, nonReferenceRanges);
        List<int[]> matchOperatorLhsRanges = new ArrayList<>();
        addMatchOperatorLhsRanges(segment, matchOperatorLhsRanges);

        record CandidateMatch(int start, int end, String field) {}
        List<CandidateMatch> matches = new ArrayList<>();

        for (String field : scope) {
            Pattern p = Pattern.compile(
                "(?<!" + IDENTIFIER_BOUNDARY_CLASS + ")" + Pattern.quote(field) + "(?!" + IDENTIFIER_BOUNDARY_CLASS + ")"
            );
            Matcher m = p.matcher(segment);
            while (m.find()) {
                if (overlapsAnyRange(m.start(), m.end(), nonReferenceRanges)) {
                    continue;
                }
                if (overlapsAnyRange(m.start(), m.end(), matchOperatorLhsRanges)) {
                    skipSink.add(new SkipEvent(SkipSite.MATCH_OPERATOR_LHS, field));
                    continue;
                }
                matches.add(new CandidateMatch(m.start(), m.end(), field));
            }
        }
        if (matches.isEmpty()) {
            return segment;
        }
        matches.sort(Comparator.comparingInt(CandidateMatch::start).reversed());

        StringBuilder result = new StringBuilder(segment);
        for (CandidateMatch m : matches) {
            String wrapped = FIELD_EXTRACT_FUNCTION + "(" + m.field() + ", \"" + wrapperSubKey + "\")";
            result.replace(m.start(), m.end(), wrapped);
            rewrittenSink.add(m.field());
        }
        return result.toString();
    }

    /**
     * Records one {@link SkipEvent} per in-scope identifier appearing inside the body of a
     * {@code MV_EXPAND} or {@code ENRICH} command. Both grammar slots accept only attribute
     * references (no expressions), so the rewriter passes the body through unchanged; this
     * method makes the resulting "we did not wrap field X here" decisions visible to callers.
     * String literals and line comments inside the body are masked first so an identifier that
     * happens to appear inside one of them does not falsely trigger a skip event.
     */
    private static void recordNameOnlyBodySkips(
        String cmd,
        String segment,
        CommandPosition pos,
        Set<String> scope,
        List<SkipEvent> skipSink
    ) {
        if (scope.isEmpty()) {
            return;
        }
        SkipSite site = cmd.equals("MV_EXPAND") ? SkipSite.MV_EXPAND_ARG : SkipSite.ENRICH_BODY;
        String body = segment.substring(pos.bodyStart());
        String masked = maskStringsAndComments(body);
        for (String field : scope) {
            Pattern p = Pattern.compile(
                "(?<!" + IDENTIFIER_BOUNDARY_CLASS + ")" + Pattern.quote(field) + "(?!" + IDENTIFIER_BOUNDARY_CLASS + ")"
            );
            Matcher m = p.matcher(masked);
            while (m.find()) {
                skipSink.add(new SkipEvent(site, field));
            }
        }
    }

    /**
     * Returns a copy of {@code query} with every double-quoted string literal (including the
     * {@code """..."""} triple-quoted form) and every {@code // ...} line comment replaced by
     * spaces of equal length. Offsets of all other characters are preserved.
     */
    private static String maskStringsAndComments(String query) {
        char[] out = query.toCharArray();
        int i = 0;
        int n = out.length;
        while (i < n) {
            char c = out[i];
            if (c == '"') {
                int start = i;
                if (i + 2 < n && out[i + 1] == '"' && out[i + 2] == '"') {
                    i += 3;
                    while (i + 2 < n && (out[i] != '"' || out[i + 1] != '"' || out[i + 2] != '"')) {
                        i++;
                    }
                    i = Math.min(i + 3, n);
                } else {
                    i++;
                    while (i < n && out[i] != '"') {
                        if (out[i] == '\\' && i + 1 < n) {
                            i++;
                        }
                        i++;
                    }
                    i = Math.min(i + 1, n);
                }
                for (int k = start; k < i; k++) {
                    out[k] = ' ';
                }
            } else if (c == '/' && i + 1 < n && out[i + 1] == '/') {
                int start = i;
                while (i < n && out[i] != '\n') {
                    i++;
                }
                for (int k = start; k < i; k++) {
                    out[k] = ' ';
                }
            } else {
                i++;
            }
        }
        return new String(out);
    }

    /**
     * Same as {@link #maskStringsAndComments} but records the [start, end) ranges of each string
     * literal and line comment directly into {@code ranges}. Used by {@link #rewriteFieldRefs} to
     * build the protected-range list without re-scanning.
     */
    private static void addStringAndCommentRanges(String query, List<int[]> ranges) {
        int i = 0;
        int n = query.length();
        while (i < n) {
            char c = query.charAt(i);
            if (c == '"') {
                int start = i;
                if (i + 2 < n && query.charAt(i + 1) == '"' && query.charAt(i + 2) == '"') {
                    i += 3;
                    while (i + 2 < n && (query.charAt(i) != '"' || query.charAt(i + 1) != '"' || query.charAt(i + 2) != '"')) {
                        i++;
                    }
                    i = Math.min(i + 3, n);
                } else {
                    i++;
                    while (i < n && query.charAt(i) != '"') {
                        if (query.charAt(i) == '\\' && i + 1 < n) {
                            i++;
                        }
                        i++;
                    }
                    i = Math.min(i + 1, n);
                }
                ranges.add(new int[] { start, i });
            } else if (c == '/' && i + 1 < n && query.charAt(i + 1) == '/') {
                int start = i;
                while (i < n && query.charAt(i) != '\n') {
                    i++;
                }
                ranges.add(new int[] { start, i });
            } else {
                i++;
            }
        }
    }

    /**
     * Adds the identifier on the left-hand side of every assignment ({@code name = ...}) to
     * {@code ranges}. The {@code (?!=)} lookahead in {@link #ASSIGNMENT_LHS} prevents the
     * comparison operator {@code ==} from being treated as an assignment.
     */
    private static void addAssignmentTargetRanges(String query, List<int[]> ranges) {
        Matcher m = ASSIGNMENT_LHS.matcher(query);
        while (m.find()) {
            int start = m.start(1);
            int end = m.end(1);
            if (overlapsAnyRange(start, end, ranges) == false) {
                ranges.add(new int[] { start, end });
            }
        }
    }

    /**
     * Adds the identifier on the left-hand side of every match-operator comparison
     * ({@code name : "value"}, {@code name::cast : "value"}) to {@code ranges}. The match
     * operator {@code :} accepts only an attribute reference on its left side; wrapping it in
     * {@code field_extract(...)} would surface as a parser error, so the identifier is protected
     * from the regex rewrite. The pattern's negative lookahead skips the {@code ::} cast operator
     * so that a cast suffix neither protects the wrong range nor pulls the rewriter off track.
     * <p>
     * String literals and line comments are masked first so a {@code :} appearing inside one (for
     * example inside a literal regex pattern or a {@code KQL("a : b")} call) does not falsely
     * trigger LHS protection.
     */
    private static void addMatchOperatorLhsRanges(String query, List<int[]> ranges) {
        String masked = maskStringsAndComments(query);
        Matcher m = MATCH_OPERATOR_LHS.matcher(masked);
        while (m.find()) {
            int start = m.start(1);
            int end = m.end(1);
            if (overlapsAnyRange(start, end, ranges) == false) {
                ranges.add(new int[] { start, end });
            }
        }
    }

    /** Returns {@code true} if {@code [start, end)} overlaps any of {@code ranges}. */
    private static boolean overlapsAnyRange(int start, int end, List<int[]> ranges) {
        for (int[] r : ranges) {
            if (start < r[1] && end > r[0]) {
                return true;
            }
        }
        return false;
    }
}
