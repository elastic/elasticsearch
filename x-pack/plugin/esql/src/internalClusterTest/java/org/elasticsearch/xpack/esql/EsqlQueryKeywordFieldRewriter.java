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
 * handle backtick-quoted identifiers. Grammar slots that admit only an attribute (not an arbitrary
 * expression) are protected via {@link #NAME_ONLY_BODY_COMMANDS} and the
 * {@link SkipSite#MATCH_OPERATOR_LHS} carve-out: {@code MV_EXPAND}, {@code ENRICH ON / WITH},
 * {@code LOOKUP JOIN ... ON ...}, and the LHS of the match operator {@code :}. Each in-scope
 * identifier in those positions is left bare and recorded as a {@link SkipEvent} so callers can
 * inventory exactly what was not exercised. Other expression-rejecting positions not yet enumerated
 * here will surface as parser failures, which is the intended way to inventory ES|QL surfaces that
 * {@code field_extract} cannot currently substitute for.
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
        MATCH_OPERATOR_LHS,
        /**
         * Body of a {@code LOOKUP JOIN <index> ON <field>[, <field>]*} command. ES|QL's
         * {@code JOIN ON} clause accepts only bare attribute references on each side of every
         * join condition (see {@code LogicalPlanBuilder.processFieldBasedJoin} and
         * {@code Analyzer.resolveAndOrientJoinCondition}), so wrapping the join key in
         * {@code field_extract(...)} would surface as a parser or verifier error rather than
         * exercise the function. The rewriter passes the body through verbatim and emits one
         * skip event per in-scope identifier appearing anywhere after the {@code LOOKUP JOIN}
         * keyword.
         */
        LOOKUP_JOIN_ON,
        /**
         * Body of an {@code INSIST_🐔 <field>[, <field>]*} command. {@code INSIST_🐔} is a
         * developer-only command (gated behind {@code isDevVersion()} in {@code Project.g4})
         * whose grammar accepts only a list of qualified-name patterns; wrapping a name there
         * in {@code field_extract(...)} would surface as a parser error.
         */
        INSIST_BODY,
        /**
         * Identifier appearing inside an ES|QL qualified-name reference of the form
         * {@code [<index>].[<field>]}. The bracketed positions accept only an unquoted
         * identifier (see {@code qualifiedName} / {@code qualifiedNamePattern} in the parser
         * grammar), so wrapping the inside in {@code field_extract(...)} would not parse. The
         * rewriter records the skip but leaves the bracket contents untouched.
         */
        QUALIFIED_NAME_BRACKETS
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
     * Anchored form of {@link #IDENTIFIER_PATTERN}, used to decide whether a candidate column
     * name can appear unquoted inside a generated {@code KEEP} clause. Names that do not match
     * are wrapped in backticks before emission; see {@code quoteIdentifierIfNeeded} on the
     * pipeline walker.
     */
    private static final Pattern BARE_IDENTIFIER_FORM = Pattern.compile("^" + IDENTIFIER_PATTERN + "$");

    /**
     * Pattern matching the left-hand side of an assignment: an identifier (possibly dotted)
     * followed by optional whitespace, a single {@code =}, and a negative lookahead to exclude the
     * comparison operator {@code ==}.
     */
    private static final Pattern ASSIGNMENT_LHS = Pattern.compile("(?<![a-zA-Z0-9_.])(" + IDENTIFIER_PATTERN + ")\\s*=(?!=)");

    /**
     * Two consecutive bracketed names separated by a dot, as in {@code [employees].[gender]}.
     * Two capture groups expose the unquoted contents of each bracket pair so they can be added
     * to the rewriter's protected-range list. The pattern intentionally accepts arbitrary
     * non-{@code ]} content inside each pair: validating the inside as an identifier would only
     * matter if the rewriter were going to wrap something there, and any non-identifier content
     * would not match a scope field name anyway, so over-protecting is harmless. {@code [1, 2]}
     * array literals are not matched because they are never followed by a {@code .[...]} second
     * bracket pair.
     */
    private static final Pattern QUALIFIED_NAME_BRACKETS = Pattern.compile("\\[([^\\]]*)\\]\\s*\\.\\s*\\[([^\\]]*)\\]");

    /** Pattern matching a {@code RENAME old AS new} pair (used after splitting on top-level commas). */
    private static final Pattern RENAME_PAIR = Pattern.compile(
        "^\\s*(" + IDENTIFIER_PATTERN + ")\\s+AS\\s+(" + IDENTIFIER_PATTERN + ")\\s*$",
        Pattern.CASE_INSENSITIVE
    );

    /** Pattern matching a bare identifier with optional surrounding whitespace. */
    private static final Pattern BARE_IDENTIFIER = Pattern.compile("^\\s*(" + IDENTIFIER_PATTERN + ")\\s*$");

    /**
     * Pattern matching an explicit assignment alias at the start of a STATS aggregate or
     * grouping-key piece: optional leading whitespace, an identifier (group 1), optional
     * whitespace, and a single {@code =} that is not part of the {@code ==} comparison
     * operator. The pattern is anchored at the start of the piece so it cannot be misled by an
     * inner {@code =} that lives inside a function argument or a {@code WHERE} predicate.
     */
    private static final Pattern STATS_PIECE_ALIAS_LHS = Pattern.compile("^\\s*(" + IDENTIFIER_PATTERN + ")\\s*=(?!=)");

    /**
     * Pattern matching any single identifier with word-boundary surroundings, used to scan a
     * piece text for in-scope field names without compiling a per-call alternation. Compiled
     * once and reused; identity-comparison membership in the active scope happens in the
     * caller.
     */
    private static final Pattern ANY_IDENTIFIER = Pattern.compile("(?<![a-zA-Z0-9_.])(" + IDENTIFIER_PATTERN + ")(?![a-zA-Z0-9_.])");

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

    /** Pattern matching the standalone {@code WITH} keyword used to delimit the ENRICH WITH clause. */
    private static final Pattern WITH_KEYWORD = Pattern.compile("\\bWITH\\b", Pattern.CASE_INSENSITIVE);

    /**
     * Pattern matching the left-hand side of an ENRICH WITH piece, in either of the two forms
     * the grammar accepts: {@code new_field = enrich_field} (capture group 1 = {@code new_field})
     * and the bare {@code new_field} short-hand (capture group 2 = {@code new_field}). Trailing
     * whitespace is tolerated. The mutually-exclusive groups let the caller pick whichever
     * matched.
     */
    private static final Pattern ENRICH_WITH_LHS = Pattern.compile(
        "^\\s*(?:(" + IDENTIFIER_PATTERN + ")\\s*=|(" + IDENTIFIER_PATTERN + ")\\s*)"
    );

    /**
     * Prefix for the synthetic alias names the rewriter introduces for bare in-scope grouping
     * keys in a {@code STATS}/{@code INLINE STATS BY} clause. Using a name that does not appear
     * in any csv-spec dataset (the prefix is checked against {@link #IDENTIFIER_PATTERN} and is
     * not a valid mapping leaf) keeps the alias from colliding with any pre-existing column on
     * the input row, and lets a follow-up {@code RENAME} step restore the original column name
     * after the aggregation completes. See {@code preprocessStatsByAliasing} for the full
     * rationale, including why the trivial {@code BY field = field} aliasing the rewriter used
     * before would shadow the input column inside the same {@code STATS} segment.
     */
    private static final String BY_ALIAS_PREFIX = "__kw_";

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
     * {@code MV_EXPAND} unflattens a multi-value column without changing its type, {@code ENRICH}
     * appends new columns from the policy's enrich index without touching any existing column,
     * and {@code INSIST_🐔} is a developer-only command (gated on the dev version per
     * {@code Project.g4}) that asserts a column is mapped without otherwise modifying the row.
     */
    private static final Set<String> NAME_ONLY_BODY_COMMANDS = Set.of("MV_EXPAND", "ENRICH", "LOOKUP JOIN", "INSIST_🐔");

    /** Names bound by a {@code DISSECT} pattern as {@code %{[modifier]name}}. */
    private static final Pattern DISSECT_NAME_BINDING = Pattern.compile("%\\{[?+&*-]?\\s*(" + IDENTIFIER_PATTERN + ")");

    /** Names bound by a {@code GROK} pattern as {@code %{PATTERN:name[:type]}}. */
    private static final Pattern GROK_PERCENT_NAME_BINDING = Pattern.compile("%\\{[A-Za-z_][A-Za-z0-9_]*:(" + IDENTIFIER_PATTERN + ")");

    /** Names bound by a {@code GROK} pattern as a Java named-capture group {@code (?<name>...)}. */
    private static final Pattern GROK_NAMED_CAPTURE_BINDING = Pattern.compile("\\(\\?<(" + IDENTIFIER_PATTERN + ")>");

    /**
     * Command keywords whose canonical form is a literal string the per-character identifier loop
     * in {@link #parseCommandPosition} cannot reconstruct on its own. Two reasons drive entries
     * onto this list:
     * <ul>
     *   <li>the keyword spans more than one whitespace-separated token ({@code INLINE STATS},
     *       {@code LOOKUP JOIN}), so the regular alphanumeric loop would stop at the first
     *       internal space and miss the second word; and</li>
     *   <li>the keyword contains a non-identifier character that the loop's
     *       {@link Character#isLetterOrDigit(char)} predicate rejects &mdash; in particular
     *       {@code INSIST_🐔}, whose trailing chicken emoji is a supplementary-plane codepoint
     *       (surrogate pair {@code U+D83D U+DC14}) that fails the {@code char}-level predicate.</li>
     * </ul>
     * Entries are matched literally (case-insensitive) at the segment's keyword position before
     * the alphanumeric fallback, so the canonical form returned by {@link #parseCommandPosition}
     * always preserves the multi-token shape.
     */
    private static final List<String> LITERAL_COMMAND_KEYWORDS = List.of("INLINE STATS", "LOOKUP JOIN", "INSIST_🐔");

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
                processSegment(query.substring(range[0], range[1]), out);
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
                    sb.append(quoteIdentifierIfNeeded(expectedColumnOrder.get(i)));
                }
            }
            out.append(sb);
        }

        /**
         * Returns {@code name} unchanged when it is a syntactically valid bare ES|QL identifier
         * (matches {@link #IDENTIFIER_PATTERN}), and otherwise returns it wrapped in backticks.
         * <p>
         * The csv-spec expected-results header may declare columns whose names are not valid bare
         * identifiers &mdash; either because they were given as quoted strings in the header
         * (e.g. {@code "COUNT(*)":long}) or because they include grammar-significant characters
         * such as parentheses, dashes, asterisks, spaces, or arithmetic operators (e.g.
         * {@code constant_keyword-foo:keyword}, {@code SUBSTRING(last_name, 0, 1):keyword},
         * {@code height * 3.281:double}). Emitting such a name verbatim into the trailing
         * {@code KEEP} produces an ES|QL parser error (typically a token-recognition failure on
         * the first non-identifier character) before any of the rewriter's wrap-site verification
         * can run. Wrapping the name in backticks is the parser-level mechanism for projecting a
         * column whose name is not a bare identifier, and matches what users would write by hand
         * in the original csv-spec query (see e.g. {@code WHERE `COUNT(*)` > 1} in
         * {@code inlinestats.csv-spec}'s {@code afterEnrich}).
         * <p>
         * Backticks already inside {@code name} are not currently escaped: no csv-spec header in
         * the corpus contains a backtick in a column name, and adding escape logic without a real
         * test case would only obscure the more common cases this method is built for.
         */
        private static String quoteIdentifierIfNeeded(String name) {
            return BARE_IDENTIFIER_FORM.matcher(name).matches() ? name : "`" + name + "`";
        }

        /**
         * Dispatches a single top-level pipeline command to the rewrite strategy that matches its
         * keyword, appending the (possibly rewritten) segment text to {@code sink}. The walker's
         * top-level loop passes {@link #out} as the sink so command output flows into the final
         * rewritten query; the FORK-branch path passes a branch-local {@code StringBuilder} so
         * that each parenthesised branch is rewritten in isolation while still sharing the
         * walker's {@link #scope}, {@link #rewrittenNames}, and {@link #skipEvents} sinks.
         */
        private void processSegment(String segment, StringBuilder sink) {
            CommandPosition pos = parseCommandPosition(segment);
            String cmd = pos.keyword();

            if (SOURCE_COMMANDS.contains(cmd)) {
                sink.append(rewriteSubqueriesInSourceSegment(segment));
                return;
            }
            if (NAME_ONLY_COMMANDS.contains(cmd)) {
                sink.append(segment);
                scope = updateScopeForNameOnlyCommand(cmd, segment, pos, scope);
                return;
            }
            if (cmd.equals("EVAL")) {
                sink.append(rewriteFieldRefs(segment, scope, wrapperSubKey, rewrittenNames, skipEvents));
                scope = updateScopeForEvalCommand(segment, pos, scope);
                return;
            }
            if (STATS_REPLACE_COMMANDS.contains(cmd)) {
                StatsByPreprocessing pre = preprocessStatsByAliasing(segment, pos, scope);
                sink.append(rewriteFieldRefs(pre.segment(), scope, wrapperSubKey, rewrittenNames, skipEvents));
                appendRenameBackForAliasedBy(sink, pre.aliasedFields(), false);
                scope = new HashSet<>();
                return;
            }
            if (STATS_PRESERVE_COMMANDS.contains(cmd)) {
                StatsByPreprocessing pre = preprocessStatsByAliasing(segment, pos, scope);
                sink.append(rewriteFieldRefs(pre.segment(), scope, wrapperSubKey, rewrittenNames, skipEvents));
                appendRenameBackForAliasedBy(sink, pre.aliasedFields(), true);
                scope.removeAll(pre.shadowedFields());
                return;
            }
            if (DISSECT_GROK_COMMANDS.contains(cmd)) {
                sink.append(rewriteFieldRefs(segment, scope, wrapperSubKey, rewrittenNames, skipEvents));
                scope = updateScopeForDissectOrGrokCommand(cmd, segment, pos, scope);
                return;
            }
            if (NAME_ONLY_BODY_COMMANDS.contains(cmd)) {
                sink.append(segment);
                recordNameOnlyBodySkips(cmd, segment, pos, scope, skipEvents);
                if (cmd.equals("ENRICH")) {
                    scope = updateScopeForEnrichCommand(segment, pos, scope);
                }
                return;
            }
            if (cmd.equals("FORK")) {
                sink.append(rewriteForkSegment(segment, pos));
                return;
            }
            sink.append(rewriteFieldRefs(segment, scope, wrapperSubKey, rewrittenNames, skipEvents));
        }

        /**
         * Rewrites a {@code FORK} segment by recursing into each parenthesised branch as its own
         * mini-pipeline. Without this recursion the regex rewriter inside {@link #rewriteFieldRefs}
         * would treat the entire body of {@code FORK} as a single expression context and wrap
         * in-scope identifiers indiscriminately, even when they sit inside a name-only command
         * such as {@code DROP gender} or {@code MV_EXPAND job_positions} that the grammar does
         * not allow expressions in (see {@code fork.forkBranchWithDrop} and
         * {@code fork.forkBranchWithMvExpand} in the corpus, which produce ES|QL parser errors
         * on the {@code field_extract(} token of the wrapped name).
         * <p>
         * FORK branches execute in parallel against the same input row, so each branch starts
         * from the same {@link #scope} the FORK does. The walker therefore snapshots the outer
         * scope, restores it before each branch, and reinstates the snapshot after the segment
         * returns &mdash; FORK itself does not rebind any column at the outer level (it only adds
         * the {@code _fork} sentinel column, which is keyword and not in flattened scope), so the
         * post-FORK scope equals the pre-FORK scope. {@link #rewrittenNames} and
         * {@link #skipEvents} are preserved across branches because they are union-shaped sinks
         * (the same field wrapped or skipped in two branches is reported once per occurrence,
         * which is the desired inventory granularity).
         * <p>
         * Parenthesised content inside string literals or comments is not treated as a branch;
         * the standard {@link #maskStringsAndComments} mask is applied before the scan. The
         * {@code FORK} keyword itself plus any whitespace between branches is emitted verbatim.
         */
        private String rewriteForkSegment(String segment, CommandPosition pos) {
            String body = segment.substring(pos.bodyStart());
            String masked = maskStringsAndComments(body);
            int n = body.length();
            StringBuilder result = new StringBuilder();
            result.append(segment, 0, pos.bodyStart());
            Set<String> snapshot = new HashSet<>(scope);
            // Collect each branch's rewritten body and outgoing scope. ES|QL FORK requires
            // every branch to expose the same column name with the same type; if one branch
            // rebinds a column to keyword (e.g. via the INLINE STATS BY {@code __kw_} alias
            // pattern from Bug 8) while another keeps it flattened, the analyzer fails with
            // "conflicting data types in FORK branches". To equalize the schema we append an
            // {@code | EVAL <name> = field_extract(<name>, "v")} tail to every branch that
            // still carries a flattened column that some sibling branch dropped, so all
            // branches converge on the same keyword type for that column. See
            // {@code fork.forkBranchWithInlineStats} in the corpus.
            List<ForkBranchRewrite> branchRewrites = new ArrayList<>();
            List<int[]> branchSpans = new ArrayList<>();
            int i = 0;
            while (i < n) {
                char c = masked.charAt(i);
                if (c == '(') {
                    int depth = 1;
                    int j = i + 1;
                    while (j < n && depth > 0) {
                        char cj = masked.charAt(j);
                        if (cj == '(') {
                            depth++;
                        } else if (cj == ')') {
                            depth--;
                            if (depth == 0) {
                                break;
                            }
                        }
                        j++;
                    }
                    if (j >= n) {
                        // Unbalanced parens; bail out and append the rest verbatim.
                        result.append(body, i, n);
                        i = n;
                        break;
                    }
                    String branchBody = body.substring(i + 1, j);
                    scope = new HashSet<>(snapshot);
                    String rewritten = rewriteForkBranch(branchBody);
                    branchRewrites.add(new ForkBranchRewrite(rewritten, new HashSet<>(scope)));
                    branchSpans.add(new int[] { i, j + 1 });
                    i = j + 1;
                } else {
                    i++;
                }
            }
            Set<String> droppedByAny = new HashSet<>();
            for (ForkBranchRewrite br : branchRewrites) {
                for (String name : snapshot) {
                    if (br.outgoingScope().contains(name) == false) {
                        droppedByAny.add(name);
                    }
                }
            }
            int cursor = 0;
            for (int b = 0; b < branchRewrites.size(); b++) {
                int[] span = branchSpans.get(b);
                ForkBranchRewrite br = branchRewrites.get(b);
                if (cursor < span[0]) {
                    result.append(body, cursor, span[0]);
                }
                result.append('(').append(br.rewrittenBody());
                for (String name : droppedByAny) {
                    if (br.outgoingScope().contains(name)) {
                        result.append("\n| EVAL ").append(name).append(" = field_extract(").append(name).append(", \"v\")");
                    }
                }
                result.append(')');
                cursor = span[1];
            }
            if (cursor < n) {
                result.append(body, cursor, n);
            }
            Set<String> newScope = new HashSet<>(snapshot);
            newScope.removeAll(droppedByAny);
            scope = newScope;
            return result.toString();
        }

        /**
         * Captures the rewritten body and outgoing flattened scope for one FORK branch. Used
         * by {@link #rewriteForkSegment} to defer assembly of the FORK output until all
         * branches have been rewritten, so the cross-branch type equalization can append
         * {@code field_extract} tails to siblings that retained columns the rebinding branch
         * dropped.
         */
        private record ForkBranchRewrite(String rewrittenBody, Set<String> outgoingScope) {}

        /**
         * Rewrites a single FORK branch body (the content between the parentheses, excluding the
         * surrounding parens themselves). The body is split on top-level pipes the same way the
         * outer walker splits the query, and each segment is dispatched through
         * {@link #processSegment} into a branch-local {@link StringBuilder}. The branch's first
         * segment does not start with {@code |}, so the splitter's range list naturally yields a
         * leading segment with no pipe prefix; subsequent segments retain their pipe.
         */
        private String rewriteForkBranch(String branchBody) {
            StringBuilder branchOut = new StringBuilder();
            for (int[] range : splitIntoSegmentRanges(branchBody)) {
                processSegment(branchBody.substring(range[0], range[1]), branchOut);
            }
            return branchOut.toString();
        }

        /**
         * Appends the follow-up commands that restore the test-expected column names after a
         * {@code STATS}/{@code INLINE STATS} segment whose {@code BY} clause was rewritten by
         * {@link #preprocessStatsByAliasing} to use the {@link #BY_ALIAS_PREFIX} alias form.
         * Emits {@code | RENAME __kw_<name> AS <name>} for every aliased grouping key so the
         * grouping column reverts to its original name (now of type {@code keyword}, since the
         * alias's right-hand side is a {@code field_extract} call).
         * <p>
         * For {@code INLINE STATS} (signalled by {@code preserveInputColumns}) the original
         * input column with the same name survives the aggregation as a {@code flattened}
         * column, so a {@code DROP <name>} is emitted before the rename to remove it. Without
         * this drop the rename would collide with an existing column and the segment would fail
         * verification with a duplicate-column error. For {@code STATS} the input row has been
         * replaced by the aggregation outputs, so no drop is needed.
         * <p>
         * Names are emitted in sorted order to keep the rewritten query deterministic across
         * runs (the aliased list is built by walking the BY clause right-to-left and would
         * otherwise reflect that traversal order, which is fragile and harder to read in test
         * logs).
         */
        private void appendRenameBackForAliasedBy(StringBuilder sink, List<String> aliasedFields, boolean preserveInputColumns) {
            if (aliasedFields.isEmpty()) {
                return;
            }
            List<String> ordered = new ArrayList<>(aliasedFields);
            ordered.sort(Comparator.naturalOrder());
            if (preserveInputColumns) {
                sink.append("\n| DROP ");
                for (int i = 0; i < ordered.size(); i++) {
                    if (i > 0) {
                        sink.append(", ");
                    }
                    sink.append(ordered.get(i));
                }
            }
            sink.append("\n| RENAME ");
            for (int i = 0; i < ordered.size(); i++) {
                if (i > 0) {
                    sink.append(", ");
                }
                sink.append(BY_ALIAS_PREFIX).append(ordered.get(i)).append(" AS ").append(ordered.get(i));
            }
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
        for (String cmd : LITERAL_COMMAND_KEYWORDS) {
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
     * <p>
     * For {@code RENAME old AS new}, the new column always inherits {@code old}'s type, so the
     * scope effect is symmetrical regardless of whether {@code old} was already in scope:
     * <ul>
     *   <li>{@code old} is always removed (it no longer exists as a column post-rename), and</li>
     *   <li>{@code new} is also always removed first (any pre-existing {@code new} column is
     *       shadowed and replaced by {@code old}'s value), then added back only when
     *       {@code old} was a flattened-scope column (because the renamed column inherits
     *       {@code old}'s flattened type).</li>
     * </ul>
     * The previous behavior &mdash; only mutating scope when {@code old} was in scope &mdash;
     * left a flattened {@code new} column in scope after a non-flattened {@code old} had
     * shadowed it (e.g. {@code RENAME emp_no AS last_name} where {@code last_name} was
     * flattened and {@code emp_no} integer), and the trailing recovery would then attempt to
     * wrap the now-integer {@code last_name} and fail verification with "first argument must be
     * flattened, found type integer". See {@code rename.shadowing} in the corpus.
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
                        boolean oldWasInScope = next.remove(oldName);
                        next.remove(newName);
                        if (oldWasInScope) {
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
     * For {@code DISSECT} the body has a single string-literal pattern, so only the first literal
     * is consulted. For {@code GROK} the body may carry a comma-separated list of patterns
     * (e.g. {@code GROK msg "pat1", "pat2"}); every literal is scanned for bindings because a
     * name bound only in the second pattern (such as {@code first_name} in
     * {@code grok.multiPatterns2}) is just as much rebound on a successful match as one bound in
     * the first. Bindings are recognised as {@code %{[modifier]name}} for DISSECT, and as both
     * {@code %{PATTERN:name[:type]}} and Java named-capture groups {@code (?<name>...)} for
     * GROK. Pattern fragments that bind no name (for example a bare {@code %{IP}}, which would
     * bind the column {@code IP}) are intentionally not handled here because the csv-spec corpus
     * consistently uses the {@code :name} form, and matching bare {@code %{PATTERN}} would risk
     * over-aggressive scope mutation against arbitrary user identifiers.
     */
    private static Set<String> updateScopeForDissectOrGrokCommand(String cmd, String segment, CommandPosition pos, Set<String> scope) {
        Set<String> next = new HashSet<>(scope);
        String body = segment.substring(pos.bodyStart());
        if (cmd.equals("DISSECT")) {
            String pattern = extractFirstStringLiteralContent(body);
            if (pattern != null) {
                removeMatches(pattern, DISSECT_NAME_BINDING, next);
            }
            return next;
        }
        for (String pattern : extractAllStringLiteralContents(body)) {
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
     * Returns the scope after an {@code ENRICH} command body. The {@code ENRICH} grammar is
     * {@code ENRICH policy [ON match_field] [WITH new_field [= enrich_field], ...]}; any name
     * that appears as a {@code WITH}-clause left-hand side is rebound by the policy and is
     * therefore removed from the flattened scope (the policy supplies values whose type comes
     * from the policy index, almost never {@code flattened}). The {@code ON} clause is a pure
     * read of an existing column and does not shadow anything; the policy name itself is not a
     * column reference. Without this scope update, tail-end recovery would re-wrap a column
     * the policy has already replaced (see {@code enrich.shadowingSubfields} in the corpus,
     * where the policy returns a {@code text}-typed value and the trailing
     * {@code field_extract(<name>, "v")} fails verification with "found type text").
     * <p>
     * When the {@code WITH} clause contains a wildcard the scope is preserved as-is; the
     * conservative over-approximation matches what {@link #updateScopeForNameOnlyCommand} does
     * for wildcards in {@code KEEP}/{@code DROP}/{@code RENAME}. {@code ENRICH} bodies without a
     * {@code WITH} clause leave the scope unchanged because the policy's added columns are
     * implicit (their names are determined by the policy definition, which is not visible to
     * the rewriter); any subsequent reference to one of those columns will be left unwrapped
     * naturally because the column was never in the flattened scope to begin with.
     */
    private static Set<String> updateScopeForEnrichCommand(String segment, CommandPosition pos, Set<String> scope) {
        Set<String> next = new HashSet<>(scope);
        String body = segment.substring(pos.bodyStart());
        String masked = maskStringsAndComments(body);
        Matcher with = WITH_KEYWORD.matcher(masked);
        if (with.find() == false) {
            return next;
        }
        int withClauseStart = with.end();
        String withClause = body.substring(withClauseStart);
        List<String> pieces = splitTopLevelCommaPieces(withClause);
        if (pieces.stream().anyMatch(s -> s.contains("*"))) {
            return next;
        }
        for (String piece : pieces) {
            Matcher lhs = ENRICH_WITH_LHS.matcher(piece);
            if (lhs.find()) {
                String name = lhs.group(1) != null ? lhs.group(1) : lhs.group(2);
                next.remove(name);
            }
        }
        return next;
    }

    /**
     * Returns the contents (without the surrounding quotes) of the first string literal that
     * appears in {@code text}, or {@code null} if {@code text} has no string literal. Recognises
     * both the standard {@code "..."} form and the triple-quoted {@code """..."""} form.
     * Backslash escapes inside a standard string consume the next character; the triple-quoted
     * form has no escape processing.
     */
    private static String extractFirstStringLiteralContent(String text) {
        int[] range = findStringLiteral(text, 0);
        return range == null ? null : stringLiteralContent(text, range);
    }

    /**
     * Returns the contents of every string literal in {@code text}, in order. Recognises both
     * the standard {@code "..."} form and the triple-quoted {@code """..."""} form, with the
     * same escape handling as {@link #extractFirstStringLiteralContent}. Used by GROK scope
     * tracking, whose body may carry a comma-separated list of patterns (each pattern is its
     * own literal).
     */
    private static List<String> extractAllStringLiteralContents(String text) {
        List<String> contents = new ArrayList<>();
        int from = 0;
        while (true) {
            int[] range = findStringLiteral(text, from);
            if (range == null) {
                return contents;
            }
            contents.add(stringLiteralContent(text, range));
            from = range[1];
        }
    }

    /**
     * Locates the next string literal in {@code text} at or after {@code from}. Returns a two-
     * element array {@code {start, endExclusive}} where {@code start} is the index of the
     * opening quote character and {@code endExclusive} is one past the closing quote sequence
     * (or {@code text.length()} for an unterminated literal at end-of-text). Returns
     * {@code null} when no string literal exists.
     */
    private static int[] findStringLiteral(String text, int from) {
        int n = text.length();
        for (int i = from; i < n; i++) {
            if (text.charAt(i) != '"') {
                continue;
            }
            if (i + 2 < n && text.charAt(i + 1) == '"' && text.charAt(i + 2) == '"') {
                int j = i + 3;
                while (j + 2 < n && (text.charAt(j) != '"' || text.charAt(j + 1) != '"' || text.charAt(j + 2) != '"')) {
                    j++;
                }
                return new int[] { i, j + 2 < n ? j + 3 : n };
            }
            int j = i + 1;
            while (j < n && text.charAt(j) != '"') {
                if (text.charAt(j) == '\\' && j + 1 < n) {
                    j++;
                }
                j++;
            }
            return new int[] { i, j < n ? j + 1 : n };
        }
        return null;
    }

    /**
     * Returns the contents of the literal whose range is {@code range} (as produced by
     * {@link #findStringLiteral}), stripping the surrounding quote characters. Handles both the
     * triple-quoted and standard string forms.
     */
    private static String stringLiteralContent(String text, int[] range) {
        int start = range[0];
        int endExclusive = range[1];
        if (start + 3 <= endExclusive && text.charAt(start) == '"' && text.charAt(start + 1) == '"' && text.charAt(start + 2) == '"') {
            int contentStart = start + 3;
            int contentEnd = endExclusive >= start + 6
                && text.charAt(endExclusive - 3) == '"'
                && text.charAt(endExclusive - 2) == '"'
                && text.charAt(endExclusive - 1) == '"' ? endExclusive - 3 : endExclusive;
            return text.substring(contentStart, contentEnd);
        }
        int contentStart = start + 1;
        int contentEnd = endExclusive > start && text.charAt(endExclusive - 1) == '"' ? endExclusive - 1 : endExclusive;
        return text.substring(contentStart, contentEnd);
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
     * Result of {@link #preprocessStatsByAliasing}.
     *
     * @param segment        the (possibly modified) segment text, with each grouping or aggregate
     *                       piece pre-processed for whichever case in {@link #preprocessStatsByAliasing}
     *                       it falls under
     * @param aliasedFields  names of in-scope grouping keys that the rewriter rewrote with the
     *                       {@link #BY_ALIAS_PREFIX} synthetic alias and that therefore need a
     *                       follow-up {@code RENAME __kw_<name> AS <name>} step (and, for
     *                       {@code INLINE STATS}, a {@code DROP <name>} of the surviving
     *                       flattened input column before the rename) to restore the test-expected
     *                       column name as a keyword column
     * @param shadowedFields names rebound by an explicit user-written alias (in either an
     *                       aggregate or a {@code BY} piece) whose left-hand side names a
     *                       column already in flattened scope. Includes every name in
     *                       {@code aliasedFields}. The caller drops these names from the
     *                       active scope after the segment so subsequent commands and the
     *                       tail-end recovery do not re-wrap a column the user has already
     *                       rebound
     */
    private record StatsByPreprocessing(String segment, List<String> aliasedFields, Set<String> shadowedFields) {}

    /**
     * Pre-processes the body of a {@code STATS} or {@code INLINE STATS} segment so the
     * subsequent field-reference pass and follow-up {@code RENAME}/{@code DROP} step produce a
     * shape the ES|QL analyzer accepts and whose output columns keep the names the csv-spec
     * test expects to assert on. Three kinds of pieces are handled, all detected from the
     * pre-rewrite segment text:
     * <ol>
     *   <li><b>Bare grouping key in flattened scope</b> &mdash; the original
     *       {@code BY ..., field, ...} shape is rewritten to
     *       {@code BY ..., __kw_field = field, ...}. The field-reference pass then wraps the
     *       right-hand side, yielding {@code __kw_field = field_extract(field, "v")} and a
     *       grouping column of type keyword. Without the synthetic prefix the alias's
     *       left-hand side would collide with the input column name and the analyzer would
     *       resolve {@code field} inside the same segment's aggregate arguments to the
     *       keyword-typed BY alias rather than the flattened input attribute, failing
     *       verification with "first argument must be flattened, found type keyword". See
     *       {@code keep.statsOfInteger} in the corpus.</li>
     *   <li><b>Explicit user-written alias whose left-hand side names an in-scope flattened
     *       column</b> ({@code BY field = expr} or {@code STATS field = expr ...}) &mdash; the
     *       LHS is rebinding a column already in scope, which triggers the same analyzer
     *       shadowing bug as case (1) when the wrapped right-hand side then references the
     *       same name. For {@code BY} pieces the LHS is rewritten by inserting the
     *       {@link #BY_ALIAS_PREFIX} prefix before the identifier, producing
     *       {@code __kw_field = expr}; the follow-up {@code RENAME} restores the test-expected
     *       column name. For aggregate pieces the user's alias is left as-is (the analyzer
     *       does not hit the same bug for aggregate aliases that the field-ref pass wraps),
     *       but the LHS is recorded so the active scope drops it after the segment. See
     *       {@code inlinestats.groupingFilterIsAlwaysTrue} (BY shape) and
     *       {@code inlinestats.sortBeforeDoubleInlinestats} (aggregate shape) in the
     *       corpus.</li>
     *   <li><b>Unaliased non-bare expression that mentions an in-scope flattened column</b>
     *       (e.g. {@code BY LEFT(last_name, 1)} or {@code STATS SUM(LENGTH(last_name))})
     *       &mdash; ES|QL auto-names the resulting column from the rewritten expression text,
     *       so a downstream {@code SORT \`LEFT(last_name, 1)\`} or trailing
     *       {@code KEEP SUM(LENGTH(last_name))} from the test header would no longer find the
     *       column once the field-ref pass turns the body into
     *       {@code LEFT(field_extract(last_name, "v"), 1)}. The piece is wrapped in a
     *       {@code \`<original-text>\` = ...} alias so the column survives the rewrite under
     *       the name the test expects (the alias's right-hand side is then wrapped normally
     *       by the field-ref pass). See {@code stats.docsStatsByExpression},
     *       {@code stats.nestedMultiExpressionInGroupingsAndAggs},
     *       {@code inlinestats.maxOfLongByCalculatedKeyword}, and {@code string.aggregate length}
     *       in the corpus.</li>
     * </ol>
     * Pieces that match none of the above are left untouched. Pieces are walked back-to-front
     * so range offsets in {@code segment} stay valid as in-place edits accumulate; aggregate
     * pieces and {@code BY} pieces share the same per-piece classifier (only step 1's
     * bare-name aliasing is restricted to {@code BY} pieces, since a bare in-scope name in an
     * aggregate position is not a meaningful aggregate input).
     */
    private static StatsByPreprocessing preprocessStatsByAliasing(String segment, CommandPosition pos, Set<String> scope) {
        int byOffset = findTopLevelBy(segment, pos.bodyStart());
        int aggBodyStart = pos.bodyStart();
        int aggBodyEnd = byOffset >= 0 ? byOffset : segment.length();
        int byBodyStart = byOffset >= 0 ? byOffset + "BY".length() : -1;

        StringBuilder sb = new StringBuilder(segment);
        List<String> aliased = new ArrayList<>();
        Set<String> shadowed = new HashSet<>();

        if (byBodyStart >= 0) {
            List<int[]> byRanges = splitTopLevelCommaPiecesAsRanges(segment, byBodyStart);
            for (int i = byRanges.size() - 1; i >= 0; i--) {
                rewriteStatsPiece(sb, segment, byRanges.get(i), scope, true, aliased, shadowed);
            }
        }
        List<int[]> aggRanges = splitTopLevelCommaPiecesAsRanges(segment, aggBodyStart, aggBodyEnd);
        for (int i = aggRanges.size() - 1; i >= 0; i--) {
            rewriteStatsPiece(sb, segment, aggRanges.get(i), scope, false, aliased, shadowed);
        }
        return new StatsByPreprocessing(sb.toString(), List.copyOf(aliased), Set.copyOf(shadowed));
    }

    /**
     * Classifies one comma-separated piece of a STATS or INLINE STATS body and applies the
     * corresponding in-place rewrite to {@code sb}. {@code range} indexes into the unmodified
     * {@code segment} text and is only valid because the caller walks pieces back-to-front; an
     * earlier-piece edit cannot affect a later piece's range. {@code isByPiece} distinguishes
     * grouping-key pieces from aggregate pieces; only grouping-key pieces are eligible for the
     * bare-name {@link #BY_ALIAS_PREFIX} aliasing because a bare in-scope name in an aggregate
     * position is not a meaningful aggregate input. See {@link #preprocessStatsByAliasing} for
     * the per-case rewrite contract.
     */
    private static void rewriteStatsPiece(
        StringBuilder sb,
        String segment,
        int[] range,
        Set<String> scope,
        boolean isByPiece,
        List<String> aliased,
        Set<String> shadowed
    ) {
        String pieceText = segment.substring(range[0], range[1]);
        if (isByPiece) {
            Matcher bare = BARE_IDENTIFIER.matcher(pieceText);
            if (bare.matches() && scope.contains(bare.group(1))) {
                int absStart = range[0] + bare.start(1);
                int absEnd = range[0] + bare.end(1);
                String name = bare.group(1);
                sb.replace(absStart, absEnd, BY_ALIAS_PREFIX + name + " = " + name);
                aliased.add(name);
                shadowed.add(name);
                return;
            }
        }
        Matcher alias = STATS_PIECE_ALIAS_LHS.matcher(pieceText);
        if (alias.find()) {
            String aliasName = alias.group(1);
            if (scope.contains(aliasName)) {
                if (isByPiece) {
                    sb.insert(range[0] + alias.start(1), BY_ALIAS_PREFIX);
                    aliased.add(aliasName);
                }
                shadowed.add(aliasName);
            }
            return;
        }
        if (containsInScopeIdentifier(pieceText, scope)) {
            int contentStart = range[0];
            while (contentStart < range[1] && Character.isWhitespace(segment.charAt(contentStart))) {
                contentStart++;
            }
            int contentEnd = range[1];
            while (contentEnd > contentStart && Character.isWhitespace(segment.charAt(contentEnd - 1))) {
                contentEnd--;
            }
            if (contentStart >= contentEnd) {
                return;
            }
            String original = segment.substring(contentStart, contentEnd);
            sb.insert(contentStart, "`" + original + "` = ");
        }
    }

    /**
     * Returns true when {@code text} contains a word-bounded occurrence of any name in
     * {@code scope}, ignoring matches that fall inside a string literal or comment. Used by
     * the STATS/INLINE STATS preprocessor to decide whether an unaliased non-bare piece needs
     * an auto-name-preserving alias because the field-reference pass will wrap at least one of
     * its identifiers.
     */
    private static boolean containsInScopeIdentifier(String text, Set<String> scope) {
        if (scope.isEmpty()) {
            return false;
        }
        String masked = maskStringsAndComments(text);
        Matcher m = ANY_IDENTIFIER.matcher(masked);
        while (m.find()) {
            if (scope.contains(m.group(1))) {
                return true;
            }
        }
        return false;
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
        return splitTopLevelCommaPiecesAsRanges(segment, from, segment.length());
    }

    /**
     * Same as {@link #splitTopLevelCommaPiecesAsRanges(String, int)} but with an explicit
     * exclusive end offset. Used to split aggregate pieces of a STATS body without spilling
     * past the {@code BY} keyword (the BY-clause text would otherwise be treated as a
     * continuation of the last aggregate piece because BY is not a comma boundary).
     */
    private static List<int[]> splitTopLevelCommaPiecesAsRanges(String segment, int from, int toExclusive) {
        String masked = maskStringsAndComments(segment);
        List<int[]> ranges = new ArrayList<>();
        int depth = 0;
        int start = from;
        for (int i = from; i < toExclusive; i++) {
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
        ranges.add(new int[] { start, toExclusive });
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
        List<int[]> qualifiedNameBracketRanges = new ArrayList<>();
        addQualifiedNameBracketRanges(segment, qualifiedNameBracketRanges);

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
                if (overlapsAnyRange(m.start(), m.end(), qualifiedNameBracketRanges)) {
                    skipSink.add(new SkipEvent(SkipSite.QUALIFIED_NAME_BRACKETS, field));
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
     * Records one {@link SkipEvent} per in-scope identifier appearing inside the body of an
     * attribute-only command ({@code MV_EXPAND}, {@code ENRICH}, {@code LOOKUP JOIN}, or
     * {@code INSIST_🐔}). All four grammar slots accept only attribute references (no
     * expressions) on their key position, so the rewriter passes the body through unchanged;
     * this method makes the resulting "we did not wrap field X here" decisions visible to
     * callers. String literals and line comments inside the body are masked first so an
     * identifier that happens to appear inside one of them does not falsely trigger a skip event.
     * <p>
     * For {@code LOOKUP JOIN} the body includes the index pattern that precedes the {@code ON}
     * keyword. Index names are not field references and almost never collide with field names
     * in scope, so scanning the entire body (rather than only the post-{@code ON} portion) is
     * safe; in the rare case of a collision the resulting "skipped a wrap on the index pattern"
     * event is harmless because the rewriter already would not have wrapped there.
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
        SkipSite site = switch (cmd) {
            case "MV_EXPAND" -> SkipSite.MV_EXPAND_ARG;
            case "ENRICH" -> SkipSite.ENRICH_BODY;
            case "LOOKUP JOIN" -> SkipSite.LOOKUP_JOIN_ON;
            case "INSIST_🐔" -> SkipSite.INSIST_BODY;
            default -> throw new AssertionError("recordNameOnlyBodySkips called for unsupported command [" + cmd + "]");
        };
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
     * {@code """..."""} triple-quoted form), every backtick-quoted identifier ({@code `...`}),
     * and every {@code // ...} line comment replaced by spaces of equal length. Offsets of all
     * other characters are preserved.
     * <p>
     * Backticks are masked alongside string literals because the rewriter both consumes
     * masked text (for splitter and find-keyword passes that must not be fooled by content
     * inside a backtick-quoted identifier) and emits backtick-quoted aliases of its own
     * during STATS auto-name preservation, which the field-reference pass must leave intact.
     * The grammar treats a backtick-quoted identifier as a single atomic token whose contents
     * are never field references, so masking them is safe across every caller.
     */
    private static String maskStringsAndComments(String query) {
        char[] out = query.toCharArray();
        int i = 0;
        int n = out.length;
        while (i < n) {
            int next = endOfMaskedSpan(query, i);
            if (next > i) {
                for (int k = i; k < next; k++) {
                    out[k] = ' ';
                }
                i = next;
            } else {
                i++;
            }
        }
        return new String(out);
    }

    /**
     * Same as {@link #maskStringsAndComments} but records the [start, end) ranges of each
     * masked span (string literal, backtick-quoted identifier, or line comment) directly into
     * {@code ranges}. Used by {@link #rewriteFieldRefs} to build the protected-range list
     * without re-scanning.
     */
    private static void addStringAndCommentRanges(String query, List<int[]> ranges) {
        int i = 0;
        int n = query.length();
        while (i < n) {
            int next = endOfMaskedSpan(query, i);
            if (next > i) {
                ranges.add(new int[] { i, next });
                i = next;
            } else {
                i++;
            }
        }
    }

    /**
     * Returns the exclusive end offset of the masked span beginning at {@code i} in
     * {@code query}, or {@code i} when no span begins there. A masked span is one of:
     * <ul>
     *   <li>a triple-quoted string literal ({@code """..."""} &mdash; no escape processing);</li>
     *   <li>a standard string literal ({@code "..."} &mdash; backslash escapes consume the
     *       next character);</li>
     *   <li>a backtick-quoted identifier ({@code `...`} &mdash; no escape processing in the
     *       grammar, and no backtick has appeared in any csv-spec column name in the corpus,
     *       so backslashes are treated as ordinary characters here); or</li>
     *   <li>a {@code //} line comment that runs to the next newline.</li>
     * </ul>
     * Anything else returns {@code i}, signalling that the caller should advance one
     * character. The end offset of an unterminated span is the end of {@code query}; never
     * past it.
     */
    private static int endOfMaskedSpan(String query, int i) {
        int n = query.length();
        if (i >= n) {
            return i;
        }
        char c = query.charAt(i);
        if (c == '"') {
            if (i + 2 < n && query.charAt(i + 1) == '"' && query.charAt(i + 2) == '"') {
                int j = i + 3;
                while (j + 2 < n && (query.charAt(j) != '"' || query.charAt(j + 1) != '"' || query.charAt(j + 2) != '"')) {
                    j++;
                }
                return Math.min(j + 3, n);
            }
            int j = i + 1;
            while (j < n && query.charAt(j) != '"') {
                if (query.charAt(j) == '\\' && j + 1 < n) {
                    j++;
                }
                j++;
            }
            return Math.min(j + 1, n);
        }
        if (c == '`') {
            int j = i + 1;
            while (j < n && query.charAt(j) != '`') {
                j++;
            }
            return Math.min(j + 1, n);
        }
        if (c == '/' && i + 1 < n && query.charAt(i + 1) == '/') {
            int j = i;
            while (j < n && query.charAt(j) != '\n') {
                j++;
            }
            return j;
        }
        return i;
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

    /**
     * Adds the contents of every {@code [<index>].[<field>]} qualified-name reference to
     * {@code ranges}. ES|QL's parser accepts only a bare unquoted identifier inside each bracket
     * pair (see {@code qualifiedName} / {@code qualifiedNamePattern} in the grammar), so wrapping
     * a name there in {@code field_extract(...)} would not parse. The captured group spans cover
     * the bracket interiors only, leaving the brackets themselves outside the protected range so
     * that the rest of the segment can still be matched normally on either side.
     * <p>
     * String literals and line comments are masked first so a literal that happens to contain
     * {@code "].["} (for example a sample query embedded in a {@code KQL(...)} argument) cannot
     * trigger spurious bracket protection.
     */
    private static void addQualifiedNameBracketRanges(String query, List<int[]> ranges) {
        String masked = maskStringsAndComments(query);
        Matcher m = QUALIFIED_NAME_BRACKETS.matcher(masked);
        while (m.find()) {
            int qualifierStart = m.start(1);
            int qualifierEnd = m.end(1);
            int nameStart = m.start(2);
            int nameEnd = m.end(2);
            if (qualifierEnd > qualifierStart && overlapsAnyRange(qualifierStart, qualifierEnd, ranges) == false) {
                ranges.add(new int[] { qualifierStart, qualifierEnd });
            }
            if (nameEnd > nameStart && overlapsAnyRange(nameStart, nameEnd, ranges) == false) {
                ranges.add(new int[] { nameStart, nameEnd });
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
