/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the {@link CsvTestsDataLoader.TestDataset} entries that a csv-spec ES|QL query actually
 * reads from, by parsing the query's source command(s) and lookup-join clauses.
 * <p>
 * {@link CsvFlattenedKeywordIT} uses this to scope the keyword-path set passed to
 * {@link AstKeywordFieldRewriter} to only the fields that are {@code keyword} in the
 * specific indices the query touches &mdash; instead of the union of every keyword path across
 * every dataset, which produces cross-dataset false positives (e.g. wrapping {@code id} because it
 * is {@code keyword} in {@code apps}, even when the query is reading {@code employees} where
 * {@code id} is {@code long}).
 *
 * <h2>What is recognized</h2>
 * <ul>
 *   <li>{@code FROM &lt;patterns&gt; [METADATA ...]} at the start of the query, after a top-level
 *       pipe, or at the start of a parenthesised sub-pipeline ({@code FROM ... , (FROM ... | ...)}
 *       inside a {@code FROM} list). {@code &lt;patterns&gt;} is a comma-separated list of index
 *       names, optional with a suffix wildcard ({@code sample_data*}) or the universal pattern
 *       ({@code *}). Exclude-form patterns ({@code -employees*}) are recognized and resolved
 *       against their bare form; widening the path set in those rare cases is a safe
 *       over-approximation.</li>
 *   <li>{@code TS &lt;patterns&gt; [METADATA ...]} &mdash; same shape as {@code FROM}, for
 *       time-series queries.</li>
 *   <li>{@code LOOKUP JOIN &lt;index&gt; ON ...} &mdash; the right-hand index is included so its
 *       keyword paths are also in scope.</li>
 * </ul>
 *
 * <h2>What is intentionally not handled</h2>
 * <ul>
 *   <li>Backtick-quoted index names. The regex assumes patterns are composed of identifier
 *       characters plus {@code *}, {@code -}, and {@code .}.</li>
 *   <li>{@code ENRICH &lt;policy&gt;} clauses. Enrich policies reference enrich indices (not
 *       datasets in {@link CsvTestsDataLoader#CSV_DATASET}); the policy's source dataset is loaded
 *       separately by {@link CsvIT} and does not need to feed into this scoping.</li>
 * </ul>
 *
 * String literals ({@code "..."} and {@code """..."""}) and line comments ({@code // ...}) are
 * masked before regex matching so a keyword that happens to appear inside one of them does not
 * trigger a spurious match.
 */
public final class EsqlQueryDatasetResolver {

    /**
     * Captures the index list after a {@code FROM} or {@code TS} source command at the start of
     * the query, right after a top-level pipe, or right after an opening parenthesis (the latter
     * being how csv-spec subqueries appear: {@code FROM a, (FROM b | LIMIT 5)}). The body extends
     * up to (but excluding) an optional {@code METADATA} clause, the next pipe, the next paren of
     * either kind, or end-of-input &mdash; so a subquery's index list is captured without
     * spilling into the inner pipeline that follows the first {@code |}.
     * <p>
     * Capture group 1 holds the raw, possibly whitespace-padded index list; the caller splits it
     * on commas.
     */
    private static final Pattern SOURCE_COMMAND = Pattern.compile(
        "(?:^|\\||\\()\\s*(?:FROM|TS)\\s+([^|()]+?)(?=\\s+METADATA\\b|\\s*[|()]|\\s*$)",
        Pattern.CASE_INSENSITIVE
    );

    /**
     * Captures the right-hand index of a {@code LOOKUP JOIN &lt;index&gt; ON ...} clause. The
     * grammar requires {@code ON} to follow the index, which lets us bound the match without
     * worrying about trailing whitespace or pipes.
     * <p>
     * Capture group 1 is the index identifier itself; we accept the same character class as the
     * source-command body so dotted or wildcarded forms degrade gracefully even though
     * {@code LOOKUP JOIN} actually requires a single concrete index.
     */
    private static final Pattern LOOKUP_JOIN_INDEX = Pattern.compile(
        "\\bLOOKUP\\s+JOIN\\s+([\\w.*\\-]+)\\s+ON\\b",
        Pattern.CASE_INSENSITIVE
    );

    /**
     * Captures both the right-hand index <em>and</em> the body of the {@code ON} clause of a
     * {@code LOOKUP JOIN &lt;index&gt; ON &lt;body&gt;} command. The body extends from just after
     * {@code ON} up to (but excluding) the next pipe, the next closing parenthesis, or end of
     * input &mdash; which mirrors how the rewriter splits segments and how a paren ends a
     * sub-pipeline.
     * <p>
     * Capture group 1 is the index identifier; group 2 is the raw body text, possibly with
     * leading and trailing whitespace. The grammar accepts arbitrary boolean expressions inside
     * the body, but the parser then constrains it to either a comma-separated list of bare
     * unqualified attributes (field-based mode) or a single binary-comparison expression
     * (expression-based mode); see
     * {@code LogicalPlanBuilder.processFieldBasedJoin / processExpressionBasedJoin}.
     */
    private static final Pattern LOOKUP_JOIN_INDEX_AND_BODY = Pattern.compile(
        "\\bLOOKUP\\s+JOIN\\s+([\\w.*\\-]+)\\s+ON\\s+([^|()]+?)(?=\\s*[|()]|\\s*$)",
        Pattern.CASE_INSENSITIVE
    );

    /**
     * Matches an unqualified ES|QL identifier as a standalone token. Used to extract candidate
     * field names from a {@code LOOKUP JOIN ... ON ...} body without trying to parse the body as
     * an expression. Reserved keywords ({@code AND}, {@code OR}, {@code NOT}, {@code TRUE},
     * {@code FALSE}, {@code NULL}, ES|QL casts) are filtered out by
     * {@link #LOOKUP_JOIN_BODY_KEYWORDS}.
     */
    private static final Pattern IDENTIFIER_TOKEN = Pattern.compile("\\b[A-Za-z_][A-Za-z0-9_.]*\\b");

    /**
     * Tokens that {@link #IDENTIFIER_TOKEN} would otherwise accept but which are not field names
     * inside a {@code LOOKUP JOIN ... ON ...} body. Both casing variants are listed so a
     * case-insensitive {@code Set::contains} on uppercased tokens covers them all.
     */
    private static final Set<String> LOOKUP_JOIN_BODY_KEYWORDS = Set.of(
        "AND",
        "OR",
        "NOT",
        "TRUE",
        "FALSE",
        "NULL",
        "IS",
        "IN",
        "LIKE",
        "RLIKE"
    );

    /**
     * Minimum prefix length for a wildcard pattern such as {@code abc*} to participate in
     * resolution. Matches the same guard {@link CsvIT} enforces &mdash; below this threshold the
     * pattern would match too broadly to be useful, and csv-spec authors are expected to use a
     * longer prefix.
     */
    private static final int MINIMUM_WILDCARD_PREFIX_LENGTH = 3;

    private EsqlQueryDatasetResolver() {}

    /**
     * Returns the set of raw index patterns referenced by {@code query}. Patterns are returned
     * verbatim &mdash; including suffix wildcards and exclude prefixes &mdash; in the order they
     * are encountered, deduplicated.
     */
    public static Set<String> extractIndexPatterns(String query) {
        String masked = maskStringsAndComments(query);
        Set<String> patterns = new LinkedHashSet<>();

        Matcher source = SOURCE_COMMAND.matcher(masked);
        while (source.find()) {
            String indexList = query.substring(source.start(1), source.end(1));
            for (String part : indexList.split(",")) {
                String trimmed = part.trim();
                if (trimmed.isEmpty() == false) {
                    patterns.add(trimmed);
                }
            }
        }

        Matcher join = LOOKUP_JOIN_INDEX.matcher(masked);
        while (join.find()) {
            String index = query.substring(join.start(1), join.end(1)).trim();
            if (index.isEmpty() == false) {
                patterns.add(index);
            }
        }

        return Set.copyOf(patterns);
    }

    /**
     * Resolves a set of index patterns to the matching {@link CsvTestsDataLoader.TestDataset}
     * entries from {@code datasetsByName}. The resolution mirrors {@code CsvIT.loadIndices}
     * &mdash; exact match, suffix-wildcard prefix match, or universal {@code *}. Unknown exact
     * patterns are silently skipped; resolution is best-effort so that an unsupported pattern shape
     * only narrows the scoped keyword-path set, never throws.
     * <p>
     * Exclude-form patterns ({@code -employees*}) are resolved as their bare form. The resulting
     * set is therefore an over-approximation of what ES|QL actually queries; that is intentional
     * because over-scoping the keyword set is the safe direction (it preserves coverage at the
     * cost of an occasional cross-dataset wrap, whereas under-scoping would silently miss real
     * keyword references).
     */
    public static Set<CsvTestsDataLoader.TestDataset> resolveDatasets(
        Set<String> patterns,
        Map<String, CsvTestsDataLoader.TestDataset> datasetsByName
    ) {
        Set<CsvTestsDataLoader.TestDataset> result = new LinkedHashSet<>();
        for (String raw : patterns) {
            String pattern = raw.startsWith("-") ? raw.substring(1) : raw;
            if (pattern.contains("*")) {
                if (pattern.equals("*")) {
                    result.addAll(datasetsByName.values());
                    continue;
                }
                if (pattern.endsWith("*") == false) {
                    continue;
                }
                String prefix = pattern.substring(0, pattern.length() - 1);
                if (prefix.length() < MINIMUM_WILDCARD_PREFIX_LENGTH) {
                    continue;
                }
                for (CsvTestsDataLoader.TestDataset d : datasetsByName.values()) {
                    if (d.indexName().startsWith(prefix)) {
                        result.add(d);
                    }
                }
            } else {
                CsvTestsDataLoader.TestDataset d = datasetsByName.get(pattern);
                if (d != null) {
                    result.add(d);
                }
            }
        }
        return result;
    }

    /**
     * Convenience that combines {@link #extractIndexPatterns} and {@link #resolveDatasets}: returns
     * the {@code TestDataset} entries that {@code query} reads from.
     */
    public static Set<CsvTestsDataLoader.TestDataset> resolveDatasetsForQuery(
        String query,
        Map<String, CsvTestsDataLoader.TestDataset> datasetsByName
    ) {
        return resolveDatasets(extractIndexPatterns(query), datasetsByName);
    }

    /**
     * Extracts every {@code LOOKUP JOIN &lt;target&gt; ON &lt;body&gt;} clause from {@code query}
     * and returns a map from each lookup-target index to the set of bare-attribute names that
     * appear anywhere inside its {@code ON} body.
     * <p>
     * The extraction is intentionally over-approximate: in expression-based mode the body may
     * mention attributes from both the left and right sides (e.g. {@code name_left == name_str}),
     * and the regex pass cannot tell which side a name belongs to. That is acceptable for the
     * intended use, which is to feed the resulting names into
     * {@link KeywordToFlattenedTransformer#transformMapping(String, Set)} as
     * {@code excludedPaths} for the lookup-target's mapping. The transformer only acts on names
     * that actually exist as keyword leaves in the mapping, so an extra name that is not on the
     * lookup-target side is silently ignored. Reserved keywords ({@code AND}, {@code OR},
     * {@code NOT}, etc.) are filtered out via {@link #LOOKUP_JOIN_BODY_KEYWORDS}.
     * <p>
     * The result is keyed by the raw index pattern as it appears in the query (e.g.
     * {@code languages_lookup}, {@code clientips_lookup}); callers resolve that to a
     * {@link CsvTestsDataLoader.TestDataset} via {@link #resolveDatasets}.
     */
    public static Map<String, Set<String>> extractLookupJoinTargets(String query) {
        String masked = maskStringsAndComments(query);
        Map<String, Set<String>> targets = new LinkedHashMap<>();
        Matcher m = LOOKUP_JOIN_INDEX_AND_BODY.matcher(masked);
        while (m.find()) {
            String index = query.substring(m.start(1), m.end(1)).trim();
            if (index.isEmpty()) {
                continue;
            }
            String body = masked.substring(m.start(2), m.end(2));
            Set<String> fields = extractAttributeNamesFromJoinBody(body);
            if (fields.isEmpty()) {
                continue;
            }
            targets.computeIfAbsent(index, k -> new LinkedHashSet<>()).addAll(fields);
        }
        return targets;
    }

    /**
     * Returns every identifier-shaped token in {@code body} that is not an ES|QL reserved word
     * recognised by {@link #LOOKUP_JOIN_BODY_KEYWORDS}. Identifier shape matches the unqualified
     * form accepted by ES|QL ({@code [A-Za-z_][A-Za-z0-9_.]*}); backtick-quoted identifiers are
     * not handled.
     */
    private static Set<String> extractAttributeNamesFromJoinBody(String body) {
        Set<String> names = new LinkedHashSet<>();
        Matcher m = IDENTIFIER_TOKEN.matcher(body);
        while (m.find()) {
            String token = m.group();
            if (LOOKUP_JOIN_BODY_KEYWORDS.contains(token.toUpperCase(java.util.Locale.ROOT))) {
                continue;
            }
            names.add(token);
        }
        return names;
    }

    /**
     * Returns a copy of {@code query} with every double-quoted string literal (including the
     * {@code """..."""} triple-quoted form) and every {@code // ...} line comment replaced by
     * spaces of equal length. The positions of all other characters are preserved, so matcher
     * groups computed against the masked string index correctly into the original.
     * <p>
     * The masking is conservative for the {@code """..."""} form: backslash escapes inside it are
     * left untouched (ES|QL's triple-quoted form treats backslashes literally), and an unterminated
     * literal at end-of-input is masked to end-of-input as well.
     */
    static String maskStringsAndComments(String query) {
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
}
