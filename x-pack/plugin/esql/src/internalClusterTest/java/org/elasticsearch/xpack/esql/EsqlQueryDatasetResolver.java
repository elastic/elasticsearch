/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

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
 * {@link EsqlQueryKeywordFieldRewriter} to only the fields that are {@code keyword} in the
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
     * Returns a copy of {@code query} with every double-quoted string literal (including the
     * {@code """..."""} triple-quoted form) and every {@code // ...} line comment replaced by
     * spaces of equal length. The positions of all other characters are preserved, so matcher
     * groups computed against the masked string index correctly into the original.
     * <p>
     * The masking is conservative for the {@code """..."""} form: backslash escapes inside it are
     * left untouched (ES|QL's triple-quoted form treats backslashes literally), and an unterminated
     * literal at end-of-input is masked to end-of-input as well. Both behaviours match the rewriter
     * in {@link EsqlQueryKeywordFieldRewriter}.
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
