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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rewrites a csv-spec ES|QL query so that every reference to a known keyword field is wrapped in
 * a call to {@code field_extract(<field>, "<wrapperSubKey>")}.
 * <p>
 * This is used by the keyword&rarr;flattened POC ({@link CsvFlattenedKeywordIT}) to exercise
 * {@code field_extract} in every place where a keyword field would otherwise appear &mdash; the
 * companion {@link KeywordToFlattenedTransformer} rewrites the index mapping and document source
 * so the field is stored as a {@code flattened} object {@code {"v": <original keyword value>}},
 * and this rewriter inserts the matching {@code field_extract(field, "v")} extraction on the
 * query side.
 *
 * <h2>What is rewritten</h2>
 * For each keyword field {@code F} in the {@code keywordFields} set, every standalone identifier
 * occurrence of {@code F} in the query is replaced with {@code field_extract(F, "<wrapperSubKey>")}.
 * "Standalone" means the identifier is not part of a longer dotted/identifier sequence
 * (so {@code message} does not match inside {@code message_id} or {@code other.message}).
 *
 * <h2>What is intentionally not rewritten</h2>
 * <ul>
 *   <li>Identifiers inside string literals ({@code "..."} and {@code """..."""}).</li>
 *   <li>Identifiers inside line comments ({@code // ...}).</li>
 *   <li>Identifiers inside the body of name-only commands &mdash; {@code FROM}, {@code KEEP},
 *       {@code DROP}, {@code RENAME} &mdash; whose grammar takes column names rather than
 *       expressions, so wrapping them in a function call would be a syntax error.</li>
 *   <li>The left-hand side of an assignment ({@code name = expr}, distinguished from the comparison
 *       operator {@code ==}). This covers {@code EVAL}, {@code STATS} aggregates, and
 *       {@code STATS ... BY} pairs, where the LHS introduces a new column rather than referencing
 *       an existing one.</li>
 * </ul>
 *
 * <h2>Known limitations of the POC</h2>
 * The rewriter is regex-driven and does not parse the ES|QL grammar. In particular it does not
 * handle backtick-quoted identifiers, named arguments inside function calls that share a syntax
 * with assignments, or commands whose grammar accepts an attribute but not an arbitrary expression
 * (for example {@code DISSECT &lt;field&gt; "&lt;pattern&gt;"} or {@code LOOKUP JOIN ... ON &lt;field&gt;}).
 * Queries that hit those cases will surface as test failures, which is the intended way to
 * inventory ES|QL surfaces that {@code field_extract} cannot currently substitute for.
 */
public final class EsqlQueryKeywordFieldRewriter {

    /** Name of the ES|QL function used to extract a sub-field from a {@code flattened} value. */
    public static final String FIELD_EXTRACT_FUNCTION = "field_extract";

    /**
     * Result of {@link #rewrite(String, Set, String)}.
     *
     * @param rewrittenQuery       the rewritten query string, or the original query if no field references were rewritten
     * @param modified             {@code true} iff at least one keyword field reference was rewritten
     * @param rewrittenFieldNames  the set of keyword field names that were actually wrapped at least once
     */
    public record RewriteResult(String rewrittenQuery, boolean modified, Set<String> rewrittenFieldNames) {}

    /**
     * Pattern that recognizes the start of a name-only command segment. A name-only command's body
     * is a list of column names rather than expressions, so we must not wrap any identifier inside
     * its body in a function call.
     * <p>
     * Capture group 1 is the command keyword, present so the matcher exposes the offset where the
     * body begins (via {@link Matcher#end(int)} on group 1).
     */
    private static final Pattern NAME_ONLY_COMMAND_START = Pattern.compile(
        "(?:\\||^)\\s*(KEEP|DROP|RENAME|FROM)\\b",
        Pattern.CASE_INSENSITIVE
    );

    /**
     * Pattern matching the left-hand side of an assignment: an identifier (possibly dotted) followed
     * by optional whitespace, a single {@code =}, and a negative lookahead to exclude the comparison
     * operator {@code ==}.
     */
    private static final Pattern ASSIGNMENT_LHS = Pattern.compile("(?<![a-zA-Z0-9_.])([a-zA-Z_][a-zA-Z0-9_.]*)\\s*=(?!=)");

    /**
     * Lookaround character class that defines what counts as "part of an identifier" for purposes of
     * deciding whether a candidate match is a complete identifier or only a partial substring of one.
     * Includes letters, digits, underscore, and dot &mdash; so {@code city.name} is treated as a
     * single dotted identifier and does not match inside {@code city.name.suffix} or
     * {@code prefix.city.name}.
     */
    private static final String IDENTIFIER_BOUNDARY_CLASS = "[a-zA-Z0-9_.]";

    private EsqlQueryKeywordFieldRewriter() {}

    /**
     * Rewrites {@code query} so that each occurrence of a name in {@code keywordFields} that is a
     * standalone identifier in an expression context becomes
     * {@code field_extract(<name>, "<wrapperSubKey>")}.
     * <p>
     * If {@code keywordFields} is empty, or no occurrence is found in an expression context, the
     * original query is returned and {@link RewriteResult#modified()} is {@code false}.
     */
    public static RewriteResult rewrite(String query, Set<String> keywordFields, String wrapperSubKey) {
        if (keywordFields.isEmpty()) {
            return new RewriteResult(query, false, Set.of());
        }

        List<int[]> protectedRanges = computeProtectedRanges(query);

        record CandidateMatch(int start, int end, String field) {}
        List<CandidateMatch> matches = new ArrayList<>();
        Set<String> rewrittenFieldNames = new HashSet<>();

        for (String field : keywordFields) {
            Pattern p = Pattern.compile(
                "(?<!" + IDENTIFIER_BOUNDARY_CLASS + ")" + Pattern.quote(field) + "(?!" + IDENTIFIER_BOUNDARY_CLASS + ")"
            );
            Matcher m = p.matcher(query);
            while (m.find()) {
                if (overlapsAnyRange(m.start(), m.end(), protectedRanges)) {
                    continue;
                }
                matches.add(new CandidateMatch(m.start(), m.end(), field));
            }
        }

        if (matches.isEmpty()) {
            return new RewriteResult(query, false, Set.of());
        }

        // Apply edits from right to left so each replacement does not shift the offsets of later edits.
        matches.sort(Comparator.comparingInt(CandidateMatch::start).reversed());

        StringBuilder result = new StringBuilder(query);
        for (CandidateMatch match : matches) {
            String wrapped = FIELD_EXTRACT_FUNCTION + "(" + match.field() + ", \"" + wrapperSubKey + "\")";
            result.replace(match.start(), match.end(), wrapped);
            rewrittenFieldNames.add(match.field());
        }

        return new RewriteResult(result.toString(), true, Set.copyOf(rewrittenFieldNames));
    }

    /**
     * Builds the set of character ranges that the rewriter must skip: string literals, line
     * comments, name-only command bodies, and assignment targets. The returned ranges may overlap
     * and are not in any particular order; callers should use {@link #overlapsAnyRange} for
     * membership tests.
     */
    private static List<int[]> computeProtectedRanges(String query) {
        List<int[]> ranges = new ArrayList<>();
        addStringAndCommentRanges(query, ranges);
        addNameOnlyCommandSegmentRanges(query, ranges);
        addAssignmentTargetRanges(query, ranges);
        return ranges;
    }

    /**
     * Scans {@code query} character by character to find all double-quoted string literals
     * (including the triple-quoted {@code """..."""} variant) and {@code //} line comments, and
     * adds the {@code [start, end)} range of each one to {@code ranges}.
     * <p>
     * The string scanner only recognizes the backslash escape inside the regular {@code "..."} form
     * &mdash; ES|QL's triple-quoted form treats backslashes literally. That matches the lexer
     * grammar closely enough for this POC.
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
     * Adds the body of every {@code FROM}/{@code KEEP}/{@code DROP}/{@code RENAME} command to
     * {@code ranges}. The body runs from just after the command keyword to the next top-level pipe
     * (a pipe character that is not itself inside a previously-recorded protected range).
     */
    private static void addNameOnlyCommandSegmentRanges(String query, List<int[]> ranges) {
        Matcher m = NAME_ONLY_COMMAND_START.matcher(query);
        while (m.find()) {
            // Defend against the keyword appearing inside a string we already protected.
            if (overlapsAnyRange(m.start(1), m.end(1), ranges)) {
                continue;
            }
            int afterCommand = m.end(1);
            int end = findNextTopLevelPipe(query, afterCommand, ranges);
            ranges.add(new int[] { afterCommand, end });
        }
    }

    /**
     * Adds the identifier on the left-hand side of every assignment ({@code name = ...}) to
     * {@code ranges}. The {@code (?!=)} lookahead in {@link #ASSIGNMENT_LHS} prevents the comparison
     * operator {@code ==} from being treated as an assignment.
     */
    private static void addAssignmentTargetRanges(String query, List<int[]> ranges) {
        Matcher m = ASSIGNMENT_LHS.matcher(query);
        while (m.find()) {
            int start = m.start(1);
            int end = m.end(1);
            if (overlapsAnyRange(start, end, ranges)) {
                continue;
            }
            ranges.add(new int[] { start, end });
        }
    }

    /**
     * Returns the offset of the next pipe character at or after {@code from} that is not inside any
     * range in {@code existingRanges}, or the length of {@code query} if no such pipe exists. Used
     * to locate the end of a name-only command body without being fooled by pipe characters inside
     * string literals.
     */
    private static int findNextTopLevelPipe(String query, int from, List<int[]> existingRanges) {
        for (int i = from; i < query.length(); i++) {
            if (query.charAt(i) == '|' && overlapsAnyRange(i, i + 1, existingRanges) == false) {
                return i;
            }
        }
        return query.length();
    }

    /**
     * Returns {@code true} if the half-open interval {@code [start, end)} overlaps any of
     * {@code ranges}.
     */
    private static boolean overlapsAnyRange(int start, int end, List<int[]> ranges) {
        for (int[] r : ranges) {
            if (start < r[1] && end > r[0]) {
                return true;
            }
        }
        return false;
    }
}
