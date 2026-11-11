/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.ChangePointGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.DedupGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.InlineStatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.LimitByGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.LimitGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.MvExpandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.StatsGenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.needsQuoting;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.quote;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomName;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.isUnmappedFieldsEnabled;
import static org.elasticsearch.xpack.esql.generator.command.source.FromGenerator.isFromSource;

/**
 * Generates random full-text search expressions (match/match_phrase/qstr/kql/:).
 */
public final class FullTextFunctionGenerator {

    private FullTextFunctionGenerator() {}

    private static final Set<String> QSTR_KQL_SAFE_COMMANDS = Set.of("from", "where", "sort");

    /**
     * Commands after which full-text expressions (match, qstr, kql, etc.) are not allowed.
     */
    private static final Set<String> FULL_TEXT_FORBIDDEN_AFTER_COMMANDS = Set.of(
        LimitGenerator.LIMIT,
        LimitByGenerator.LIMIT_BY,
        StatsGenerator.STATS,
        InlineStatsGenerator.INLINE_STATS,
        ChangePointGenerator.CHANGE_POINT,
        MvExpandGenerator.MV_EXPAND,
        DedupGenerator.DEDUP
    );

    private static boolean isFullTextAllowed(List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null || previousCommands.isEmpty()) {
            return false;
        }
        if (isFromSource(previousCommands.get(0)) == false) {
            return false;
        }
        for (CommandGenerator.CommandDescription cmd : previousCommands) {
            if (FULL_TEXT_FORBIDDEN_AFTER_COMMANDS.contains(cmd.commandName())) {
                return false;
            }
        }
        return true;
    }

    private static boolean isQstrKqlAllowed(List<CommandGenerator.CommandDescription> previousCommands) {
        if (isFullTextAllowed(previousCommands) == false) {
            return false;
        }
        for (CommandGenerator.CommandDescription cmd : previousCommands) {
            if (QSTR_KQL_SAFE_COMMANDS.contains(cmd.commandName()) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when MATCH / MATCH_PHRASE / {@code :} may target schema columns (mapped or runtime).
     * Requires a FROM source that did not set {@code unmapped_fields="nullify"}.
     * Unlike {@link #indexFieldColumns}, this stays true when every mapped column has been dropped.
     */
    public static boolean isMatchOnColumnsAllowed(List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null || previousCommands.isEmpty()) {
            return false;
        }
        return isFromSource(previousCommands.get(0)) && isUnmappedFieldsEnabled(previousCommands) == false;
    }

    /**
     * Returns the subset of columns that are index-mapped (originate from the actual index mapping).
     * Returns {@code null} when the information is unavailable (e.g. non-FROM source), when
     * {@code SET unmapped_fields="nullify"} makes even columns of a FROM source resolve as non-index-mapped,
     * or when no index-mapped column remains.
     */
    public static List<Column> indexFieldColumns(List<Column> columns, List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null || previousCommands.isEmpty()) {
            return null;
        }
        if (isFromSource(previousCommands.get(0)) == false || isUnmappedFieldsEnabled(previousCommands)) {
            return null;
        }
        List<Column> result = columns.stream().filter(Column::indexMapped).toList();
        return result.isEmpty() ? null : result;
    }

    private static final Set<String> MATCH_FIELD_TYPES = Set.of(
        "keyword",
        "text",
        "boolean",
        "date",
        "datetime",
        "double",
        "integer",
        "ip",
        "long",
        "unsigned_long",
        "version"
    );
    /** Types a non-index-mapped (runtime) full-text search can run on. Also the field types MATCH_PHRASE accepts. */
    private static final Set<String> RUNTIME_FULL_TEXT_TYPES = Set.of("keyword", "text");
    private static final Set<String> MATCH_PHRASE_FIELD_TYPES = RUNTIME_FULL_TEXT_TYPES;

    /**
     * MATCH and {@code :} may target an index-mapped field of a supported type, or a runtime keyword/text column.
     */
    public static boolean isMatchColumn(Column column) {
        return MATCH_FIELD_TYPES.contains(column.type()) && (column.indexMapped() || RUNTIME_FULL_TEXT_TYPES.contains(column.type()));
    }

    /**
     * MATCH_PHRASE may target any keyword or text column, mapped or runtime.
     */
    public static boolean isMatchPhraseColumn(Column column) {
        return MATCH_PHRASE_FIELD_TYPES.contains(column.type());
    }

    /**
     * Analyzer-style options are legal on Lucene-mapped fields and on runtime TEXT, not on runtime keyword.
     */
    public static boolean optionsAllowed(Column column) {
        return column.indexMapped() || "text".equals(column.type());
    }

    private static final String[] SAMPLE_QUERY_WORDS = {
        "test",
        "hello",
        "world",
        "data",
        "search",
        "quick",
        "brown",
        "fox",
        "ring",
        "return" };

    public static String randomQueryWord() {
        return randomFrom(SAMPLE_QUERY_WORDS);
    }

    private static String maybeOptions(String[][] optionPool) {
        if (randomIntBetween(0, 4) > 0) {
            return "";
        }
        int count = Math.min(randomIntBetween(1, 2), optionPool.length);
        Set<Integer> usedIndices = new HashSet<>();
        StringBuilder sb = new StringBuilder(", {");
        int added = 0;
        for (int i = 0; i < count; i++) {
            int idx = randomIntBetween(0, optionPool.length - 1);
            if (usedIndices.add(idx) == false) {
                continue;
            }
            String[] entry = optionPool[idx];
            String name = entry[0];
            String value = entry[randomIntBetween(1, entry.length - 1)];
            if (added > 0) {
                sb.append(", ");
            }
            sb.append("\"").append(name).append("\": ").append(value);
            added++;
        }
        sb.append("}");
        return sb.toString();
    }

    private static final String[][] MATCH_OPTIONS = {
        { "operator", "\"AND\"", "\"OR\"" },
        { "fuzziness", "\"AUTO\"", "1", "2" },
        { "lenient", "true", "false" },
        { "boost", "1.0", "2.5" },
        { "zero_terms_query", "\"none\"", "\"all\"" }, };

    private static final String[][] MATCH_PHRASE_OPTIONS = {
        { "slop", "0", "1", "2" },
        { "boost", "1.0", "2.5" },
        { "zero_terms_query", "\"none\"", "\"all\"" }, };

    private static final String[][] QSTR_OPTIONS = {
        { "default_operator", "\"OR\"", "\"AND\"" },
        { "lenient", "true", "false" },
        { "fuzziness", "\"AUTO\"", "1" },
        { "boost", "1.0", "2.5" },
        { "phrase_slop", "1", "2", "3" },
        { "analyze_wildcard", "true", "false" }, };

    private static final String[][] KQL_OPTIONS = { { "case_insensitive", "true", "false" }, { "boost", "1.0", "2.5" }, };

    /**
     * Generates a {@code match(field, "query")} expression, or its operator variant {@code field : "query"}.
     * {@code MatchOperator} extends {@code Match} — they share all constraints.
     * The operator form does not support options. Function-form options are emitted only when
     * {@link #optionsAllowed(Column)} is true.
     */
    public static String matchFunction(List<Column> columns) {
        Column column = randomColumn(columns, FullTextFunctionGenerator::isMatchColumn);
        if (column == null) {
            return null;
        }
        String field = quotedName(column);
        String query = randomQueryWord();
        if (randomBoolean()) {
            return field + " : \"" + query + "\"";
        }
        String options = optionsAllowed(column) ? maybeOptions(MATCH_OPTIONS) : "";
        return "match(" + field + ", \"" + query + "\"" + options + ")";
    }

    /**
     * Generates a {@code match_phrase(field, "query")} expression.
     * Field accepts keyword or text, mapped or runtime.
     * Query must be a string literal. Options are emitted only when {@link #optionsAllowed(Column)} is true.
     */
    public static String matchPhraseFunction(List<Column> columns) {
        Column column = randomColumn(columns, FullTextFunctionGenerator::isMatchPhraseColumn);
        if (column == null) {
            return null;
        }
        String field = quotedName(column);
        String phrase = randomQueryWord() + " " + randomQueryWord();
        String options = optionsAllowed(column) ? maybeOptions(MATCH_PHRASE_OPTIONS) : "";
        return "match_phrase(" + field + ", \"" + phrase + "\"" + options + ")";
    }

    private static Column randomColumn(List<Column> columns, Predicate<Column> eligible) {
        List<Column> candidates = columns.stream().filter(eligible).toList();
        if (candidates.isEmpty()) {
            return null;
        }
        return randomFrom(candidates);
    }

    private static String quotedName(Column column) {
        String name = column.name();
        return needsQuoting(name) ? quote(name) : name;
    }

    /**
     * Generates a {@code qstr("field:query")} expression using Lucene query string syntax.
     * query is a string literal; no field argument.
     */
    public static String qstrFunction(List<Column> columns) {
        String field = randomName(columns, MATCH_PHRASE_FIELD_TYPES);
        String query;
        if (field != null && randomBoolean()) {
            String rawName = field.startsWith("`") ? field.substring(1, field.length() - 1) : field;
            query = rawName + ":" + randomQueryWord();
        } else {
            query = randomQueryWord();
        }
        return "qstr(\"" + query + "\"" + maybeOptions(QSTR_OPTIONS) + ")";
    }

    /**
     * Generates a {@code kql("field:query")} expression using KQL syntax.
     * query is a string literal; no field argument.
     */
    public static String kqlFunction(List<Column> columns) {
        String field = randomName(columns, MATCH_PHRASE_FIELD_TYPES);
        String query;
        if (field != null && randomBoolean()) {
            String rawName = field.startsWith("`") ? field.substring(1, field.length() - 1) : field;
            query = rawName + ": " + randomQueryWord();
        } else {
            query = randomQueryWord();
        }
        return "kql(\"" + query + "\"" + maybeOptions(KQL_OPTIONS) + ")";
    }

    /**
     * Generates a random full-text search boolean expression. Picks one of: match (including
     * its {@code :} operator variant), match_phrase, qstr, or kql.
     * <p>
     * Respects two sets of constraints:
     * <ul>
     *   <li><b>Placement</b>: full-text functions are forbidden after LIMIT/STATS;
     *       QSTR and KQL additionally require all preceding commands to be FROM/WHERE/SORT.</li>
     *   <li><b>Field origin</b>: MATCH / MATCH_PHRASE / {@code :} may target index-mapped fields of the
     *       allowed types, or runtime keyword/text expressions. Options are emitted only when the chosen
     *       column is index-mapped or TEXT. {@code SET unmapped_fields="nullify"} still skips column-based
     *       match generation.</li>
     * </ul>
     * Returns {@code null} when no valid function can be generated.
     */
    public static String fullTextFunction(List<Column> columns, List<CommandGenerator.CommandDescription> previousCommands) {
        if (isFullTextAllowed(previousCommands) == false) {
            return null;
        }

        // Only offer a form when it can actually produce a value: a match/match_phrase arm that found no eligible
        // column would return null and drop the whole WHERE, even when qstr/kql could have generated a valid clause.
        List<Supplier<String>> forms = new ArrayList<>();
        if (isMatchOnColumnsAllowed(previousCommands)) {
            if (columns.stream().anyMatch(FullTextFunctionGenerator::isMatchColumn)) {
                forms.add(() -> matchFunction(columns));
            }
            if (columns.stream().anyMatch(FullTextFunctionGenerator::isMatchPhraseColumn)) {
                forms.add(() -> matchPhraseFunction(columns));
            }
        }
        if (isQstrKqlAllowed(previousCommands)) {
            forms.add(() -> qstrFunction(columns));
            forms.add(() -> kqlFunction(columns));
        }
        return forms.isEmpty() ? null : randomFrom(forms).get();
    }
}
