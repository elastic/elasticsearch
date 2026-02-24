/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.EvalGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.needsQuoting;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.quote;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomName;

/**
 * Generates random full-text search expressions (match/match_phrase/qstr/kql/multi_match/:).
 */
public final class FullTextFunctionGenerator {

    private FullTextFunctionGenerator() {}

    private static final Set<String> QSTR_KQL_SAFE_COMMANDS = Set.of("from", "where", "sort");

    private static boolean isFullTextAllowed(List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null || previousCommands.isEmpty()) {
            return false;
        }
        if ("from".equals(previousCommands.get(0).commandName()) == false) {
            return false;
        }
        for (CommandGenerator.CommandDescription cmd : previousCommands) {
            if ("limit".equals(cmd.commandName())
                || "stats".equals(cmd.commandName())
                || "inline stats".equals(cmd.commandName())
                || "change_point".equals(cmd.commandName())) {
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

    private static final Pattern RENAME_PAIR = Pattern.compile("\\s*`?([^`]+?)`?\\s+[Aa][Ss]\\s+`?([^`]+?)`?\\s*");

    @SuppressWarnings("unchecked")
    private static List<Column> indexFieldColumns(List<Column> columns, List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null || previousCommands.isEmpty()) {
            return null;
        }
        Object stored = previousCommands.get(0).context().get(FromGenerator.INDEX_FIELD_NAMES);
        if (stored instanceof Set<?> == false) {
            return null;
        }
        Set<String> safeNames = new HashSet<>((Set<String>) stored);
        for (CommandGenerator.CommandDescription cmd : previousCommands) {
            if ("eval".equals(cmd.commandName())) {
                Object newCols = cmd.context().get(EvalGenerator.NEW_COLUMNS);
                if (newCols instanceof List<?> list) {
                    list.forEach(name -> safeNames.remove((String) name));
                }
            } else if ("mv_expand".equals(cmd.commandName())) {
                String expandedField = cmd.commandString().replaceFirst("(?i)^\\s*\\|\\s*mv_expand\\s+", "").trim();
                if (expandedField.startsWith("`") && expandedField.endsWith("`")) {
                    expandedField = expandedField.substring(1, expandedField.length() - 1);
                }
                safeNames.remove(expandedField);
            } else if ("rename".equals(cmd.commandName())) {
                String cmdStr = cmd.commandString().replaceFirst("(?i)^\\s*\\|\\s*rename\\s+", "");
                for (String pair : cmdStr.split(",")) {
                    Matcher m = RENAME_PAIR.matcher(pair);
                    if (m.matches()) {
                        String oldName = m.group(1);
                        String newName = m.group(2);
                        boolean wasSafe = safeNames.remove(oldName);
                        if (wasSafe) {
                            safeNames.add(newName);
                        } else {
                            safeNames.remove(newName);
                        }
                    }
                }
            }
        }
        return columns.stream().filter(c -> safeNames.contains(c.name())).toList();
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
    private static final Set<String> MATCH_PHRASE_FIELD_TYPES = Set.of("keyword", "text");

    private static final String[] SAMPLE_QUERY_WORDS = { "test", "hello", "world", "data", "search", "quick", "brown", "fox" };

    private static String randomQueryWord() {
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
        { "boost", "1.0", "2.5" }, };

    private static final String[][] KQL_OPTIONS = { { "case_insensitive", "true", "false" }, { "boost", "1.0", "2.5" }, };

    private static final String[][] MULTI_MATCH_OPTIONS = {
        { "operator", "\"AND\"", "\"OR\"" },
        { "lenient", "true", "false" },
        { "boost", "1.0", "2.5" },
        { "type", "\"best_fields\"", "\"most_fields\"", "\"phrase\"" }, };

    /**
     * Generates a {@code match(field, "query")} expression, or its operator variant {@code field : "query"}.
     * {@code MatchOperator} extends {@code Match} — they share all constraints.
     * The operator form does not support options.
     */
    public static String matchFunction(List<Column> columns) {
        String field = randomName(columns, MATCH_FIELD_TYPES);
        if (field == null) {
            return null;
        }
        String query = randomQueryWord();
        if (randomBoolean()) {
            return field + " : \"" + query + "\"";
        }
        return "match(" + field + ", \"" + query + "\"" + maybeOptions(MATCH_OPTIONS) + ")";
    }

    /**
     * Generates a {@code match_phrase(field, "query")} expression.
     * field accepts: keyword, text only.
     * query must be a string literal.
     */
    public static String matchPhraseFunction(List<Column> columns) {
        String field = randomName(columns, MATCH_PHRASE_FIELD_TYPES);
        if (field == null) {
            return null;
        }
        String phrase = randomQueryWord() + " " + randomQueryWord();
        return "match_phrase(" + field + ", \"" + phrase + "\"" + maybeOptions(MATCH_PHRASE_OPTIONS) + ")";
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
     * Generates a {@code multi_match("query", field1, field2 [, ...])} expression.
     * Fields accept the same types as match(). Query must be a string literal.
     */
    public static String multiMatchFunction(List<Column> columns) {
        List<String> fields = columns.stream()
            .filter(c -> MATCH_FIELD_TYPES.contains(c.type()))
            .map(c -> needsQuoting(c.name()) ? quote(c.name()) : c.name())
            .collect(Collectors.toList());
        if (fields.size() < 2) {
            return null;
        }
        int count = Math.min(fields.size(), randomIntBetween(2, 4));
        List<String> selected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            selected.add(fields.get(randomIntBetween(0, fields.size() - 1)));
        }
        return "multi_match(\"" + randomQueryWord() + "\", " + String.join(", ", selected) + maybeOptions(MULTI_MATCH_OPTIONS) + ")";
    }

    /**
     * Generates a random full-text search boolean expression. Picks one of: match (including
     * its {@code :} operator variant), match_phrase, qstr, kql, or multi_match.
     * <p>
     * Respects two sets of constraints:
     * <ul>
     *   <li><b>Placement</b>: full-text functions are forbidden after LIMIT/STATS;
     *       QSTR and KQL additionally require all preceding commands to be FROM/WHERE/SORT.</li>
     *   <li><b>Field origin</b>: match, match_phrase, and multi_match
     *       require fields from the actual index mapping (FieldAttribute), not columns
     *       created by EVAL, GROK, DISSECT, etc.</li>
     * </ul>
     * Returns {@code null} when no valid function can be generated.
     */
    public static String fullTextFunction(List<Column> columns, List<CommandGenerator.CommandDescription> previousCommands) {
        if (isFullTextAllowed(previousCommands) == false) {
            return null;
        }

        boolean qstrKqlAllowed = isQstrKqlAllowed(previousCommands);

        List<Column> indexColumns = indexFieldColumns(columns, previousCommands);
        boolean fieldBasedAllowed = indexColumns != null && indexColumns.isEmpty() == false;

        if (fieldBasedAllowed && qstrKqlAllowed) {
            return switch (randomIntBetween(0, 4)) {
                case 0 -> matchFunction(indexColumns);
                case 1 -> matchPhraseFunction(indexColumns);
                case 2 -> qstrFunction(columns);
                case 3 -> kqlFunction(columns);
                default -> multiMatchFunction(indexColumns);
            };
        } else if (fieldBasedAllowed) {
            return switch (randomIntBetween(0, 2)) {
                case 0 -> matchFunction(indexColumns);
                case 1 -> matchPhraseFunction(indexColumns);
                default -> multiMatchFunction(indexColumns);
            };
        } else if (qstrKqlAllowed) {
            return randomBoolean() ? qstrFunction(columns) : kqlFunction(columns);
        } else {
            return null;
        }
    }
}
