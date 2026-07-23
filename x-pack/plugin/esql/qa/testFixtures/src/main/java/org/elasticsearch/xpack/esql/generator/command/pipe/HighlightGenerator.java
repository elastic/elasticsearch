/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Generates {@code HIGHLIGHT} commands with string and full-text queries. Queries only reference fields in the
 * {@code ON} clause, as required by HIGHLIGHT.
 */
public class HighlightGenerator implements CommandGenerator {

    public static final String HIGHLIGHT = "highlight";

    /** Context key for the names of columns created or overwritten by HIGHLIGHT. */
    public static final String HIGHLIGHT_COLUMNS = "highlight_columns";

    public static final CommandGenerator INSTANCE = new HighlightGenerator();

    private static final String[] QUERY_WORDS = { "test", "hello", "world", "data", "search", "quick", "brown", "fox", "ring", "return" };

    /** MATCH options are valid only for index-mapped fields. */
    private static final String[][] MATCH_OPTIONS = {
        { "operator", "\"AND\"", "\"OR\"" },
        { "fuzziness", "\"AUTO\"", "1", "2" },
        { "boost", "1.0", "2.5" },
        { "zero_terms_query", "\"none\"", "\"all\"" } };

    /** Entries have the form {@code { name, value1, value2, ... }}. */
    private static final String[][] WITH_OPTIONS = {
        { "pre_tags", "[\"<b>\"]", "[\"<em>\"]", "[\"<mark>\"]" },
        { "post_tags", "[\"</b>\"]", "[\"</em>\"]", "[\"</mark>\"]" },
        { "number_of_fragments", "1", "2", "3", "5" },
        { "fragment_size", "50", "100", "150", "200" },
        { "encoder", "\"default\"", "\"html\"" },
        { "analyzer", "\"standard\"", "\"english\"", "\"whitespace\"", "\"simple\"", "\"keyword\"", "\"stop\"" },
        { "boundary_scanner", "\"sentence\"", "\"word\"" },
        { "boundary_scanner_locale", "\"en-US\"", "\"en\"", "\"fr\"" },
        { "order", "\"none\"", "\"score\"" },
        { "no_match_size", "0", "30", "100", "200" },
        // max_analyzed_offset must be a positive integer or -1 (0 is invalid).
        { "max_analyzed_offset", "-1", "100", "1000" },
        // Accepted for Query DSL parity but no-ops for the unified highlighter HIGHLIGHT uses.
        { "boundary_chars", "\".,!?\"" },
        { "boundary_max_scan", "10", "20" },
        { "phrase_limit", "128", "256" } };

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        if (EsqlCapabilities.Cap.HIGHLIGHT_V5.isEnabled() == false) {
            return EMPTY_DESCRIPTION;
        }

        List<Column> stringColumns = previousOutput.stream()
            .filter(HighlightGenerator::isStringField)
            .filter(HighlightGenerator::canPrefix)
            .toList();
        if (stringColumns.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }

        List<Column> onFields = pickOnFields(stringColumns);
        String query = buildQuery(onFields);

        // Generated column names must work with EsqlQueryGenerator.needsQuoting. Do not generate prefixes with spaces.
        String prefixValue;
        String prefixClause;
        switch (randomIntBetween(0, 2)) {
            case 0 -> {
                prefixValue = "highlight_";
                prefixClause = "";
            }
            case 1 -> {
                prefixValue = randomBoolean() ? "hl_" : EsqlQueryGenerator.randomIdentifier() + "_";
                prefixClause = "prefix = \"" + prefixValue + "\" ";
            }
            case 2 -> {
                prefixValue = "";
                prefixClause = "prefix = \"\" ";
            }
            default -> throw new IllegalStateException("unexpected prefix choice");
        }

        String onClause = onFields.stream().map(c -> ref(c.name())).collect(Collectors.joining(", "));

        StringBuilder command = new StringBuilder(" | HIGHLIGHT ");
        command.append(prefixClause);
        command.append(query);
        command.append(" ON ").append(onClause);
        command.append(maybeWith());

        List<String> generatedColumns = onFields.stream().map(c -> prefixValue + c.name()).toList();
        Map<String, Object> commandContext = Map.of(HIGHLIGHT_COLUMNS, generatedColumns);
        return new CommandDescription(HIGHLIGHT, this, command.toString(), commandContext);
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription commandDescription,
        List<Column> previousColumns,
        List<List<Object>> previousOutput,
        List<Column> columns,
        List<List<Object>> output
    ) {
        if (commandDescription == EMPTY_DESCRIPTION) {
            return VALIDATION_OK;
        }
        if (previousColumns == null || columns == null) {
            return VALIDATION_OK;
        }

        // HIGHLIGHT appends columns (or overwrites with an empty prefix), so it never drops any.
        ValidationResult sizeCheck = CommandGenerator.expectAtLeastSameNumberOfColumns(previousColumns, columns);
        if (sizeCheck.success() == false) {
            return sizeCheck;
        }

        Object generated = commandDescription.context().get(HIGHLIGHT_COLUMNS);
        if (generated instanceof List<?> generatedColumns) {
            Map<String, String> typesByName = new HashMap<>();
            for (Column c : columns) {
                typesByName.put(c.name(), c.type());
            }
            for (Object nameObj : generatedColumns) {
                String name = (String) nameObj;
                String type = typesByName.get(name);
                if (type == null) {
                    return new ValidationResult(false, "HIGHLIGHT output is missing expected column [" + name + "]");
                }
                if (type.equals("keyword") == false) {
                    return new ValidationResult(false, "HIGHLIGHT column [" + name + "] should be [keyword] but was [" + type + "]");
                }
            }
        }
        return VALIDATION_OK;
    }

    /** Union-typed string columns can be used by the ON clause and string queries. */
    private static boolean isStringField(Column column) {
        return (column.type().equals("text") || column.type().equals("keyword")) && EsqlQueryGenerator.fieldCanBeUsed(column);
    }

    /**
     * An {@code @} is valid at the start of an unquoted identifier, but not after a prefix. Exclude such fields because
     * {@link EsqlQueryGenerator#needsQuoting} does not handle that case.
     */
    private static boolean canPrefix(Column column) {
        return column.name().indexOf('@') < 0;
    }

    /** Excludes union types that can resolve to a different field name during MATCH translation. */
    private static boolean cleanStringField(Column column) {
        if (isStringField(column) == false) {
            return false;
        }
        for (String originalType : column.originalTypes()) {
            if (originalType.equals("text") == false && originalType.equals("keyword") == false) {
                return false;
            }
        }
        return true;
    }

    private static List<Column> pickOnFields(List<Column> stringColumns) {
        int n = randomIntBetween(1, Math.min(2, stringColumns.size()));
        List<Column> pool = new ArrayList<>(stringColumns);
        List<Column> chosen = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            chosen.add(pool.remove(randomIntBetween(0, pool.size() - 1)));
        }
        return chosen;
    }

    /** Builds a query using only the given ON fields. */
    private static String buildQuery(List<Column> onFields) {
        List<Column> functionFields = onFields.stream().filter(HighlightGenerator::cleanStringField).toList();
        List<Column> indexMappedFields = functionFields.stream().filter(Column::indexMapped).toList();
        List<Column> simpleFields = functionFields.stream().filter(c -> EsqlQueryGenerator.needsQuoting(c.name()) == false).toList();
        List<Column> simpleIndexMappedFields = indexMappedFields.stream()
            .filter(c -> EsqlQueryGenerator.needsQuoting(c.name()) == false)
            .toList();

        // String queries work with every eligible field.
        if (functionFields.isEmpty() || randomIntBetween(0, 9) < 4) {
            return stringLiteralQuery();
        }

        List<Supplier<String>> forms = new ArrayList<>();
        forms.add(HighlightGenerator::stringLiteralQuery);
        forms.add(() -> matchQuery(randomFrom(functionFields)));
        forms.add(() -> qstrQuery(simpleFields));
        forms.add(() -> booleanQuery(functionFields));
        if (indexMappedFields.isEmpty() == false) {
            forms.add(() -> matchPhraseQuery(randomFrom(indexMappedFields)));
        }
        if (simpleIndexMappedFields.isEmpty() == false) {
            forms.add(() -> kqlQuery(randomFrom(simpleIndexMappedFields)));
        }
        return randomFrom(forms).get();
    }

    private static String stringLiteralQuery() {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> "\"" + word() + "\"";
            case 1 -> "\"" + word() + " " + word() + "\"";
            // An empty query analyzes to no terms (a valid no-match query).
            case 2 -> "\"\"";
            case 3 -> "\"" + word() + " AND " + word() + "\"";
            case 4 -> "\"(" + word() + " OR " + word() + ") AND " + word() + "\"";
            case 5 -> "\"" + word() + " " + word() + " " + word() + "\"";
            default -> throw new IllegalStateException("unexpected query choice");
        };
    }

    private static String matchQuery(Column field) {
        String reference = ref(field.name());
        if (randomBoolean()) {
            // The `:` operator form does not support options.
            return reference + " : \"" + word() + "\"";
        }
        String options = field.indexMapped() ? maybeMatchOptions() : "";
        return "match(" + reference + ", \"" + word() + "\"" + options + ")";
    }

    private static String matchPhraseQuery(Column field) {
        return "match_phrase(" + ref(field.name()) + ", \"" + word() + " " + word() + "\")";
    }

    private static String qstrQuery(List<Column> simpleFields) {
        if (simpleFields.isEmpty() == false && randomBoolean()) {
            return "qstr(\"" + randomFrom(simpleFields).name() + ":" + word() + "\")";
        }
        return "qstr(\"" + word() + "\")";
    }

    private static String kqlQuery(Column simpleField) {
        return "kql(\"" + simpleField.name() + ": " + word() + "\")";
    }

    private static String booleanQuery(List<Column> functionFields) {
        if (randomBoolean()) {
            return "NOT match(" + ref(randomFrom(functionFields).name()) + ", \"" + word() + "\")";
        }
        Column left = randomFrom(functionFields);
        Column right = randomFrom(functionFields);
        String operator = randomBoolean() ? " AND " : " OR ";
        return "match(" + ref(left.name()) + ", \"" + word() + "\")" + operator + "match(" + ref(right.name()) + ", \"" + word() + "\")";
    }

    private static String maybeMatchOptions() {
        if (randomIntBetween(0, 3) != 0) {
            return "";
        }
        String[] entry = randomFrom(MATCH_OPTIONS);
        return ", {\"" + entry[0] + "\": " + entry[randomIntBetween(1, entry.length - 1)] + "}";
    }

    private static String maybeWith() {
        if (randomIntBetween(0, 9) < 6) {
            return "";
        }
        int count = randomIntBetween(1, 3);
        Set<Integer> usedIndices = new HashSet<>();
        List<String> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int idx = randomIntBetween(0, WITH_OPTIONS.length - 1);
            if (usedIndices.add(idx) == false) {
                continue;
            }
            String[] entry = WITH_OPTIONS[idx];
            entries.add("\"" + entry[0] + "\": " + entry[randomIntBetween(1, entry.length - 1)]);
        }
        return " WITH { " + String.join(", ", entries) + " }";
    }

    private static String word() {
        return randomFrom(QUERY_WORDS);
    }

    private static String ref(String rawName) {
        return EsqlQueryGenerator.needsQuoting(rawName) ? EsqlQueryGenerator.quote(rawName) : rawName;
    }
}
