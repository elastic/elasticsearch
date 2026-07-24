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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomSubsetOf;

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
        { "zero_terms_query", "\"none\"", "\"all\"" },
        { "lenient", "true", "false" } };

    /** Field-independent QSTR options ({@code default_field} is handled separately since its value is a field name). */
    private static final String[][] QSTR_OPTIONS = {
        { "default_operator", "\"OR\"", "\"AND\"" },
        { "lenient", "true", "false" },
        { "fuzziness", "\"AUTO\"", "1" },
        { "boost", "1.0", "2.5" },
        { "phrase_slop", "1", "2", "3" },
        { "analyze_wildcard", "true", "false" } };

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
        Prefix prefix = pickPrefix();
        String onClause = onFields.stream().map(c -> ref(c.name())).collect(Collectors.joining(", "));

        String command = " | HIGHLIGHT " + prefix.clause() + query + " ON " + onClause + maybeWith();

        List<String> generatedColumns = onFields.stream().map(c -> prefix.value() + c.name()).toList();
        Map<String, Object> commandContext = Map.of(HIGHLIGHT_COLUMNS, generatedColumns);
        return new CommandDescription(HIGHLIGHT, this, command, commandContext);
    }

    /** A generated column prefix and the {@code prefix = "..."} clause (if any) that produces it. */
    private record Prefix(String value, String clause) {}

    /** Generated column names must work with {@link EsqlQueryGenerator#needsQuoting}. Do not generate prefixes with spaces. */
    private static Prefix pickPrefix() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new Prefix("highlight_", "");
            case 1 -> {
                String value = randomBoolean() ? "hl_" : EsqlQueryGenerator.randomIdentifier() + "_";
                yield new Prefix(value, "prefix = \"" + value + "\" ");
            }
            case 2 -> new Prefix("", "prefix = \"\" ");
            default -> throw new IllegalStateException("unexpected prefix choice");
        };
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
            Map<String, String> typesByName = columns.stream()
                .collect(Collectors.toMap(Column::name, Column::type, (first, second) -> second));
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

    private static final Set<String> STRING_TYPES = Set.of("text", "keyword");

    /** Union-typed string columns can be used by the ON clause and string queries. */
    private static boolean isStringField(Column column) {
        return STRING_TYPES.contains(column.type()) && EsqlQueryGenerator.fieldCanBeUsed(column);
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
        return isStringField(column) && column.originalTypes().stream().allMatch(STRING_TYPES::contains);
    }

    private static List<Column> pickOnFields(List<Column> stringColumns) {
        int n = randomIntBetween(1, Math.min(2, stringColumns.size()));
        return randomSubsetOf(n, stringColumns);
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
        if (simpleFields.isEmpty() == false) {
            forms.add(() -> fieldQualifiedLiteralQuery(randomFrom(simpleFields)));
        }
        return randomFrom(forms).get();
    }

    private static String stringLiteralQuery() {
        return switch (randomIntBetween(0, 10)) {
            case 0 -> "\"" + word() + "\"";
            case 1 -> "\"" + word() + " " + word() + "\"";
            // An empty query analyzes to no terms (a valid no-match query).
            case 2 -> "\"\"";
            case 3 -> "\"" + word() + " AND " + word() + "\"";
            case 4 -> "\"(" + word() + " OR " + word() + ") AND " + word() + "\"";
            case 5 -> "\"" + word() + " " + word() + " " + word() + "\"";
            // A prohibited term suppresses the whole match when present.
            case 6 -> "\"" + word() + " -" + word() + "\"";
            // Wildcard term: exercises the multi-term (consumeTermsMatching) highlighter path.
            case 7 -> "\"" + wildcardTerm() + "\"";
            // Fuzzy term with AUTO fuzziness.
            case 8 -> "\"" + word() + "~\"";
            // Regexp term between slashes.
            case 9 -> "\"/" + word() + "/\"";
            // Quoted phrase: wraps the exact sequence in a single weight-matches span.
            case 10 -> "\"\\\"" + word() + " " + word() + "\\\"\"";
            default -> throw new IllegalStateException("unexpected query choice");
        };
    }

    /** A prefix, leading, or double-sided wildcard term (all valid query_string syntax in HIGHLIGHT). */
    private static String wildcardTerm() {
        String w = word();
        return switch (randomIntBetween(0, 2)) {
            case 0 -> w + "*";
            case 1 -> "*" + w;
            case 2 -> "*" + w + "*";
            default -> throw new IllegalStateException("unexpected wildcard choice");
        };
    }

    /** A field-qualified literal string query; the field is in ON so it highlights (require_field_match parity). */
    private static String fieldQualifiedLiteralQuery(Column simpleField) {
        return "\"" + simpleField.name() + ":" + word() + "\"";
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
        List<Supplier<String>> bodies = new ArrayList<>();
        bodies.add(HighlightGenerator::word);
        // Quoted phrase -> single weight-matches span.
        bodies.add(() -> "\\\"" + word() + " " + word() + "\\\"");
        if (simpleFields.isEmpty() == false) {
            // Field-qualified term, and a range matching every in-range term; both need a simple ON field.
            bodies.add(() -> randomFrom(simpleFields).name() + ":" + word());
            bodies.add(() -> randomFrom(simpleFields).name() + ":[a TO z]");
        }
        return "qstr(\"" + randomFrom(bodies).get() + "\"" + maybeQstrOptions(simpleFields) + ")";
    }

    private static String maybeQstrOptions(List<Column> simpleFields) {
        if (randomIntBetween(0, 3) != 0) {
            return "";
        }
        // default_field must name an ON field, so only offer it when a simple (unquoted) field exists.
        if (simpleFields.isEmpty() == false && randomBoolean()) {
            return ", {\"default_field\": \"" + randomFrom(simpleFields).name() + "\"}";
        }
        return ", {" + optionEntry(randomFrom(QSTR_OPTIONS)) + "}";
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
        // fuzzy_rewrite only has an effect alongside fuzziness, so emit the two together.
        if (randomBoolean()) {
            String rewrite = randomFrom("\"constant_score\"", "\"scoring_boolean\"", "\"constant_score_boolean\"");
            return ", {\"fuzziness\": \"AUTO\", \"fuzzy_rewrite\": " + rewrite + "}";
        }
        return ", {" + optionEntry(randomFrom(MATCH_OPTIONS)) + "}";
    }

    private static String maybeWith() {
        if (randomIntBetween(0, 9) < 6) {
            return "";
        }
        int count = randomIntBetween(1, 3);
        List<String> entries = randomSubsetOf(Math.min(count, WITH_OPTIONS.length), List.of(WITH_OPTIONS)).stream()
            .map(HighlightGenerator::optionEntry)
            .toList();
        return " WITH { " + String.join(", ", entries) + " }";
    }

    /** Renders a {@code { name, value1, value2, ... }} entry as {@code "name": value}, picking one value at random. */
    private static String optionEntry(String[] entry) {
        return "\"" + entry[0] + "\": " + entry[randomIntBetween(1, entry.length - 1)];
    }

    private static String word() {
        return randomFrom(QUERY_WORDS);
    }

    private static String ref(String rawName) {
        return EsqlQueryGenerator.needsQuoting(rawName) ? EsqlQueryGenerator.quote(rawName) : rawName;
    }
}
