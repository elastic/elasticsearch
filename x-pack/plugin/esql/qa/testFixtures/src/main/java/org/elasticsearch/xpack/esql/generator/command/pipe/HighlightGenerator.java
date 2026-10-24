/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.function.FullTextFunctionGenerator;

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
import static org.elasticsearch.xpack.esql.generator.function.FullTextFunctionGenerator.randomQueryWord;

/**
 * Generates {@code HIGHLIGHT} commands with string and full-text queries. Queries only reference fields in the
 * {@code ON} clause, as required by HIGHLIGHT.
 */
public class HighlightGenerator implements CommandGenerator {

    public static final String HIGHLIGHT = "highlight";

    /** Context key for the names of columns created or overwritten by HIGHLIGHT. */
    public static final String HIGHLIGHT_COLUMNS = "highlight_columns";

    public static final CommandGenerator INSTANCE = new HighlightGenerator();

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
        { "max_analyzed_offset", "-1", "100", "1000" } };

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        List<Column> stringColumns = previousOutput.stream()
            .filter(HighlightGenerator::isStringField)
            .filter(HighlightGenerator::canPrefix)
            .toList();
        if (stringColumns.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }

        List<Column> onFields = pickOnFields(stringColumns);
        String query = buildQuery(onFields, previousCommands);
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
        } else {
            throw new IllegalStateException("HIGHLIGHT description is missing its [" + HIGHLIGHT_COLUMNS + "] context: " + generated);
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
    private static String buildQuery(List<Column> onFields, List<CommandDescription> previousCommands) {
        List<Column> functionFields = onFields.stream().filter(HighlightGenerator::cleanStringField).toList();
        List<Column> indexFields = FullTextFunctionGenerator.indexFieldColumns(functionFields, previousCommands);
        List<Column> indexMappedFields = indexFields == null ? List.of() : indexFields;
        // MATCH accepts options on a non-index-mapped field only when it is TEXT; MATCH_PHRASE never does.
        List<Column> matchFields = functionFields.stream().filter(c -> c.type().equals("text") || indexMappedFields.contains(c)).toList();
        List<Column> simpleFields = functionFields.stream().filter(c -> EsqlQueryGenerator.needsQuoting(c.name()) == false).toList();
        List<Column> simpleIndexMappedFields = indexMappedFields.stream()
            .filter(c -> EsqlQueryGenerator.needsQuoting(c.name()) == false)
            .toList();

        // String queries work with every eligible field and cover HIGHLIGHT-specific query string syntax.
        // Reserve 40% of queries for this form rather than giving it one slot among the forms below.
        if (functionFields.isEmpty() || randomIntBetween(0, 9) < 4) {
            return stringLiteralQuery();
        }

        List<Supplier<String>> forms = new ArrayList<>();
        forms.add(HighlightGenerator::stringLiteralQuery);
        forms.add(() -> FullTextFunctionGenerator.qstrFunction(simpleFields));
        forms.add(() -> qstrQuery(simpleFields));
        if (matchFields.isEmpty() == false) {
            forms.add(() -> FullTextFunctionGenerator.matchFunction(matchFields));
            forms.add(() -> booleanQuery(matchFields));
        }
        if (indexMappedFields.isEmpty() == false) {
            forms.add(() -> fuzzyMatchQuery(randomFrom(indexMappedFields)));
            forms.add(() -> FullTextFunctionGenerator.matchPhraseFunction(indexMappedFields));
        }
        if (simpleIndexMappedFields.isEmpty() == false) {
            forms.add(() -> FullTextFunctionGenerator.kqlFunction(simpleIndexMappedFields));
        }
        if (simpleFields.isEmpty() == false) {
            forms.add(() -> fieldQualifiedLiteralQuery(randomFrom(simpleFields)));
        }
        return randomFrom(forms).get();
    }

    private static String stringLiteralQuery() {
        return switch (randomIntBetween(0, 10)) {
            case 0 -> "\"" + randomQueryWord() + "\"";
            case 1 -> "\"" + randomQueryWord() + " " + randomQueryWord() + "\"";
            // An empty query analyzes to no terms (a valid no-match query).
            case 2 -> "\"\"";
            case 3 -> "\"" + randomQueryWord() + " AND " + randomQueryWord() + "\"";
            case 4 -> "\"(" + randomQueryWord() + " OR " + randomQueryWord() + ") AND " + randomQueryWord() + "\"";
            case 5 -> "\"" + randomQueryWord() + " " + randomQueryWord() + " " + randomQueryWord() + "\"";
            // A prohibited term suppresses the whole match when present.
            case 6 -> "\"" + randomQueryWord() + " -" + randomQueryWord() + "\"";
            // Wildcard term: exercises the multi-term (consumeTermsMatching) highlighter path.
            case 7 -> "\"" + wildcardTerm() + "\"";
            // Fuzzy term with AUTO fuzziness.
            case 8 -> "\"" + randomQueryWord() + "~\"";
            // Regexp term between slashes.
            case 9 -> "\"/" + randomQueryWord() + "/\"";
            // Quoted phrase: wraps the exact sequence in a single weight-matches span.
            case 10 -> "\"\\\"" + randomQueryWord() + " " + randomQueryWord() + "\\\"\"";
            default -> throw new IllegalStateException("unexpected query choice");
        };
    }

    /** A prefix, leading, or double-sided wildcard term (all valid query_string syntax in HIGHLIGHT). */
    private static String wildcardTerm() {
        String w = randomQueryWord();
        return switch (randomIntBetween(0, 2)) {
            case 0 -> w + "*";
            case 1 -> "*" + w;
            case 2 -> "*" + w + "*";
            default -> throw new IllegalStateException("unexpected wildcard choice");
        };
    }

    /** A field-qualified literal string query; the field is in ON so it highlights (require_field_match parity). */
    private static String fieldQualifiedLiteralQuery(Column simpleField) {
        return "\"" + simpleField.name() + ":" + randomQueryWord() + "\"";
    }

    private static String qstrQuery(List<Column> simpleFields) {
        if (simpleFields.isEmpty() || randomBoolean()) {
            return "qstr(\"\\\"" + randomQueryWord() + " " + randomQueryWord() + "\\\"\")";
        }
        String name = randomFrom(simpleFields).name();
        if (randomBoolean()) {
            return "qstr(\"" + randomQueryWord() + "\", {\"default_field\": \"" + name + "\"})";
        }
        return "qstr(\"" + name + ":[a TO z]\")";
    }

    private static String fuzzyMatchQuery(Column indexMappedField) {
        String rewrite = randomFrom("\"constant_score\"", "\"scoring_boolean\"", "\"constant_score_boolean\"");
        return "match("
            + ref(indexMappedField.name())
            + ", \""
            + randomQueryWord()
            + "\", {\"fuzziness\": \"AUTO\", \"fuzzy_rewrite\": "
            + rewrite
            + "})";
    }

    private static String booleanQuery(List<Column> matchFields) {
        String left = "(" + FullTextFunctionGenerator.matchFunction(matchFields) + ")";
        if (randomBoolean()) {
            return "NOT " + left;
        }
        String right = "(" + FullTextFunctionGenerator.matchFunction(matchFields) + ")";
        return left + (randomBoolean() ? " AND " : " OR ") + right;
    }

    private static String maybeWith() {
        // Exercise WITH options without making them more common than the default behavior.
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

    private static String ref(String rawName) {
        return EsqlQueryGenerator.needsQuoting(rawName) ? EsqlQueryGenerator.quote(rawName) : rawName;
    }
}
