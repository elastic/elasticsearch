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
 * Generates {@code HIGHLIGHT} commands with string and full-text queries.
 * <p>
 * When {@link EsqlCapabilities.Cap#HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS} is enabled, the {@code ON} clause may be
 * omitted (fields are derived from a MATCH-style query, or every text/keyword column for a string/QSTR/KQL query) or
 * written as {@code ON *} (every text/keyword column). Otherwise the command lists explicit {@code ON} fields, and
 * queries only reference those fields.
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

        // ON * / a string query with no ON expand to every highlightable column, including ones canPrefix rejects.
        boolean allHighlightableCanPrefix = previousOutput.stream()
            .filter(HighlightGenerator::isStringField)
            .allMatch(HighlightGenerator::canPrefix);
        Prefix prefix = pickPrefix();
        OnMode onMode = pickOnMode(allHighlightableCanPrefix);
        OnClauseParts parts = switch (onMode) {
            case STAR -> starOn(stringColumns, previousCommands);
            case OMITTED -> {
                OnClauseParts omitted = omittedOn(stringColumns, previousCommands, allHighlightableCanPrefix);
                yield omitted != null ? omitted : explicitOn(stringColumns, previousCommands);
            }
            case EXPLICIT -> explicitOn(stringColumns, previousCommands);
        };

        String command = " | HIGHLIGHT " + prefix.clause() + parts.query() + parts.onClause() + maybeWith();
        List<String> generatedColumns = parts.generatedFrom().stream().map(c -> prefix.value() + c.name()).toList();
        Map<String, Object> commandContext = Map.of(HIGHLIGHT_COLUMNS, generatedColumns);
        return new CommandDescription(HIGHLIGHT, this, command, commandContext);
    }

    /** Query text, ON clause (including the leading space, or empty), and columns HIGHLIGHT will append. */
    private record OnClauseParts(String query, String onClause, List<Column> generatedFrom) {}

    /** How the generated command names its ON fields. */
    private enum OnMode {
        EXPLICIT,
        STAR,
        OMITTED
    }

    private static OnMode pickOnMode(boolean allHighlightableCanPrefix) {
        if (EsqlCapabilities.Cap.HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS.isEnabled() == false) {
            return OnMode.EXPLICIT;
        }
        return switch (randomIntBetween(0, 2)) {
            case 0 -> OnMode.EXPLICIT;
            case 1 -> allHighlightableCanPrefix ? OnMode.STAR : OnMode.EXPLICIT;
            case 2 -> OnMode.OMITTED;
            default -> throw new IllegalStateException("unexpected ON clause choice");
        };
    }

    private static String joinOn(List<Column> fields) {
        return fields.stream().map(c -> ref(c.name())).collect(Collectors.joining(", "));
    }

    private static OnClauseParts starOn(List<Column> stringColumns, List<CommandDescription> previousCommands) {
        // ON * highlights every text/keyword column, so a generated column exists for each regardless of the query.
        return new OnClauseParts(buildQuery(pickOnFields(stringColumns), previousCommands, true), " ON *", stringColumns);
    }

    private static OnClauseParts explicitOn(List<Column> stringColumns, List<CommandDescription> previousCommands) {
        List<Column> queryFields = pickOnFields(stringColumns);
        return new OnClauseParts(buildQuery(queryFields, previousCommands, true), " ON " + joinOn(queryFields), queryFields);
    }

    /**
     * A no-ON command and the columns it will generate, or {@code null} when neither no-ON form is safe: an unnamed
     * query falls back to every string column (unsafe when some cannot be prefixed) and no MATCH-eligible field is
     * available to name instead.
     */
    private static OnClauseParts omittedOn(
        List<Column> stringColumns,
        List<CommandDescription> previousCommands,
        boolean allHighlightableCanPrefix
    ) {
        // A MATCH query without ON derives only the single (prefixable) field it names; a string/QSTR/KQL query cannot
        // be narrowed and falls back to every string column (highlightMatchOmitsOn vs explicitQueryNoOnFallsBackToAllFields).
        List<Column> matchFields = QueryFields.of(stringColumns, previousCommands).matchFields();
        if (matchFields.isEmpty() == false && (allHighlightableCanPrefix == false || randomBoolean())) {
            Column field = randomFrom(matchFields);
            return new OnClauseParts(namedFieldQuery(field, previousCommands), "", List.of(field));
        }
        if (allHighlightableCanPrefix) {
            return new OnClauseParts(unnamedQuery(stringColumns, previousCommands), "", stringColumns);
        }
        return null;
    }

    private static List<Column> indexMappedFields(List<Column> functionFields, List<CommandDescription> previousCommands) {
        List<Column> indexFields = FullTextFunctionGenerator.indexFieldColumns(functionFields, previousCommands);
        return indexFields == null ? List.of() : indexFields;
    }

    /** A MATCH-family query that narrows to the single given field (no {@code NOT}: that would derive an empty ON list). */
    private static String namedFieldQuery(Column field, List<CommandDescription> previousCommands) {
        List<Supplier<String>> forms = new ArrayList<>();
        addNamedForms(forms, QueryFields.of(List.of(field), previousCommands), false);
        return randomFrom(forms).get();
    }

    /** A string / QSTR / KQL query, which cannot be narrowed to concrete fields and so falls back to every string column. */
    private static String unnamedQuery(List<Column> stringColumns, List<CommandDescription> previousCommands) {
        List<Supplier<String>> forms = new ArrayList<>();
        addUnnamedForms(forms, QueryFields.of(stringColumns, previousCommands));
        return randomFrom(forms).get();
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

    /** The field subsets each query form needs, derived once from a set of candidate columns. */
    private record QueryFields(
        List<Column> functionFields,
        List<Column> matchFields,
        List<Column> indexMappedFields,
        List<Column> simpleFields,
        List<Column> simpleIndexMappedFields
    ) {
        static QueryFields of(List<Column> columns, List<CommandDescription> previousCommands) {
            List<Column> functionFields = columns.stream().filter(HighlightGenerator::cleanStringField).toList();
            List<Column> indexMappedFields = HighlightGenerator.indexMappedFields(functionFields, previousCommands);
            // MATCH accepts options on a non-index-mapped field only when it is TEXT; MATCH_PHRASE never does.
            List<Column> matchFields = functionFields.stream()
                .filter(c -> c.type().equals("text") || indexMappedFields.contains(c))
                .toList();
            List<Column> simpleFields = functionFields.stream().filter(c -> EsqlQueryGenerator.needsQuoting(c.name()) == false).toList();
            List<Column> simpleIndexMappedFields = indexMappedFields.stream()
                .filter(c -> EsqlQueryGenerator.needsQuoting(c.name()) == false)
                .toList();
            return new QueryFields(functionFields, matchFields, indexMappedFields, simpleFields, simpleIndexMappedFields);
        }
    }

    /** Builds a query over the given fields. {@code allowNot} is false when omitting ON (NOT MATCH derives no fields). */
    private static String buildQuery(List<Column> onFields, List<CommandDescription> previousCommands, boolean allowNot) {
        QueryFields fields = QueryFields.of(onFields, previousCommands);
        // The string form works with every eligible field and covers HIGHLIGHT-specific query string syntax.
        // Reserve 40% of queries for it rather than giving it one slot among the forms below.
        if (fields.functionFields().isEmpty() || randomIntBetween(0, 9) < 4) {
            return stringLiteralQuery();
        }
        List<Supplier<String>> forms = new ArrayList<>();
        addUnnamedForms(forms, fields);
        addNamedForms(forms, fields, allowNot);
        return randomFrom(forms).get();
    }

    /** Query forms that cannot be narrowed to concrete fields (string literal, QSTR, KQL). */
    private static void addUnnamedForms(List<Supplier<String>> forms, QueryFields fields) {
        forms.add(HighlightGenerator::stringLiteralQuery);
        forms.add(() -> FullTextFunctionGenerator.qstrFunction(fields.simpleFields()));
        forms.add(() -> qstrQuery(fields.simpleFields()));
        if (fields.simpleFields().isEmpty() == false) {
            forms.add(() -> fieldQualifiedLiteralQuery(randomFrom(fields.simpleFields())));
        }
        if (fields.simpleIndexMappedFields().isEmpty() == false) {
            forms.add(() -> FullTextFunctionGenerator.kqlFunction(fields.simpleIndexMappedFields()));
        }
    }

    /** Query forms that narrow to the fields they name (MATCH, MATCH_PHRASE, fuzzy MATCH, boolean MATCH). */
    private static void addNamedForms(List<Supplier<String>> forms, QueryFields fields, boolean allowNot) {
        if (fields.matchFields().isEmpty() == false) {
            forms.add(() -> FullTextFunctionGenerator.matchFunction(fields.matchFields()));
            forms.add(() -> booleanQuery(fields.matchFields(), allowNot));
        }
        if (fields.indexMappedFields().isEmpty() == false) {
            forms.add(() -> fuzzyMatchQuery(randomFrom(fields.indexMappedFields())));
            forms.add(() -> FullTextFunctionGenerator.matchPhraseFunction(fields.indexMappedFields()));
        }
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

    private static String booleanQuery(List<Column> matchFields, boolean allowNot) {
        String left = "(" + FullTextFunctionGenerator.matchFunction(matchFields) + ")";
        if (allowNot && randomBoolean()) {
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
