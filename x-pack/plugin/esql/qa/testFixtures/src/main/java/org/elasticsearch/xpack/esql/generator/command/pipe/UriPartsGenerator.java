/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;

public class UriPartsGenerator implements CommandGenerator {

    public static final CommandGenerator INSTANCE = new UriPartsGenerator();

    public static final String URI_PARTS = "uri_parts";

    /**
     * Context key for the output field prefix (unquoted) used in the generated command.
     */
    private static final String PREFIX = "prefix";

    /**
     * Expected URI_PARTS output field names and their ES|QL types. Computed once from
     * {@link UriPartsFunctionBridge#getAllOutputFields()} and {@link DataType#fromJavaType}.
     */
    private static final LinkedHashMap<String, String> URI_PARTS_OUTPUT_FIELDS;
    static {
        LinkedHashMap<String, Class<?>> outputFields = UriPartsFunctionBridge.getAllOutputFields();
        URI_PARTS_OUTPUT_FIELDS = new LinkedHashMap<>(outputFields.size());
        for (Map.Entry<String, Class<?>> e : outputFields.entrySet()) {
            URI_PARTS_OUTPUT_FIELDS.putLast(e.getKey(), Objects.requireNonNull(DataType.fromJavaType(e.getValue())).typeName());
        }
    }
    /**
    * Valid literal URIs used so that at least some generated commands parse real URIs (happy path).
    */
    private static final String[] LITERAL_URIS = new String[] {
        "http://myusername:mypassword@www.example.com:80/foo.gif?key1=val1&key2=val2#fragment",
        "https://www.elastic.co/downloads/elasticsearch",
        "https://www.elastic.co/guide/en/elasticsearch/reference/current/esql.html",
        "https://www.google.com/search?q=elasticsearch",
        "https://github.com/elastic/elasticsearch",
        "ftp://user:pass@files.internal/data.zip",
        "/app/login?session=expired",
        "/api/v1/users/123",
        "https://www.example.com:8080/path?query=1#section" };

    /**
     * Column names that typically hold URI data (e.g. from web_logs). Prefer these when present.
     */
    private static final Set<String> URI_LIKE_FIELD_NAMES = Set.of("uri", "url");

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        String inputExpression = pickUriInput(previousOutput);
        if (inputExpression == null) {
            return EMPTY_DESCRIPTION;  // no string column or literal to use, skip
        }
        String prefixRaw = EsqlQueryGenerator.randomIdentifier();
        String prefixForCmd = EsqlQueryGenerator.needsQuoting(prefixRaw) ? EsqlQueryGenerator.quote(prefixRaw) : prefixRaw;
        String cmdString = " | uri_parts " + prefixForCmd + " = " + inputExpression;
        return new CommandDescription(URI_PARTS, this, cmdString, Map.of(PREFIX, prefixRaw));
    }

    /**
     * Pick the input for URI_PARTS: either a literal valid URI (so we exercise the happy path)
     * or a string field, preferring columns named "uri" or "url" when present.
     */
    private static String pickUriInput(List<Column> previousOutput) {
        if (randomBoolean()) {
            return "\"" + randomFrom(LITERAL_URIS) + "\"";
        }
        return uriLikeFieldOrRandomString(previousOutput);
    }

    private static String uriLikeFieldOrRandomString(List<Column> previousOutput) {
        List<Column> stringColumns = previousOutput.stream()
            .filter(c -> "keyword".equals(c.type()) || "text".equals(c.type()))
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .toList();
        if (stringColumns.isEmpty()) {
            return null;
        }
        for (Column c : stringColumns) {
            String name = c.name();
            if (URI_LIKE_FIELD_NAMES.contains(EsqlQueryGenerator.unquote(name))) {
                return EsqlQueryGenerator.needsQuoting(name) ? EsqlQueryGenerator.quote(name) : name;
            }
        }
        Column chosen = randomFrom(stringColumns);
        String name = chosen.name();
        return EsqlQueryGenerator.needsQuoting(name) ? EsqlQueryGenerator.quote(name) : name;
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

        String prefix = (String) commandDescription.context().get(PREFIX);
        if (prefix == null) {
            return new ValidationResult(false, "Missing prefix in command context");
        }

        int expectedUriPartsColumns = URI_PARTS_OUTPUT_FIELDS.size();
        int expectedTotal = previousColumns.size() + expectedUriPartsColumns;
        if (columns.size() != expectedTotal) {
            return new ValidationResult(
                false,
                "Expecting ["
                    + expectedTotal
                    + "] columns ("
                    + previousColumns.size()
                    + " previous + "
                    + expectedUriPartsColumns
                    + " URI_PARTS), got ["
                    + columns.size()
                    + "]"
            );
        }

        var it = columns.iterator();
        int pos = 0;

        // Previous columns must appear first, in order, with the same name and type
        for (Column prev : previousColumns) {
            if (it.hasNext() == false) {
                return new ValidationResult(false, "Missing previous column [" + prev.name() + "] in output");
            }
            Column actual = it.next();
            pos++;
            if (actual.name().equals(prev.name()) == false) {
                return new ValidationResult(
                    false,
                    "At position " + pos + ": expected column [" + prev.name() + "], got [" + actual.name() + "]"
                );
            }
            if (actual.type().equals(prev.type()) == false) {
                return new ValidationResult(
                    false,
                    "Column [" + prev.name() + "] type changed from [" + prev.type() + "] to [" + actual.type() + "]"
                );
            }
        }

        // URI_PARTS columns must follow, in order, with the correct name and type
        for (Map.Entry<String, String> e : URI_PARTS_OUTPUT_FIELDS.entrySet()) {
            if (it.hasNext() == false) {
                return new ValidationResult(
                    false,
                    "Missing URI_PARTS column [" + prefix + "." + e.getKey() + "] (expected type [" + e.getValue() + "])"
                );
            }
            Column actual = it.next();
            pos++;
            String expectedName = prefix + "." + e.getKey();
            String expectedType = e.getValue();
            if (actual.name().equals(expectedName) == false) {
                return new ValidationResult(
                    false,
                    "At position " + pos + ": expected URI_PARTS column [" + expectedName + "], got [" + actual.name() + "]"
                );
            }
            if (actual.type().equals(expectedType) == false) {
                return new ValidationResult(
                    false,
                    "URI_PARTS column [" + expectedName + "] expected type [" + expectedType + "], got [" + actual.type() + "]"
                );
            }
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}
