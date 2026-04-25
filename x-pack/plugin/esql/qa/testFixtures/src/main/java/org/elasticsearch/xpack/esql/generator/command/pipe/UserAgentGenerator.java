/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.DEVICE_NAME;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.NAME;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.OS_FULL;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.OS_NAME;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.OS_VERSION;
import static org.elasticsearch.useragent.api.UserAgentParsedInfo.VERSION;

/**
 * Generator for the USER_AGENT pipe command. Produces {@code | user_agent <prefix> = <ua_expression>}
 * with no WITH clause (default properties and extract_device_type=false).
 */
public class UserAgentGenerator implements CommandGenerator {

    public static final CommandGenerator INSTANCE = new UserAgentGenerator();

    public static final String USER_AGENT = "user_agent";

    private static final String PREFIX = "prefix";

    /**
     * Default output fields when USER_AGENT is used without WITH clause: extractDeviceType=false,
     * properties=["name", "version", "os", "device"] → device.name but not device.type.
     */
    private static final LinkedHashMap<String, String> USER_AGENT_DEFAULT_OUTPUT_FIELDS = new LinkedHashMap<>();
    static {
        USER_AGENT_DEFAULT_OUTPUT_FIELDS.put(NAME, "keyword");
        USER_AGENT_DEFAULT_OUTPUT_FIELDS.put(VERSION, "keyword");
        USER_AGENT_DEFAULT_OUTPUT_FIELDS.put(OS_NAME, "keyword");
        USER_AGENT_DEFAULT_OUTPUT_FIELDS.put(OS_VERSION, "keyword");
        USER_AGENT_DEFAULT_OUTPUT_FIELDS.put(OS_FULL, "keyword");
        USER_AGENT_DEFAULT_OUTPUT_FIELDS.put(DEVICE_NAME, "keyword");
    }

    private static final String[] LITERAL_USER_AGENTS = new String[] {
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.149 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "curl/7.68.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
        "FileZilla/3.57.0",
        "Go-http-client/1.1",
        "Uptime-Robot/1.0" };

    private static final Set<String> USER_AGENT_LIKE_FIELD_NAMES = Set.of("user_agent", "useragent", "ua");

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        String inputExpression = pickUserAgentInput(previousOutput);
        if (inputExpression == null) {
            return EMPTY_DESCRIPTION;
        }
        String prefixRaw = EsqlQueryGenerator.randomIdentifier();
        String prefixForCmd = EsqlQueryGenerator.needsQuoting(prefixRaw) ? EsqlQueryGenerator.quote(prefixRaw) : prefixRaw;
        String cmdString = " | user_agent " + prefixForCmd + " = " + inputExpression;
        return new CommandDescription(USER_AGENT, this, cmdString, Map.of(PREFIX, prefixRaw));
    }

    private static String pickUserAgentInput(List<Column> previousOutput) {
        if (randomBoolean()) {
            return "\"" + randomFrom(LITERAL_USER_AGENTS) + "\"";
        }
        return userAgentLikeFieldOrRandomString(previousOutput);
    }

    private static String userAgentLikeFieldOrRandomString(List<Column> previousOutput) {
        List<Column> stringColumns = previousOutput.stream()
            .filter(c -> "keyword".equals(c.type()) || "text".equals(c.type()))
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .toList();
        if (stringColumns.isEmpty()) {
            return null;
        }
        for (Column c : stringColumns) {
            String name = c.name();
            if (USER_AGENT_LIKE_FIELD_NAMES.contains(EsqlQueryGenerator.unquote(name))) {
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

        int expectedColumns = USER_AGENT_DEFAULT_OUTPUT_FIELDS.size();
        int expectedTotal = previousColumns.size() + expectedColumns;
        if (columns.size() != expectedTotal) {
            return new ValidationResult(
                false,
                "Expecting ["
                    + expectedTotal
                    + "] columns ("
                    + previousColumns.size()
                    + " previous + "
                    + expectedColumns
                    + " USER_AGENT), got ["
                    + columns.size()
                    + "]"
            );
        }

        var it = columns.iterator();
        int pos = 0;

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

        for (Map.Entry<String, String> e : USER_AGENT_DEFAULT_OUTPUT_FIELDS.entrySet()) {
            if (it.hasNext() == false) {
                return new ValidationResult(
                    false,
                    "Missing USER_AGENT column [" + prefix + "." + e.getKey() + "] (expected type [" + e.getValue() + "])"
                );
            }
            Column actual = it.next();
            pos++;
            String expectedName = prefix + "." + e.getKey();
            String expectedType = e.getValue();
            if (actual.name().equals(expectedName) == false) {
                return new ValidationResult(
                    false,
                    "At position " + pos + ": expected USER_AGENT column [" + expectedName + "], got [" + actual.name() + "]"
                );
            }
            if (actual.type().equals(expectedType) == false) {
                return new ValidationResult(
                    false,
                    "USER_AGENT column [" + expectedName + "] expected type [" + expectedType + "], got [" + actual.type() + "]"
                );
            }
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}
