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

/**
 * Generator for the IP_LOCATION pipe command. Produces {@code | IP_LOCATION <prefix> = <ip_expression>}
 * with no WITH clause (default database GeoLite2-City.mmdb, default properties).
 */
public class IpLocationGenerator implements CommandGenerator {

    public static final CommandGenerator INSTANCE = new IpLocationGenerator();

    public static final String IP_LOCATION = "ip_location";

    private static final String PREFIX = "prefix";

    /**
     * Default output fields for GeoLite2-City.mmdb with default properties, matching
     * {@code Database.City.defaultProperties()} sorted by {@code DatabaseProperty} ordinal.
     */
    private static final LinkedHashMap<String, String> IP_LOCATION_DEFAULT_OUTPUT_FIELDS = new LinkedHashMap<>();
    static {
        IP_LOCATION_DEFAULT_OUTPUT_FIELDS.put("country_iso_code", "keyword");
        IP_LOCATION_DEFAULT_OUTPUT_FIELDS.put("country_name", "keyword");
        IP_LOCATION_DEFAULT_OUTPUT_FIELDS.put("continent_name", "keyword");
        IP_LOCATION_DEFAULT_OUTPUT_FIELDS.put("region_iso_code", "keyword");
        IP_LOCATION_DEFAULT_OUTPUT_FIELDS.put("region_name", "keyword");
        IP_LOCATION_DEFAULT_OUTPUT_FIELDS.put("city_name", "keyword");
        IP_LOCATION_DEFAULT_OUTPUT_FIELDS.put("location", "geo_point");
    }

    private static final String[] LITERAL_IPS = new String[] {
        "89.160.20.128",
        "2602:306:33d3:8000::3257:9652",
        "82.170.213.79",
        "175.16.199.0",
        "8.8.8.8",
        "93.184.216.34" };

    private static final Set<String> IP_LIKE_FIELD_NAMES = Set.of("ip", "ip_addr", "ip_address", "client_ip", "source_ip", "dest_ip");

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        String inputExpression = pickIpInput(previousOutput);
        if (inputExpression == null) {
            return EMPTY_DESCRIPTION;
        }
        String prefixRaw = EsqlQueryGenerator.randomIdentifier();
        String prefixForCmd = EsqlQueryGenerator.needsQuoting(prefixRaw) ? EsqlQueryGenerator.quote(prefixRaw) : prefixRaw;
        String cmdString = " | ip_location " + prefixForCmd + " = " + inputExpression;
        return new CommandDescription(IP_LOCATION, this, cmdString, Map.of(PREFIX, prefixRaw));
    }

    private static String pickIpInput(List<Column> previousOutput) {
        if (randomBoolean()) {
            return "\"" + randomFrom(LITERAL_IPS) + "\"";
        }
        return ipLikeFieldOrRandomIpOrString(previousOutput);
    }

    private static String ipLikeFieldOrRandomIpOrString(List<Column> previousOutput) {
        List<Column> ipColumns = previousOutput.stream()
            .filter(c -> "ip".equals(c.type()))
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .toList();
        if (ipColumns.isEmpty() == false) {
            for (Column c : ipColumns) {
                if (IP_LIKE_FIELD_NAMES.contains(EsqlQueryGenerator.unquote(c.name()))) {
                    String name = c.name();
                    return EsqlQueryGenerator.needsQuoting(name) ? EsqlQueryGenerator.quote(name) : name;
                }
            }
            Column chosen = randomFrom(ipColumns);
            String name = chosen.name();
            return EsqlQueryGenerator.needsQuoting(name) ? EsqlQueryGenerator.quote(name) : name;
        }

        List<Column> stringColumns = previousOutput.stream()
            .filter(c -> "keyword".equals(c.type()) || "text".equals(c.type()))
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .toList();
        if (stringColumns.isEmpty()) {
            return null;
        }
        for (Column c : stringColumns) {
            if (IP_LIKE_FIELD_NAMES.contains(EsqlQueryGenerator.unquote(c.name()))) {
                String name = c.name();
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

        int expectedColumns = IP_LOCATION_DEFAULT_OUTPUT_FIELDS.size();
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
                    + " IP_LOCATION), got ["
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

        for (Map.Entry<String, String> e : IP_LOCATION_DEFAULT_OUTPUT_FIELDS.entrySet()) {
            if (it.hasNext() == false) {
                return new ValidationResult(
                    false,
                    "Missing IP_LOCATION column [" + prefix + "." + e.getKey() + "] (expected type [" + e.getValue() + "])"
                );
            }
            Column actual = it.next();
            pos++;
            String expectedName = prefix + "." + e.getKey();
            String expectedType = e.getValue();
            if (actual.name().equals(expectedName) == false) {
                return new ValidationResult(
                    false,
                    "At position " + pos + ": expected IP_LOCATION column [" + expectedName + "], got [" + actual.name() + "]"
                );
            }
            if (actual.type().equals(expectedType) == false) {
                return new ValidationResult(
                    false,
                    "IP_LOCATION column [" + expectedName + "] expected type [" + expectedType + "], got [" + actual.type() + "]"
                );
            }
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}
