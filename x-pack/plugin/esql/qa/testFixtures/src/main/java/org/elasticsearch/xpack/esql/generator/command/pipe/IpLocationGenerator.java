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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomNonEmptySubsetOf;

/**
 * Generator for the IP_LOCATION pipe command. Produces {@code | IP_LOCATION <prefix> = <ip_expression>}, optionally
 * followed by a {@code WITH { ... }} options map exercising {@code first_only} (no schema impact) and/or
 * {@code properties} (a random non-empty subset/order of the default GeoLite2-City.mmdb fields, which narrows and
 * reorders the output columns). The {@code database_file} option is intentionally not generated, as it would change
 * the valid field set and depend on which databases are available in the test environment.
 */
public class IpLocationGenerator implements CommandGenerator {

    public static final CommandGenerator INSTANCE = new IpLocationGenerator();

    public static final String IP_LOCATION = "ip_location";

    private static final String PREFIX = "prefix";

    /**
     * Context key under which the effective output fields (field name -&gt; ES|QL type, in output order) chosen at
     * generation time are stored, so {@link #validateOutput} can reconstruct the expected schema.
     */
    private static final String OUTPUT_FIELDS = "outputFields";

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
        QueryExecutor executor,
        GenerationContext context
    ) {
        String inputExpression = pickIpInput(previousOutput);
        String prefixRaw = EsqlQueryGenerator.randomIdentifier();
        String prefixForCmd = EsqlQueryGenerator.needsQuoting(prefixRaw) ? EsqlQueryGenerator.quote(prefixRaw) : prefixRaw;

        // Effective output fields (field name -> type), in output order. Defaults to the full default set unless a
        // "properties" option narrows/reorders it below.
        LinkedHashMap<String, String> outputFields = new LinkedHashMap<>(IP_LOCATION_DEFAULT_OUTPUT_FIELDS);

        String withClause = "";
        if (randomBoolean()) {
            List<String> options = new ArrayList<>(2);
            if (randomBoolean()) {
                // first_only does not affect the output schema, only multi-value handling.
                options.add("\"first_only\": " + randomBoolean());
            }
            if (randomBoolean()) {
                // A random non-empty subset (and order) of the default fields. Empty lists are rejected by the parser,
                // so the subset is always non-empty.
                List<String> properties = randomNonEmptySubsetOf(IP_LOCATION_DEFAULT_OUTPUT_FIELDS.keySet());
                outputFields = new LinkedHashMap<>();
                StringBuilder list = new StringBuilder();
                for (int i = 0; i < properties.size(); i++) {
                    String field = properties.get(i);
                    outputFields.putLast(field, IP_LOCATION_DEFAULT_OUTPUT_FIELDS.get(field));
                    if (i > 0) {
                        list.append(", ");
                    }
                    list.append('"').append(field).append('"');
                }
                options.add("\"properties\": [" + list + "]");
            }
            withClause = options.isEmpty() ? " WITH {}" : " WITH { " + String.join(", ", options) + " }";
        }

        String cmdString = " | ip_location " + prefixForCmd + " = " + inputExpression + withClause;

        Map<String, Object> commandContext = new HashMap<>();
        commandContext.put(PREFIX, prefixRaw);
        commandContext.put(OUTPUT_FIELDS, outputFields);
        return new CommandDescription(IP_LOCATION, this, cmdString, commandContext);
    }

    private static String pickIpInput(List<Column> previousOutput) {
        // Prefer an existing field most of the time to exercise real field references through the planner; fall back to
        // a literal IP only when no usable field exists, or occasionally (~15%) for literal coverage.
        String field = ipLikeFieldOrRandomIpOrString(previousOutput);
        if (field != null && randomIntBetween(1, 100) <= 85) {
            return field;
        }
        return "\"" + randomFrom(LITERAL_IPS) + "\"";
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

        // Effective IP_LOCATION output fields (field name -> type, in output order), as chosen at generation time
        // (the full default set, or the subset/order from a "properties" option). Stored in the command context.
        @SuppressWarnings("unchecked")
        Map<String, String> outputFields = (Map<String, String>) commandDescription.context().get(OUTPUT_FIELDS);
        if (outputFields == null) {
            return new ValidationResult(false, "Missing output fields in command context");
        }

        // Reconstruct the expected merged output, mirroring NamedExpressions.mergeOutputExpressions: previous columns
        // whose names are not produced by IP_LOCATION keep their original order first, then the IP_LOCATION columns
        // follow. A previous column whose name collides with an IP_LOCATION column is dropped from its position and
        // replaced (it takes the new type and moves to the end), so we must not assume a plain "previous + new" layout
        // or that previous columns keep their original type.
        Set<String> ipLocationNames = new LinkedHashSet<>();
        for (String field : outputFields.keySet()) {
            ipLocationNames.add(prefix + "." + field);
        }
        LinkedHashMap<String, String> expected = new LinkedHashMap<>();
        for (Column prev : previousColumns) {
            if (ipLocationNames.contains(prev.name()) == false) {
                expected.put(prev.name(), prev.type());
            }
        }
        for (Map.Entry<String, String> e : outputFields.entrySet()) {
            expected.put(prefix + "." + e.getKey(), e.getValue());
        }

        if (columns.size() != expected.size()) {
            return new ValidationResult(
                false,
                "Expecting ["
                    + expected.size()
                    + "] distinct columns "
                    + expected.keySet()
                    + ", got ["
                    + columns.size()
                    + "] "
                    + columns.stream().map(Column::name).toList()
            );
        }

        var expectedIt = expected.entrySet().iterator();
        int pos = 0;
        for (Column actual : columns) {
            Map.Entry<String, String> exp = expectedIt.next();
            pos++;
            if (actual.name().equals(exp.getKey()) == false) {
                return new ValidationResult(
                    false,
                    "At position " + pos + ": expected column [" + exp.getKey() + "], got [" + actual.name() + "]"
                );
            }
            if (actual.type().equals(exp.getValue()) == false) {
                return new ValidationResult(
                    false,
                    "Column [" + exp.getKey() + "] expected type [" + exp.getValue() + "], got [" + actual.type() + "]"
                );
            }
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}
