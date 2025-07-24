/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.CommandGenerator;
import org.junit.AfterClass;
import org.junit.Before;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class GenerativeTimeseriesRestTest extends ESRestTestCase {

    public static final int ITERATIONS = 200; // More test cases
    public static final int MAX_DEPTH = 8;    // Fewer statements

    private static final String PROMETHEUS_IMAGE = "prom/prometheus:latest";
    private static GenericContainer<?> prometheusContainer;

    public static final Set<String> ALLOWED_ERRORS = Set.of(
//        "Reference \\[.*\\] is ambiguous",
//        "Cannot use field \\[.*\\] due to ambiguities",
//        "cannot sort on .*",
//        "argument of \\[count.*\\] must",
//        "Cannot use field \\[.*\\] with unsupported type \\[.*_range\\]",
//        "Unbounded sort not supported yet",
//        "The field names are too complex to process",
//        "must be \\[any type except counter types\\]",
//        "Unknown column \\[<all-fields-projected>\\]",
//        "Plan \\[ProjectExec\\[\\[<no-fields>.* optimized incorrectly due to missing references",
//        "optimized incorrectly due to missing references",
//        "The incoming YAML document exceeds the limit:",
//        "Data too large",
//        "Expecting the following columns \\[.*\\], got",
//        "Expecting at most \\[.*\\] columns, got \\[.*\\]"
    );

    public static final Set<Pattern> ALLOWED_ERROR_PATTERNS = ALLOWED_ERRORS.stream()
        .map(x -> ".*" + x + ".*")
        .map(x -> Pattern.compile(x, Pattern.DOTALL))
        .collect(Collectors.toSet());

    @Before
    public void setup() throws IOException {
        if (prometheusContainer == null) {
//            prometheusContainer = new GenericContainer<>(DockerImageName.parse(PROMETHEUS_IMAGE)).withExposedPorts(9090);
//            prometheusContainer.start();
        }

        if (indexExists("timeseries") == false) {
            TimeseriesDataGenerationHelper dataGen = new TimeseriesDataGenerationHelper();
            Request createIndex = new Request("PUT", "/timeseries");
            XContentBuilder builder = XContentFactory.jsonBuilder().map(dataGen.getMapping().raw());
            createIndex.setJsonEntity("{\"mappings\": " + Strings.toString(builder) + "}");
            client().performRequest(createIndex);

            for (int i = 0; i < 1000; i++) {
                Request indexRequest = new Request("POST", "/timeseries/_doc");
                indexRequest.setJsonEntity(dataGen.generateDocumentAsString());
                client().performRequest(indexRequest);
            }
            client().performRequest(new Request("POST", "/timeseries/_refresh"));
        }
    }

    @AfterClass
    public static void wipeTestData() throws IOException {
        if (prometheusContainer != null) {
            prometheusContainer.stop();
            prometheusContainer = null;
        }
        try {
            adminClient().performRequest(new Request("DELETE", "/*"));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    public void test() throws IOException {
        List<String> indices = List.of("timeseries");
        CommandGenerator.QuerySchema mappingInfo = new CommandGenerator.QuerySchema(indices, List.of(), List.of());
        EsqlQueryGenerator.QueryExecuted previousResult = null;
        for (int i = 0; i < ITERATIONS; i++) {
            List<CommandGenerator.CommandDescription> previousCommands = new ArrayList<>();
            CommandGenerator commandGenerator = EsqlQueryGenerator.sourceCommand();
            CommandGenerator.CommandDescription desc = commandGenerator.generate(List.of(), List.of(), mappingInfo);
            String command = desc.commandString();
            EsqlQueryGenerator.QueryExecuted result = execute(command, 0);
            if (result.exception() != null) {
                checkException(result);
                continue;
            }
            if (checkResults(List.of(), commandGenerator, desc, null, result).success() == false) {
                continue;
            }
            previousResult = result;
            previousCommands.add(desc);
            for (int j = 0; j < MAX_DEPTH; j++) {
                if (result.outputSchema().isEmpty()) {
                    break;
                }
                commandGenerator = EsqlQueryGenerator.randomPipeCommandGenerator();
                desc = commandGenerator.generate(previousCommands, result.outputSchema(), mappingInfo);
                if (desc == CommandGenerator.EMPTY_DESCRIPTION) {
                    continue;
                }
                command = desc.commandString();
                result = execute(result.query() + command, result.depth() + 1);
                if (result.exception() != null) {
                    checkException(result);
                    break;
                }
                if (checkResults(previousCommands, commandGenerator, desc, previousResult, result).success() == false) {
                    break;
                }
                previousCommands.add(desc);
                previousResult = result;
            }
        }
    }

    private static CommandGenerator.ValidationResult checkResults(
        List<CommandGenerator.CommandDescription> previousCommands,
        CommandGenerator commandGenerator,
        CommandGenerator.CommandDescription commandDescription,
        EsqlQueryGenerator.QueryExecuted previousResult,
        EsqlQueryGenerator.QueryExecuted result
    ) {
        CommandGenerator.ValidationResult outputValidation = commandGenerator.validateOutput(
            previousCommands,
            commandDescription,
            previousResult == null ? null : previousResult.outputSchema(),
            previousResult == null ? null : previousResult.result(),
            result.outputSchema(),
            result.result()
        );
        if (outputValidation.success() == false) {
            for (Pattern allowedError : ALLOWED_ERROR_PATTERNS) {
                if (allowedError.matcher(outputValidation.errorMessage()).matches()) {
                    return outputValidation;
                }
            }
            fail("query: " + result.query() + "\nerror: " + outputValidation.errorMessage());
        }
        return outputValidation;
    }

    private void checkException(EsqlQueryGenerator.QueryExecuted query) {
        for (Pattern allowedError : ALLOWED_ERROR_PATTERNS) {
            if (allowedError.matcher(query.exception().getMessage()).matches()) {
                return;
            }
        }
        fail("query: " + query.query() + "\nexception: " + query.exception().getMessage());
    }

    @SuppressWarnings("unchecked")
    private EsqlQueryGenerator.QueryExecuted execute(String command, int depth) {
        try {
            Map<String, Object> a = RestEsqlTestCase.runEsql(
                new RestEsqlTestCase.RequestObjectBuilder().query(command).build(),
                new AssertWarnings.AllowedRegexes(List.of(Pattern.compile(".*"))),
                RestEsqlTestCase.Mode.SYNC
            );
            List<EsqlQueryGenerator.Column> outputSchema = outputSchema(a);
            List<List<Object>> values = (List<List<Object>>) a.get("values");
            return new EsqlQueryGenerator.QueryExecuted(command, depth, outputSchema, values, null);
        } catch (Exception e) {
            return new EsqlQueryGenerator.QueryExecuted(command, depth, null, null, e);
        } catch (AssertionError ae) {
            return new EsqlQueryGenerator.QueryExecuted(command, depth, null, null, new RuntimeException(ae.getMessage()));
        }
    }

    @SuppressWarnings("unchecked")
    private List<EsqlQueryGenerator.Column> outputSchema(Map<String, Object> a) {
        List<Map<String, String>> cols = (List<Map<String, String>>) a.get("columns");
        if (cols == null) {
            return null;
        }
        return cols.stream().map(x -> new EsqlQueryGenerator.Column(x.get("name"), x.get("type"))).collect(Collectors.toList());
    }
}
