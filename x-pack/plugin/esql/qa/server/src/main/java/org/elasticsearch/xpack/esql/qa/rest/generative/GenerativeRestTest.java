/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.CommandGenerator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET_MAP;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.ENRICH_POLICIES;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.availableDatasetsForEs;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.loadDataSetIntoEs;

public abstract class GenerativeRestTest extends ESRestTestCase {

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    public static final int ITERATIONS = 100;
    public static final int MAX_DEPTH = 20;

    public static final Set<String> ALLOWED_ERRORS = Set.of(
        "Reference \\[.*\\] is ambiguous",
        "Cannot use field \\[.*\\] due to ambiguities",
        "cannot sort on .*",
        "argument of \\[count.*\\] must",
        "Cannot use field \\[.*\\] with unsupported type \\[.*\\]",
        "Unbounded sort not supported yet",
        "The field names are too complex to process", // field_caps problem
        "must be \\[any type except counter types\\]", // TODO refine the generation of count()

        // Awaiting fixes for query failure
        "Unknown column \\[<all-fields-projected>\\]", // https://github.com/elastic/elasticsearch/issues/121741,
        "Plan \\[ProjectExec\\[\\[<no-fields>.* optimized incorrectly due to missing references", // https://github.com/elastic/elasticsearch/issues/125866
        "The incoming YAML document exceeds the limit:", // still to investigate, but it seems to be specific to the test framework
        "Data too large", // Circuit breaker exceptions eg. https://github.com/elastic/elasticsearch/issues/130072
        "optimized incorrectly due to missing references", // https://github.com/elastic/elasticsearch/issues/131509

        // Awaiting fixes for correctness
        "Expecting at most \\[.*\\] columns, got \\[.*\\]" // https://github.com/elastic/elasticsearch/issues/129561
    );

    public static final Set<Pattern> ALLOWED_ERROR_PATTERNS = ALLOWED_ERRORS.stream()
        .map(x -> ".*" + x + ".*")
        .map(x -> Pattern.compile(x, Pattern.DOTALL))
        .collect(Collectors.toSet());

    @Before
    public void setup() throws IOException {
        if (indexExists(CSV_DATASET_MAP.keySet().iterator().next()) == false) {
            loadDataSetIntoEs(client(), true, supportsSourceFieldMapping(), false);
        }
    }

    protected abstract boolean supportsSourceFieldMapping();

    @AfterClass
    public static void wipeTestData() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "/*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    public void test() throws IOException {
        List<String> indices = availableIndices();
        List<LookupIdx> lookupIndices = lookupIndices();
        List<CsvTestsDataLoader.EnrichConfig> policies = availableEnrichPolicies();
        CommandGenerator.QuerySchema mappingInfo = new CommandGenerator.QuerySchema(indices, lookupIndices, policies);

        for (int i = 0; i < ITERATIONS; i++) {
            var exec = new EsqlQueryGenerator.Executor() {
                @Override
                public void run(CommandGenerator generator, CommandGenerator.CommandDescription current) {
                    previousCommands.add(current);
                    final String command = current.commandString();

                    final EsqlQueryGenerator.QueryExecuted result = previousResult == null
                        ? execute(command, 0, profileLogger)
                        : execute(previousResult.query() + command, previousResult.depth(), profileLogger);
                    previousResult = result;

                    final boolean hasException = result.exception() != null;
                    if (hasException || checkResults(List.of(), generator, current, previousResult, result).success() == false) {
                        if (hasException) {
                            checkException(result);
                        }
                        continueExecuting = false;
                        currentSchema = List.of();
                    } else {
                        continueExecuting = true;
                        currentSchema = result.outputSchema();
                    }
                }

                @Override
                public List<CommandGenerator.CommandDescription> previousCommands() {
                    return previousCommands;
                }

                @Override
                public boolean continueExecuting() {
                    return continueExecuting;
                }

                @Override
                public List<EsqlQueryGenerator.Column> currentSchema() {
                    return currentSchema;
                }

                boolean continueExecuting;
                List<EsqlQueryGenerator.Column> currentSchema;
                final List<CommandGenerator.CommandDescription> previousCommands = new ArrayList<>();
                EsqlQueryGenerator.QueryExecuted previousResult;
            };
            EsqlQueryGenerator.generatePipeline(MAX_DEPTH, EsqlQueryGenerator.sourceCommand(), mappingInfo, exec);
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
    public static EsqlQueryGenerator.QueryExecuted execute(String command, int depth, @Nullable ProfileLogger profileLogger) {
        try {
            Map<String, Object> json = RestEsqlTestCase.runEsql(
                new RestEsqlTestCase.RequestObjectBuilder().query(command).build(),
                new AssertWarnings.AllowedRegexes(List.of(Pattern.compile(".*"))),// we don't care about warnings
                profileLogger,
                RestEsqlTestCase.Mode.SYNC
            );
            List<EsqlQueryGenerator.Column> outputSchema = outputSchema(json);
            List<List<Object>> values = (List<List<Object>>) json.get("values");
            return new EsqlQueryGenerator.QueryExecuted(command, depth, outputSchema, values, null);
        } catch (Exception e) {
            return new EsqlQueryGenerator.QueryExecuted(command, depth, null, null, e);
        } catch (AssertionError ae) {
            // this is for ensureNoWarnings()
            return new EsqlQueryGenerator.QueryExecuted(command, depth, null, null, new RuntimeException(ae.getMessage()));
        }

    }

    @SuppressWarnings("unchecked")
    private static List<EsqlQueryGenerator.Column> outputSchema(Map<String, Object> a) {
        List<Map<String, String>> cols = (List<Map<String, String>>) a.get("columns");
        if (cols == null) {
            return null;
        }
        return cols.stream().map(x -> new EsqlQueryGenerator.Column(x.get("name"), x.get("type"))).collect(Collectors.toList());
    }

    private List<String> availableIndices() throws IOException {
        return availableDatasetsForEs(true, supportsSourceFieldMapping(), false).stream()
            .filter(x -> x.requiresInferenceEndpoint() == false)
            .map(x -> x.indexName())
            .toList();
    }

    public record LookupIdx(String idxName, String key, String keyType) {}

    private List<LookupIdx> lookupIndices() {
        List<LookupIdx> result = new ArrayList<>();
        // we don't have key info from the dataset loader, let's hardcode it for now
        result.add(new LookupIdx("languages_lookup", "language_code", "integer"));
        result.add(new LookupIdx("message_types_lookup", "message", "keyword"));
        return result;
    }

    List<CsvTestsDataLoader.EnrichConfig> availableEnrichPolicies() {
        return ENRICH_POLICIES;
    }
}
