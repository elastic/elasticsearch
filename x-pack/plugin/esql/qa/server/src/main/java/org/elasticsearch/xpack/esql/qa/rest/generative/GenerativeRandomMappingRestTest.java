/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.RandomMappingGenerator;
import org.elasticsearch.xpack.esql.generator.RandomMappingGenerator.GeneratedIndex;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Generative test that creates random Elasticsearch indices with varied mappings
 * and runs random ESQL queries against them. Extends {@link GenerativeRestTest}
 * to reuse its query execution, validation logic, and allowed-error infrastructure.
 * <p>
 * The indices share field names from a common pool, with a mix of compatible and
 * conflicting type mappings, exercising ESQL's union-type handling, cross-index
 * field resolution, and varied field settings.
 * <p>
 * Indices are created once per JVM for this test class; cleared by the parent's
 * {@code @AfterClass wipeTestData()} which deletes all indices.
 */
public abstract class GenerativeRandomMappingRestTest extends GenerativeRestTest {

    static final String INDEX_PREFIX = "gen_rm_";
    static final int RM_ITERATIONS = 50;
    static final int RM_MAX_DEPTH = 15;

    private static final String DEFAULT_METRIC_DEPRECATION = "Parameter [default_metric] is deprecated and will be removed in a future "
        + "version";

    private static final RequestOptions MAPPING_DEPRECATION_OPTIONS = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(warnings -> {
            for (String warning : warnings) {
                if (DEFAULT_METRIC_DEPRECATION.equals(warning) == false) {
                    return true;
                }
            }
            return false;
        })
        .build();

    private static volatile List<GeneratedIndex> generatedIndices;

    /**
     * Additional error patterns beyond {@link GenerativeRestTest#ALLOWED_ERRORS}
     * for errors caused by exotic field types (dense_vector, aggregate_metric_double,
     * histogram, range types, rank_feature, completion, etc.).
     */
    private static final Set<String> ADDITIONAL_ALLOWED_ERRORS = Set.of(
        // Type ambiguity across indices with conflicting mappings for the same field name
        //"Cannot use field \\[.*\\] due to ambiguities being mapped as \\[.*\\] in .*",
        // Unsupported types: dense_vector, aggregate_metric_double, histogram, etc.
        //"Cannot use field \\[.*\\] with unsupported type \\[unsupported\\]",
        //"found value \\[.*\\] type \\[unsupported\\]",
        //"Dense vector \\[.*\\] cannot be used",
        // Sorting limitations for exotic types
        //"cannot sort on \\[.*\\] of type \\[.*_range\\]",
        //"cannot sort on \\[.*\\] of type \\[geo_.*\\]",
        //"cannot sort on \\[.*\\] of type \\[shape\\]",
        //"cannot sort on \\[.*\\] of type \\[point\\]",
        //"cannot sort on \\[.*\\] of type \\[unsupported\\]",
        //"Sorting by range field \\[.*\\] is not supported",
        // Field operations on text/fielddata-disabled fields
        //"Fielddata is not supported on field \\[.*\\] of type \\[.*\\]",
        //"Text fields are not optimised for operations that require per-document field data",
        //"Fielddata is disabled on \\[.*\\] in \\[.*\\]",
        // Aggregate/metric type restrictions
        //"is not supported for \\[aggregate_metric_double\\]",
        //"is not supported for \\[histogram\\]",
        // Total fields limit
        //"Limit of total fields \\[.*\\] has been exceeded",
        // Partial results from range doc values reader bugs (known ES issue with date_range/other range types)
        //"RangeArrayBlock.*has \\[\\d+\\] positions instead of"
    );

    private static final Set<Pattern> ADDITIONAL_ALLOWED_PATTERNS = ADDITIONAL_ALLOWED_ERRORS.stream()
        .map(x -> ".*" + x + ".*")
        .map(x -> Pattern.compile(x, Pattern.DOTALL))
        .collect(Collectors.toSet());

    // ──────────────────────────────────────────────────
    // Setup / teardown
    // ──────────────────────────────────────────────────

    @Before
    public void setupRandomIndices() throws IOException {
        synchronized (GenerativeRandomMappingRestTest.class) {
            if (generatedIndices == null) {
                int numIndices = randomIntBetween(2, 5);
                List<GeneratedIndex> indices = RandomMappingGenerator.generateIndices(numIndices, INDEX_PREFIX);
                for (GeneratedIndex idx : indices) {
                    createRandomIndex(idx);
                    bulkLoadDocuments(idx);
                }
                generatedIndices = indices;
                logger.info("Created {} random mapping indices: {}", indices.size(), indexNames());
            }
        }
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return false;
    }

    @AfterClass
    public static void wipeRandomIndexState() {
        generatedIndices = null;
    }

    // ──────────────────────────────────────────────────
    // Test — simpler loop without lookup/enrich/unmapped tracking
    // ──────────────────────────────────────────────────

    @Override
    public void test() throws IOException {
        List<String> names = indexNames();
        CommandGenerator.QuerySchema schema = new CommandGenerator.QuerySchema(names, List.of(), List.of());

        for (int i = 0; i < RM_ITERATIONS; i++) {
            var exec = new EsqlQueryGenerator.Executor() {
                @Override
                public void run(CommandGenerator generator, CommandGenerator.CommandDescription current) {
                    final String command = current.commandString();
                    QueryExecuted result = previousResult == null
                        ? execute(command, 0)
                        : execute(previousResult.query() + command, previousResult.depth());

                    if (FromGenerator.hasApproximationSettings(result.query())) {
                        result = stripApproximationColumns(result);
                    }

                    final boolean hasException = result.exception() != null;
                    if (hasException
                        || checkPipelineResults(previousCommands, generator, current, previousResult, result, currentSchema)
                            .success() == false) {
                        if (hasException) {
                            checkPipelineException(result, previousCommands, currentSchema);
                        }
                        continueExecuting = false;
                        currentSchema = List.of();
                    } else {
                        continueExecuting = true;
                        currentSchema = result.outputSchema();
                    }

                    previousCommands.add(current);
                    previousResult = result;
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
                public List<Column> currentSchema() {
                    return currentSchema;
                }

                @Override
                public void clearCommandHistory() {
                    previousCommands = new ArrayList<>();
                    previousResult = null;
                }

                boolean continueExecuting;
                List<Column> currentSchema;
                List<CommandGenerator.CommandDescription> previousCommands = new ArrayList<>();
                QueryExecuted previousResult;
            };
            try {
                EsqlQueryGenerator.generatePipeline(RM_MAX_DEPTH, sourceCommand(), schema, exec, false, this);
            } catch (AssertionError ae) {
                // Thrown by checkPipelineResults/checkPipelineException via fail();
                // augment with full reproduction context.
                String query = exec.previousResult != null ? exec.previousResult.query() : null;
                throw new AssertionError(ae.getMessage() + formatReproductionContext(query), ae.getCause() != null ? ae.getCause() : ae);
            } catch (Exception e) {
                if (e.getMessage() != null && isAdditionalAllowedError(e.getMessage())) {
                    continue;
                }
                boolean knownError = false;
                if (e.getMessage() != null) {
                    for (Pattern allowedError : ALLOWED_ERROR_PATTERNS) {
                        if (isAllowedError(e.getMessage(), allowedError)) {
                            knownError = true;
                            break;
                        }
                    }
                }
                if (knownError == false) {
                    String query = exec.previousResult != null ? exec.previousResult.query() : null;
                    throw new AssertionError(
                        "Random mapping generative tests, error generating new command"
                            + formatReproductionContext(query),
                        e
                    );
                }
            }
        }
    }

    // ──────────────────────────────────────────────────
    // Extended error handling — adds random-mapping patterns
    // ──────────────────────────────────────────────────

    @Override
    protected CommandGenerator.ValidationResult checkPipelineResults(
        List<CommandGenerator.CommandDescription> previousCommands,
        CommandGenerator commandGenerator,
        CommandGenerator.CommandDescription commandDescription,
        QueryExecuted previousResult,
        QueryExecuted result,
        List<Column> currentSchema
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
            if (isAdditionalAllowedError(outputValidation.errorMessage())) {
                return CommandGenerator.VALIDATION_OK;
            }
            failOnUnexpectedValidationError(outputValidation, result, previousCommands, currentSchema);
        }
        return outputValidation;
    }

    @Override
    protected void checkPipelineException(
        QueryExecuted query,
        List<CommandGenerator.CommandDescription> previousCommands,
        List<Column> currentSchema
    ) {
        if (query.exception() != null && query.exception().getMessage() != null) {
            if (isAdditionalAllowedError(query.exception().getMessage())) {
                return;
            }
        }
        super.checkPipelineException(query, previousCommands, currentSchema);
    }

    private static boolean isAdditionalAllowedError(String errorMessage) {
        if (errorMessage == null) return false;
        for (Pattern p : ADDITIONAL_ALLOWED_PATTERNS) {
            if (p.matcher(errorMessage).matches()) {
                return true;
            }
        }
        return false;
    }

    // ──────────────────────────────────────────────────
    // Failure context — full reproduction commands
    // ──────────────────────────────────────────────────

    private static String formatReproductionContext(String query) {
        List<GeneratedIndex> indices = generatedIndices;
        if (indices == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder("\n\n=== Reproduction commands (copy-paste into Kibana Dev Tools) ===\n");
        for (GeneratedIndex idx : indices) {
            sb.append("\nPUT /").append(idx.name()).append("\n");
            sb.append("{\"mappings\":{").append(RandomMappingGenerator.toMappingJson(idx.fields())).append("}}\n");

            sb.append("\nPOST /").append(idx.name()).append("/_bulk?refresh=true\n");
            for (Map<String, Object> doc : idx.documents()) {
                if (doc.isEmpty()) {
                    continue;
                }
                sb.append("{\"index\":{}}\n");
                sb.append(RandomMappingGenerator.toDocumentJson(doc)).append("\n");
            }
        }
        if (query != null) {
            sb.append("\nPOST /_query\n");
            sb.append("{\"query\":\"").append(query.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"}\n");
        }
        sb.append("\n=== End of reproduction commands ===\n");
        return sb.toString();
    }

    // ──────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────

    private static List<String> indexNames() {
        List<String> names = new ArrayList<>();
        for (GeneratedIndex idx : generatedIndices) {
            names.add(idx.name());
        }
        return names;
    }

    // ──────────────────────────────────────────────────
    // Index creation and data loading
    // ──────────────────────────────────────────────────

    private void createRandomIndex(GeneratedIndex idx) throws IOException {
        String mappingJson = RandomMappingGenerator.toMappingJson(idx.fields());
        createIndex(idx.name(), null, mappingJson, null, MAPPING_DEPRECATION_OPTIONS);
        logger.info("Created index [{}] with mapping: {}", idx.name(), mappingJson);
    }

    @SuppressWarnings("unchecked")
    private void bulkLoadDocuments(GeneratedIndex idx) throws IOException {
        if (idx.documents().isEmpty()) {
            return;
        }
        StringBuilder bulk = new StringBuilder();
        int docCount = 0;
        for (Map<String, Object> doc : idx.documents()) {
            if (doc.isEmpty()) {
                continue;
            }
            bulk.append("{\"index\":{}}\n");
            bulk.append(RandomMappingGenerator.toDocumentJson(doc)).append("\n");
            docCount++;
        }
        if (docCount == 0) {
            return;
        }
        Request request = new Request("POST", "/" + idx.name() + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> responseBody = entityAsMap(response);
        Object errors = responseBody.get("errors");
        if (Boolean.TRUE.equals(errors)) {
            List<Map<String, Object>> items = (List<Map<String, Object>>) responseBody.get("items");
            int failureCount = 0;
            for (Map<String, Object> item : items) {
                Map<String, Object> indexResult = (Map<String, Object>) item.get("index");
                if (indexResult != null && indexResult.containsKey("error")) {
                    failureCount++;
                }
            }
            logger.warn(
                "Bulk load into [{}]: {} of {} documents had errors (expected for some type combinations)",
                idx.name(),
                failureCount,
                docCount
            );
        }
        logger.info("Loaded {} documents into [{}]", docCount, idx.name());
    }
}
