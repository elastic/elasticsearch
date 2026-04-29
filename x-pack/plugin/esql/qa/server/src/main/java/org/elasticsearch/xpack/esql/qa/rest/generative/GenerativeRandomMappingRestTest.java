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
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

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

    private static final RequestOptions MAPPING_DEPRECATION_OPTIONS = RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> {
        for (String warning : warnings) {
            if (DEFAULT_METRIC_DEPRECATION.equals(warning) == false) {
                return true;
            }
        }
        return false;
    }).build();

    private static volatile List<GeneratedIndex> generatedIndices;

    private static final Pattern CANNOT_LOAD_BLOCKS_WITHOUT_DOC_VALUES = Pattern.compile(
        ".*Cannot load blocks without doc values.*",
        Pattern.DOTALL
    );

    // FORK converts UnsupportedAttribute to a plain ReferenceAttribute(UNSUPPORTED), stripping the
    // Unresolvable marker. Functions then reject the argument with a generic "type [unsupported]" error
    // instead of the proper "Cannot use field" message. https://github.com/elastic/elasticsearch/issues/147094
    private static final Pattern UNSUPPORTED_TYPE_IN_FUNCTION = Pattern.compile(
        ".*argument of \\[.*\\] must be \\[.*\\], found value \\[.*\\] type \\[unsupported\\].*",
        Pattern.DOTALL
    );

    // Full-text functions (match, match_phrase, qstr) pushed down to Lucene can fail with
    // "failed to create query" on random mappings where field types are incompatible (e.g., non-indexed
    // fields, numeric fields receiving text input). https://github.com/elastic/elasticsearch/issues/147610
    private static final Pattern FAILED_TO_CREATE_QUERY = Pattern.compile(".*failed to create query.*", Pattern.DOTALL);

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
                String errorMessage = e.getMessage();
                String query = exec.previousResult != null ? exec.previousResult.query() : null;
                if (isAdditionalAllowedError(errorMessage, query) || isAllowedError(errorMessage)) {
                    continue;
                }
                throw new AssertionError(
                    "Random mapping generative tests, error generating new command" + formatReproductionContext(query),
                    e
                );
            }
        }
    }

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
            if (isAdditionalAllowedError(query.exception().getMessage(), query.query())) {
                return;
            }
        }
        super.checkPipelineException(query, previousCommands, currentSchema);
    }

    private static boolean isAdditionalAllowedError(String errorMessage) {
        return isAdditionalAllowedError(errorMessage, null);
    }

    private static boolean isAdditionalAllowedError(String errorMessage, String query) {
        if (errorMessage == null) return false;
        // RangeFieldMapper throws "Cannot load blocks without doc values" for *_range fields
        // with doc_values:false. https://github.com/elastic/elasticsearch/issues/146527
        if (isAllowedError(errorMessage, CANNOT_LOAD_BLOCKS_WITHOUT_DOC_VALUES) && hasRangeFieldType()) {
            return true;
        }
        // FORK converts UnsupportedAttribute to ReferenceAttribute(UNSUPPORTED), causing functions
        // to reject the field with a generic "type [unsupported]" error.
        // https://github.com/elastic/elasticsearch/issues/147094
        if (query != null && isAllowedError(errorMessage, UNSUPPORTED_TYPE_IN_FUNCTION) && queryContainsFork(query)) {
            return true;
        }
        // Full-text functions pushed to Lucene can throw QueryShardException("failed to create query: ...")
        // when the target field has an incompatible type or is not indexed in the random mapping.
        // https://github.com/elastic/elasticsearch/issues/147610
        if (query != null && isAllowedError(errorMessage, FAILED_TO_CREATE_QUERY) && queryContainsFullTextFunction(query)) {
            return true;
        }
        return false;
    }

    private static boolean hasRangeFieldType() {
        List<GeneratedIndex> indices = generatedIndices;
        if (indices == null) {
            return false;
        }
        for (GeneratedIndex idx : indices) {
            for (RandomMappingGenerator.FieldDef field : idx.fields()) {
                if (field.esType().endsWith("_range")) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean queryContainsFork(String query) {
        return query != null && query.toUpperCase(Locale.ROOT).contains("FORK");
    }

    private static boolean queryContainsFullTextFunction(String query) {
        if (query == null) {
            return false;
        }
        String upper = query.toUpperCase(Locale.ROOT);
        return upper.contains("MATCH(") || upper.contains("MATCH_PHRASE(") || upper.contains("QSTR(") || upper.contains("KQL(");
    }

    private static String formatReproductionContext(String query) {
        List<GeneratedIndex> indices = generatedIndices;
        if (indices == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder("\n\n=== Reproduction commands ===\n");
        for (GeneratedIndex idx : indices) {
            sb.append("\nPUT /").append(idx.name()).append("\n");
            sb.append("{\"mappings\":{").append(RandomMappingGenerator.toMappingJson(idx.fields())).append("}}\n");
        }

        StringBuilder bulkBody = new StringBuilder();
        for (GeneratedIndex idx : indices) {
            for (Map<String, Object> doc : idx.documents()) {
                if (doc.isEmpty()) {
                    continue;
                }
                bulkBody.append("{\"index\":{\"_index\":\"").append(idx.name()).append("\"}}\n");
                bulkBody.append(RandomMappingGenerator.toDocumentJson(doc)).append("\n");
            }
        }
        if (bulkBody.isEmpty() == false) {
            sb.append("\nPOST /_bulk?refresh=true\n");
            sb.append(bulkBody);
        }
        if (query != null) {
            sb.append("\nPOST /_query\n");
            sb.append("{\"query\":\"").append(escapeJsonString(query)).append("\"}\n");
        }
        sb.append("\n=== End of reproduction commands ===\n");
        return sb.toString();
    }

    private static String escapeJsonString(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    private static List<String> indexNames() {
        List<String> names = new ArrayList<>();
        for (GeneratedIndex idx : generatedIndices) {
            names.add(idx.name());
        }
        return names;
    }

    private void createRandomIndex(GeneratedIndex idx) throws IOException {
        String mappingJson = RandomMappingGenerator.toMappingJson(idx.fields());
        createIndex(idx.name(), null, mappingJson, null, MAPPING_DEPRECATION_OPTIONS);
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
    }
}
