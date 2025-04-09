/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.AfterClass;
import org.junit.Before;

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

    public static final int ITERATIONS = 100;
    public static final int MAX_DEPTH = 20;

    public static final Set<String> ALLOWED_ERRORS = Set.of(
        "Reference \\[.*\\] is ambiguous",
        "Cannot use field \\[.*\\] due to ambiguities",
        "cannot sort on .*",
        "argument of \\[count_distinct\\(.*\\)\\] must",
        "Cannot use field \\[.*\\] with unsupported type \\[.*_range\\]",
        "Unbounded sort not supported yet",
        "The field names are too complex to process", // field_caps problem
        "must be \\[any type except counter types\\]", // TODO refine the generation of count()

        // warnings
        "Field '.*' shadowed by field at line .*",
        "evaluation of \\[.*\\] failed, treating result as null", // TODO investigate?

        // Awaiting fixes
        "Unknown column \\[<all-fields-projected>\\]", // https://github.com/elastic/elasticsearch/issues/121741,
        "Plan \\[ProjectExec\\[\\[<no-fields>.* optimized incorrectly due to missing references", // https://github.com/elastic/elasticsearch/issues/125866
        "only supports KEYWORD or TEXT values, found expression", // https://github.com/elastic/elasticsearch/issues/126017
        "token recognition error at: '``", // https://github.com/elastic/elasticsearch/issues/125870
        "Unknown column \\[.*\\]", // https://github.com/elastic/elasticsearch/issues/126026
        "optimized incorrectly due to missing references", // https://github.com/elastic/elasticsearch/issues/116781
        "No matches found for pattern", // https://github.com/elastic/elasticsearch/issues/126418
        "JOIN left field .* is incompatible with right field", // https://github.com/elastic/elasticsearch/issues/126419
        "The incoming YAML document exceeds the limit:" // still to investigate, but it seems to be specific to the test framework
    );

    public static final Set<Pattern> ALLOWED_ERROR_PATTERNS = ALLOWED_ERRORS.stream()
        .map(x -> ".*" + x + ".*")
        .map(x -> Pattern.compile(x, Pattern.DOTALL))
        .collect(Collectors.toSet());

    @Before
    public void setup() throws IOException {
        if (indexExists(CSV_DATASET_MAP.keySet().iterator().next()) == false) {
            loadDataSetIntoEs(client(), true, supportsSourceFieldMapping());
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
        for (int i = 0; i < ITERATIONS; i++) {
            String command = EsqlQueryGenerator.sourceCommand(indices);
            EsqlQueryGenerator.QueryExecuted result = execute(command, 0);
            if (result.exception() != null) {
                checkException(result);
                continue;
            }
            for (int j = 0; j < MAX_DEPTH; j++) {
                if (result.outputSchema().isEmpty()) {
                    break;
                }
                command = EsqlQueryGenerator.pipeCommand(result.outputSchema(), policies, lookupIndices);
                result = execute(result.query() + command, result.depth() + 1);
                if (result.exception() != null) {
                    checkException(result);
                    break;
                }
            }
        }
    }

    private void checkException(EsqlQueryGenerator.QueryExecuted query) {
        for (Pattern allowedError : ALLOWED_ERROR_PATTERNS) {
            if (allowedError.matcher(query.exception().getMessage()).matches()) {
                return;
            }
        }
        fail("query: " + query.query() + "\nexception: " + query.exception().getMessage());
    }

    private EsqlQueryGenerator.QueryExecuted execute(String command, int depth) {
        try {
            Map<String, Object> a = RestEsqlTestCase.runEsqlSync(new RestEsqlTestCase.RequestObjectBuilder().query(command).build());
            List<EsqlQueryGenerator.Column> outputSchema = outputSchema(a);
            return new EsqlQueryGenerator.QueryExecuted(command, depth, outputSchema, null);
        } catch (Exception e) {
            return new EsqlQueryGenerator.QueryExecuted(command, depth, null, e);
        } catch (AssertionError ae) {
            // this is for ensureNoWarnings()
            return new EsqlQueryGenerator.QueryExecuted(command, depth, null, new RuntimeException(ae.getMessage()));
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

    private List<String> availableIndices() throws IOException {
        return availableDatasetsForEs(client(), true, supportsSourceFieldMapping()).stream()
            .filter(x -> x.requiresInferenceEndpoint() == false)
            .map(x -> x.indexName())
            .toList();
    }

    record LookupIdx(String idxName, String key, String keyType) {}

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
