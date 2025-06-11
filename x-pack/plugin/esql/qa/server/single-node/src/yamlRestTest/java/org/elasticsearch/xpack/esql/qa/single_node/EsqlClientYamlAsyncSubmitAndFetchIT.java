/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponseException;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.xcontent.XContentLocation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Run the ESQL yaml tests async and then fetch the results with a long wait time.
 */
public class EsqlClientYamlAsyncSubmitAndFetchIT extends AbstractEsqlClientYamlIT {
    public EsqlClientYamlAsyncSubmitAndFetchIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return updateEsqlQueryDoSections(partialResultsCheckingParatmers(), DoEsqlAsync::new);
    }

    private static class DoEsqlAsync implements ExecutableSection {
        private final DoSection original;

        private DoEsqlAsync(DoSection original) {
            this.original = original;
        }

        @Override
        public XContentLocation getLocation() {
            return original.getLocation();
        }

        @Override
        public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
            try {
                // Start the query
                List<Map<String, Object>> bodies = original.getApiCallSection().getBodies().stream().map(m -> {
                    Map<String, Object> body = new HashMap<>(m);
                    if (randomBoolean()) {
                        /*
                         * Try to force the request to go async by setting the timeout to 0.
                         * This doesn't *actually* force the request async - if it finishes
                         * super duper faster it won't get async. But that's life.
                         */
                        body.put("wait_for_completion_timeout", "0ms");
                    }
                    return body;
                }).toList();
                ClientYamlTestResponse startResponse = executionContext.callApi(
                    "esql.async_query",
                    original.getApiCallSection().getParams(),
                    bodies,
                    original.getApiCallSection().getHeaders(),
                    original.getApiCallSection().getNodeSelector()
                );

                String id = startResponse.evaluate("id");
                boolean finishedEarly = id == null;
                if (finishedEarly) {
                    /*
                     * If we finished early, make sure we don't have a "catch"
                     * param and expect and error. And make sure we match the
                     * warnings folks have asked for.
                     */
                    original.failIfHasCatch(startResponse);
                    original.checkWarningHeaders(startResponse.getWarningHeaders(), testPath(executionContext));
                    return;
                }

                /*
                 * Ok, we didn't finish before the timeout. Fine, let's fetch the result.
                 */
                Map<String, String> params = new HashMap<>();
                params.put("wait_for_completion_timeout", "30m");
                params.put("id", id);
                String dropNullColumns = original.getApiCallSection().getParams().get("drop_null_columns");
                if (dropNullColumns != null) {
                    params.put("drop_null_columns", dropNullColumns);
                }
                ClientYamlTestResponse fetchResponse = executionContext.callApi(
                    "esql.async_query_get",
                    params,
                    List.of(),
                    original.getApiCallSection().getHeaders(),
                    original.getApiCallSection().getNodeSelector()
                );
                original.failIfHasCatch(fetchResponse);
                original.checkWarningHeaders(fetchResponse.getWarningHeaders(), testPath(executionContext));
            } catch (ClientYamlTestResponseException e) {
                original.checkResponseException(e, executionContext);
            }
        }

        private String testPath(ClientYamlTestExecutionContext executionContext) {
            return executionContext.getClientYamlTestCandidate() != null
                ? executionContext.getClientYamlTestCandidate().getTestPath()
                : null;
        }
    }
}
