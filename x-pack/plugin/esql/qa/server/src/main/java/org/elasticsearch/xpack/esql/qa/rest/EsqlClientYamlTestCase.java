/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for the ES|QL yaml suites. Before every ES|QL query it waits until all nodes have applied every cluster state published
 * so far. A {@code bulk} with {@code refresh=true} that adds a field through dynamic mapping can return before some search nodes have
 * applied the new mapping, and ES|QL resolves its columns from a single node's local mapping, so a query issued right after such a
 * bulk could otherwise fail with {@code Unknown column}. See https://github.com/elastic/elasticsearch-serverless/issues/7829.
 */
public abstract class EsqlClientYamlTestCase extends ESClientYamlSuiteTestCase {

    private static final Set<String> QUERY_APIS = Set.of("esql.query", "esql.async_query");

    protected EsqlClientYamlTestCase(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        Set<String> nodesVersions,
        TestFeatureService testFeatureService,
        Set<String> osSet
    ) {
        return new ClientYamlTestExecutionContext(
            clientYamlTestCandidate,
            clientYamlTestClient,
            randomizeContentType(),
            nodesVersions,
            testFeatureService,
            osSet
        ) {
            @Override
            public ClientYamlTestResponse callApi(
                String apiName,
                String method,
                Map<String, String> params,
                List<Map<String, Object>> bodies,
                Map<String, String> headers,
                NodeSelector nodeSelector
            ) throws IOException {
                if (QUERY_APIS.contains(apiName)) {
                    waitForAllNodesToApplyClusterState();
                }
                return super.callApi(apiName, method, params, bodies, headers, nodeSelector);
            }
        };
    }

    /**
     * {@link #waitForClusterUpdates()} runs a {@code LANGUID} health check, which the master executes only after every earlier
     * publication has completed, and a publication completes only once every node has applied it (or the publish timeout has elapsed).
     * It uses its own client rather than {@link ClientYamlTestExecutionContext#callApi}, so the stashed response of the test's previous
     * call is left untouched.
     */
    private void waitForAllNodesToApplyClusterState() throws IOException {
        try {
            waitForClusterUpdates();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
