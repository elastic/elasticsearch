/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_TEMPLATE_NAME;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * A base {@link ESClientYamlSuiteTestCase} test class for the security module,
 * which depends on security template and mappings being up to date before any writes
 * to the {@code .security} index can take place.
 */
public abstract class SecurityClusterClientYamlTestCase extends ESClientYamlSuiteTestCase {

    public SecurityClusterClientYamlTestCase(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Before
    public void waitForSecuritySetup() throws Exception {
        waitForSecurity();
    }

    public static void waitForSecurity() throws Exception {
        String masterNode = null;
        HttpEntity entity = client().performRequest("GET", "/_cat/nodes?h=id,master").getEntity();
        String catNodesResponse = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        for (String line : catNodesResponse.split("\n")) {
            int indexOfStar = line.indexOf('*'); // * in the node's output denotes it is master
            if (indexOfStar != -1) {
                masterNode = line.substring(0, indexOfStar).trim();
                break;
            }
        }
        assertNotNull(masterNode);
        final String masterNodeId = masterNode;

        assertBusy(() -> {
            try {
                Response nodesResponse = client().performRequest("GET", "/_nodes");
                ObjectPath nodesPath = ObjectPath.createFromResponse(nodesResponse);
                Map<String, Object> nodes = nodesPath.evaluate("nodes");
                Version masterVersion = null;
                for (String nodeId : nodes.keySet()) {
                    // get the ES version number master is on
                    if (nodeId.startsWith(masterNodeId)) {
                        masterVersion = Version.fromString(nodesPath.evaluate("nodes." + nodeId + ".version"));
                        break;
                    }
                }
                assertNotNull(masterVersion);

                Response response = client().performRequest("GET", "/_cluster/state/metadata");
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                String mappingsPath = "metadata.templates." + SECURITY_TEMPLATE_NAME + ".mappings";
                Map<String, Object> mappings = objectPath.evaluate(mappingsPath);
                assertNotNull(mappings);
                assertThat(mappings.size(), greaterThanOrEqualTo(1));
                for (String key : mappings.keySet()) {
                    String templatePath = mappingsPath + "." + key + "._meta.security-version";
                    Version templateVersion = Version.fromString(objectPath.evaluate(templatePath));
                    assertEquals(masterVersion, templateVersion);
                }
            } catch (Exception e) {
                throw new AssertionError("failed to get cluster state", e);
            }
        });
    }
}
