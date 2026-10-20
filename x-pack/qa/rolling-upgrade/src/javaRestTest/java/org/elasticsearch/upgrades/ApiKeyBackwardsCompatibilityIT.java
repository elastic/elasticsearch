/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class ApiKeyBackwardsCompatibilityIT extends AbstractXpackRollingUpgradeWithSecurityTestCase {

    private static final String CERTIFICATE_IDENTITY_FIELD_FEATURE = "certificate_identity_field";

    public ApiKeyBackwardsCompatibilityIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testCertificateIdentityBackwardsCompatibility() throws Exception {
        final List<NodeInfo> nodes = NodeInfo.getAll(adminClient());

        final Set<NodeInfo> newVersionNodes = nodes.stream().filter(NodeInfo::isUpgradedVersionCluster).collect(toSet());
        final Set<NodeInfo> oldVersionNodes = nodes.stream().filter(NodeInfo::isOriginalVersionCluster).collect(toSet());

        assumeTrue(
            "Old version nodes must not support certificate identity feature",
            oldVersionNodes.stream().noneMatch(info -> info.supportsFeature(CERTIFICATE_IDENTITY_FIELD_FEATURE))
        );
        assumeTrue(
            "New version nodes must support certificate identity feature",
            newVersionNodes.stream().allMatch(info -> info.supportsFeature(CERTIFICATE_IDENTITY_FIELD_FEATURE))
        );

        if (isOldCluster()) {
            var exception = expectThrows(Exception.class, () -> createCrossClusterApiKeyWithCertIdentity("CN=test-.*"));
            assertThat(
                exception.getMessage(),
                anyOf(containsString("unknown field [certificate_identity]"), containsString("certificate_identity not supported"))
            );
        } else if (isMixedCluster()) {
            try {
                this.createClientsByCapability(this::nodeSupportsCertificateIdentity);

                // Test against old node - should get parsing error
                Exception oldNodeException = expectThrows(
                    Exception.class,
                    () -> createCrossClusterApiKeyWithCertIdentity(oldVersionClient, "CN=test-.*")
                );
                assertThat(
                    oldNodeException.getMessage(),
                    anyOf(containsString("unknown field [certificate_identity]"), containsString("certificate_identity not supported"))
                );

                // Test against new node - should get mixed-version error
                Exception newNodeException = expectThrows(
                    Exception.class,
                    () -> createCrossClusterApiKeyWithCertIdentity(newVersionClient, "CN=test-.*")
                );
                assertThat(
                    newNodeException.getMessage(),
                    containsString("cluster is in a mixed-version state and does not yet support the [certificate_identity] field")
                );
            } finally {
                this.closeClientsByVersion();
            }
        } else if (isUpgradedCluster()) {
            // Fully upgraded cluster should support certificate identity
            final Tuple<String, String> apiKey = createCrossClusterApiKeyWithCertIdentity("CN=test-.*");

            // Verify the API key was created with certificate identity
            final Request getApiKeyRequest = new Request("GET", "/_security/api_key");
            getApiKeyRequest.addParameter("id", apiKey.v1());
            final Response getResponse = client().performRequest(getApiKeyRequest);
            assertOK(getResponse);

            final ObjectPath getPath = ObjectPath.createFromResponse(getResponse);
            assertThat(getPath.evaluate("api_keys.0.certificate_identity"), equalTo("CN=test-.*"));
        }
    }

    private boolean nodeSupportsCertificateIdentity(NodeInfo nodeDetails) {
        return nodeDetails.supportsFeature(CERTIFICATE_IDENTITY_FIELD_FEATURE);
    }

    private Tuple<String, String> createCrossClusterApiKeyWithCertIdentity(String certificateIdentity) throws IOException {
        return createCrossClusterApiKeyWithCertIdentity(client(), certificateIdentity);
    }

    private Tuple<String, String> createCrossClusterApiKeyWithCertIdentity(RestClient client, String certificateIdentity)
        throws IOException {
        final String name = "test-cc-api-key-" + randomAlphaOfLengthBetween(3, 5);
        final Request createApiKeyRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createApiKeyRequest.setJsonEntity(Strings.format("""
            {
                "name": "%s",
                "certificate_identity": "%s",
                "access": {
                    "search": [
                        {
                            "names": ["test-*"]
                        }
                    ]
                }
            }""", name, certificateIdentity));

        final Response createResponse = client.performRequest(createApiKeyRequest);
        assertOK(createResponse);
        final ObjectPath path = ObjectPath.createFromResponse(createResponse);
        final String id = path.evaluate("id");
        final String key = path.evaluate("api_key");
        assertThat(id, notNullValue());
        assertThat(key, notNullValue());
        return Tuple.tuple(id, key);
    }
}
