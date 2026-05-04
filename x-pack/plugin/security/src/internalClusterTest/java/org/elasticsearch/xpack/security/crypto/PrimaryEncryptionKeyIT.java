/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

// 3 master-eligible nodes so testKeySurvivesMasterFailover keeps a quorum after stopping one
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class PrimaryEncryptionKeyIT extends SecurityIntegTestCase {

    @Before
    public void checkFeatureFlag() {
        assumeTrue(
            "primary encryption key feature flag must be enabled",
            PrimaryEncryptionKeyService.PRIMARY_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled()
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    private PrimaryEncryptionKeyMetadata getKeyFromClusterState(String nodeName) {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
        return clusterService.state().metadata().getSingleProjectCustom(PrimaryEncryptionKeyMetadata.TYPE);
    }

    public void testKeyIsGeneratedOnFreshCluster() throws Exception {
        ensureGreen();

        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
            assertThat("Primary encryption key should be generated", pek, notNullValue());
            assertNotNull(pek.getActiveKeyId());
            assertThat(pek.getKeyBytes(pek.getActiveKeyId()).length, equalTo(32));
            assertThat(pek.toSecretKey().getAlgorithm(), equalTo("AES"));
        });
    }

    public void testAllNodesHaveSameKey() throws Exception {
        ensureGreen();

        assertBusy(() -> {
            String firstKeyId = null;
            for (String nodeName : internalCluster().getNodeNames()) {
                PrimaryEncryptionKeyMetadata pek = getKeyFromClusterState(nodeName);
                assertThat("Key should be available on node " + nodeName, pek, notNullValue());
                if (firstKeyId == null) {
                    firstKeyId = pek.getActiveKeyId();
                } else {
                    assertEquals("All nodes should have the same active key", firstKeyId, pek.getActiveKeyId());
                }
            }
        });
    }

    public void testKeyNotExposedInApiContext() throws Exception {
        ensureGreen();

        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
            assertThat(pek, notNullValue());
        });

        PrimaryEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
        assertTrue(pek.context().contains(Metadata.XContentContext.GATEWAY));
        assertFalse(pek.context().contains(Metadata.XContentContext.API));
        assertFalse(pek.context().contains(Metadata.XContentContext.SNAPSHOT));
    }

    public void testKeySurvivesMasterFailover() throws Exception {
        ensureGreen();

        assertBusy(() -> assertThat(getKeyFromClusterState(internalCluster().getMasterName()), notNullValue()));

        PrimaryEncryptionKeyMetadata original = getKeyFromClusterState(internalCluster().getMasterName());

        internalCluster().stopCurrentMasterNode();
        ensureGreen();

        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
            assertThat(pek, notNullValue());
            assertEquals("Key ID should be the same after master failover", original.getActiveKeyId(), pek.getActiveKeyId());
        });
    }

    public void testKeySurvivesFullClusterRestart() throws Exception {
        ensureGreen();

        assertBusy(() -> assertThat(getKeyFromClusterState(internalCluster().getMasterName()), notNullValue()));

        PrimaryEncryptionKeyMetadata original = getKeyFromClusterState(internalCluster().getMasterName());

        internalCluster().fullRestart();
        ensureGreen();

        assertBusy(() -> {
            PrimaryEncryptionKeyMetadata pek = getKeyFromClusterState(internalCluster().getMasterName());
            assertThat("Key should survive full cluster restart", pek, notNullValue());
            assertEquals("Key ID should be the same after restart", original.getActiveKeyId(), pek.getActiveKeyId());
        });
    }

    public void testKeyNotExposedViaClusterStateApi() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(getKeyFromClusterState(internalCluster().getMasterName()), notNullValue()));

        ObjectPath response = ObjectPath.createFromResponse(performClusterStateRequest());
        assertNull(
            "primary_encryption_key must not be exposed via the cluster state API",
            response.evaluate("metadata.primary_encryption_key")
        );
    }

    private Response performClusterStateRequest() throws IOException {
        Request request = new Request("GET", "/_cluster/state/metadata");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        request.setOptions(options);
        return getRestClient().performRequest(request);
    }
}
