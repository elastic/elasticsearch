/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.encryption.spi.test;

import org.apache.http.Header;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * Verifies that {@code EncryptedDataHandlerProvider} implementations contributed by other plugins via
 * {@code ExtensiblePlugin.loadExtensions} are discovered by the encryption plugin and their handlers are invoked by the running
 * {@code KeyRotationCoordinator}.
 *
 * <p>The test plugin contributes a {@code TestEncryptedDataHandlerProvider} whose handler increments an in-JVM counter. The plugin
 * also installs a small REST handler that exposes that counter so this test (running out-of-process) can observe rotation activity.
 */
public class EncryptedDataHandlerProviderSpiIT extends ESRestTestCase {

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("test-encryption-spi-cluster")
        .plugin("test-encryption-spi-extension")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.encryption.key_rotation.interval", "1s")
        .setting("xpack.encryption.key_rotation.check_interval", "1s")
        .setting("path.repo", () -> repoDirectory.getRoot().getPath())
        .keystore("cluster.state.encryption.active_password_id", "v1")
        .keystore("cluster.state.encryption.password.v1", "encryption-test-password")
        .user("test-admin", "x-pack-test-password")
        .build();

    @ClassRule
    public static final TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(
                ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()))
            )
            .build();
    }

    /**
     * The KeyRotationCoordinator submits a begin-project-encryption-key-rotation cluster-state task every ~1 s while the cluster is
     * alive. ESRestTestCase#waitForClusterStateUpdatesToFinish uses assertBusy with exponential-backoff polling that consistently misses
     * the ~200 ms clean windows between successive tasks, causing spurious teardown failures. This test creates no persistent cluster
     * state, so skipping the wipe is safe.
     */
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testProviderIsDiscoveredAndHandlerIsInvoked() throws Exception {
        assertBusy(() -> {
            var response = client().performRequest(new Request("GET", "/_test/encryption_spi/invocations"));
            int count = assertOKAndCreateObjectPath(response).evaluate("invocations");
            assertThat(count, greaterThan(0));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Verifies that the snapshot Warning header is emitted when the cluster has encrypted data in project state.
     * The test handler's reEncrypt seeds a TestEncryptedBlob on the first rotation, so the Warning fires once the
     * coordinator has run at least once.
     */
    public void testSnapshotWarningEmittedWhenEncryptedDataPresent() throws Exception {
        // Wait for the key rotation coordinator to seed the TestEncryptedBlob into cluster state.
        assertBusy(() -> {
            var response = client().performRequest(new Request("GET", "/_test/encryption_spi/invocations"));
            assertThat(assertOKAndCreateObjectPath(response).<Integer>evaluate("invocations"), greaterThan(0));
        }, 30, TimeUnit.SECONDS);

        var putRepo = new Request("PUT", "/_snapshot/test-repo");
        putRepo.setJsonEntity("{\"type\":\"fs\",\"settings\":{\"location\":\"" + repoDirectory.getRoot().getPath() + "\"}}");
        assertOK(client().performRequest(putRepo));

        var snapshotRequest = new Request("PUT", "/_snapshot/test-repo/snap");
        snapshotRequest.addParameter("wait_for_completion", "true");
        snapshotRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> false).build());
        var response = client().performRequest(snapshotRequest);

        List<String> warningValues = Arrays.stream(response.getHeaders())
            .filter(h -> h.getName().equals("Warning"))
            .map(Header::getValue)
            .map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, true))
            .toList();
        assertThat(warningValues, hasItem(containsString("Encrypted data source credentials")));
    }

    public void testNoSnapshotWarningWhenIncludeGlobalStateFalse() throws Exception {
        assertBusy(() -> {
            var response = client().performRequest(new Request("GET", "/_test/encryption_spi/invocations"));
            assertThat(assertOKAndCreateObjectPath(response).<Integer>evaluate("invocations"), greaterThan(0));
        }, 30, TimeUnit.SECONDS);

        var putRepo = new Request("PUT", "/_snapshot/test-repo-no-global-state");
        putRepo.setJsonEntity("{\"type\":\"fs\",\"settings\":{\"location\":\"" + repoDirectory.getRoot().getPath() + "\"}}");
        assertOK(client().performRequest(putRepo));

        var snapshotRequest = new Request("PUT", "/_snapshot/test-repo-no-global-state/snap");
        snapshotRequest.addParameter("wait_for_completion", "true");
        snapshotRequest.addParameter("include_global_state", "false");
        var response = client().performRequest(snapshotRequest);

        List<String> warningValues = Arrays.stream(response.getHeaders())
            .filter(h -> h.getName().equals("Warning"))
            .map(Header::getValue)
            .map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, true))
            .toList();
        assertThat(warningValues, not(hasItem(containsString("Encrypted data source credentials"))));
    }
}
