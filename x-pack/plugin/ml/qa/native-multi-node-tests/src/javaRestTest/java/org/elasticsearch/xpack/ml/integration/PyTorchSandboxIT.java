/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for the ML "Sandbox2" work: the {@code xpack.ml.trained_models.sandbox_enabled}
 * kill-switch setting (default {@code false}, dark-launched for 9.6) and the isolated per-child IPC
 * directory used to pass {@code --input=}/{@code --output=}/{@code --restore=}/{@code --logPipe=}
 * paths to the PyTorch native process.
 *
 * <p><b>Important caveat for this test class as a whole:</b> EVERY code path exercised here is
 * blocked on the paired, not-yet-integrated ml-cpp artifact (see
 * https://github.com/elastic/ml-cpp/pull/3188), for two distinct reasons depending on the setting:
 *
 * <ul>
 *   <li>{@code sandbox_enabled=false} (the default): {@code PyTorchBuilder#buildCommand} emits
 *   {@code --disableSandbox} unconditionally on Linux (see {@code DISABLE_SANDBOX_ARG}, added
 *   whenever {@code sandboxEnabled == false && isLinux}). The native controller bundled in this
 *   checkout today has no parsing for that token at all - it forwards it verbatim to
 *   {@code pytorch_inference}, which aborts on the unrecognized CLI option. This is the DEFAULT
 *   path, and it is just as blocked as the explicit-enable path below, only for a different reason.</li>
 *   <li>{@code sandbox_enabled=true}: {@code NativePyTorchProcessFactory#createProcess} additionally
 *   requests the isolated {@code $TMPDIR/ml-child-ipc/<deploymentId>/} directory (see
 *   {@code useIsolatedChildIpcDir=sandboxEnabled} there), which today's bundled controller does not
 *   create.</li>
 * </ul>
 *
 * <p>Because {@code verifyControllerProtocolVersion} (see {@code x-pack/plugin/ml/build.gradle})
 * now hard-fails the build until a compatible ml-cpp artifact is bundled, none of the test methods
 * in this class can actually run against this checkout today - the build itself will not assemble.
 * The per-method javadoc below documents each method's specific blocking reason for when the paired
 * artifact lands, not a claim of partial compatibility today.
 *
 * <p>Surfacing/countering the structured enforced-mode signal - i.e. giving operators visibility into
 * (and a way to react to) ml-cpp's own signal that it is running in an enforced sandbox mode - is
 * tracked as a separate follow-up and is not implemented in this test class.
 */
public class PyTorchSandboxIT extends PyTorchModelRestTestCase {

    /**
     * With {@code xpack.ml.trained_models.sandbox_enabled} left at its (new, dark-launched) default of
     * {@code false}, {@code PyTorchBuilder} adds {@code --disableSandbox} to the child process command
     * line on Linux (see {@code PyTorchBuilder#buildCommand}). The native controller bundled in this
     * checkout does not recognize that token and aborts the child process on startup - so, contrary to
     * an earlier version of this comment, this default path is NOT unaffected by the missing ml-cpp
     * change; it is blocked on the paired ml-cpp artifact just like the explicit-enable path is, only
     * because of an unrecognized CLI flag rather than the missing isolated-IPC-directory support. This
     * asserts that, once that artifact is bundled, a deployment started under the default configuration
     * reaches a healthy state and serves inference correctly.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/ml-cpp/pull/3188")
    public void testDefaultSandboxDisabledStartsAndInfersSuccessfully() throws IOException {
        String modelId = "sandbox_default_disabled";
        createPassThroughModel(modelId);
        putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);

        startDeployment(modelId, AllocationStatus.State.STARTED);
        try {
            Response inference = infer("my words", modelId);
            assertThat(
                EntityUtils.toString(inference.getEntity()),
                equalTo("{\"inference_results\":[{\"predicted_value\":[[1.0,1.0]]}]}")
            );

            // Limitation: no REST/notification-visible signal distinguishes "started with seccomp
            // disabled" from "started with seccomp enforced", so this only asserts the functional outcome.
            Response statsResponse = getTrainedModelStats(modelId);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> stats = (List<Map<String, Object>>) entityAsMap(statsResponse).get("trained_model_stats");
            assertThat(stats, notNullValue());
            String statusState = (String) XContentMapValues.extractValue("deployment_stats.allocation_status.state", stats.get(0));
            assertThat(statusState, is(not(nullValue())));
        } finally {
            stopDeployment(modelId);
        }
    }

    /**
     * Flips {@code xpack.ml.trained_models.sandbox_enabled} to {@code true} via a dynamic cluster
     * settings update. With the setting {@code true}, {@code PyTorchBuilder} omits
     * {@code --disableSandbox} entirely, but flipping the setting to {@code true} also makes
     * {@code NativePyTorchProcessFactory} request the isolated child IPC directory (see the class
     * javadoc), which today's bundled ml-cpp controller does not create - so this test is expected to
     * fail end-to-end until the paired ml-cpp artifact lands.
     *
     * <p>Limitation: "no automatic fallback" to a disabled/degraded sandbox is enforced entirely in
     * ml-cpp and cannot be verified here without the paired controller-protocol artifact.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/ml-cpp/pull/3188")
    public void testExplicitSandboxEnabledStartsAndInfersSuccessfully() throws IOException {
        Request clusterSettings = new Request("PUT", "_cluster/settings");
        clusterSettings.setJsonEntity("""
            {"persistent" : {
                    "xpack.ml.trained_models.sandbox_enabled": true
                }}""");
        client().performRequest(clusterSettings);

        String modelId = "sandbox_explicit_enabled";
        try {
            createPassThroughModel(modelId);
            putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
            putVocabulary(List.of("these", "are", "my", "words"), modelId);

            startDeployment(modelId, AllocationStatus.State.STARTED);
            Response inference = infer("my words", modelId);
            assertThat(
                EntityUtils.toString(inference.getEntity()),
                equalTo("{\"inference_results\":[{\"predicted_value\":[[1.0,1.0]]}]}")
            );
            stopDeployment(modelId);
        } finally {
            Request reset = new Request("PUT", "_cluster/settings");
            reset.setJsonEntity("""
                {"persistent" : {
                        "xpack.ml.trained_models.sandbox_enabled": null
                    }}""");
            client().performRequest(reset);
        }
    }

    /**
     * Runs at the (default) {@code sandbox_enabled=false} setting, where the isolated child IPC
     * directory is NOT used (see the class javadoc) - the legacy, flat pipe naming applies instead.
     * Per the class javadoc, this path is blocked on the paired ml-cpp artifact too - the bundled
     * controller aborts on the unrecognized {@code --disableSandbox} token before either deployment
     * can even start, so today this test cannot pass any more than the explicit-enable tests can. Once
     * the paired artifact lands, this asserts that two concurrently-running deployments, each
     * identified by their own deployment id, do not collide under that legacy flat pipe naming.
     *
     * <p>Limitation: the REST-only IT harness cannot list a node's {@code $TMPDIR} to assert pipe
     * paths directly, so this checks the functional proxy instead - two concurrent deployments both
     * starting healthy and serving correct, uncorrupted inference independently.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/ml-cpp/pull/3188")
    public void testConcurrentDeploymentsDoNotCollideUnderLegacyPipeNaming() throws Exception {
        String modelIdA = "sandbox_ipc_isolation_a";
        String modelIdB = "sandbox_ipc_isolation_b";
        String deploymentIdA = "sandbox_ipc_isolation_dep_a";
        String deploymentIdB = "sandbox_ipc_isolation_dep_b";

        createPassThroughModel(modelIdA);
        putModelDefinition(modelIdA, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("these", "are", "my", "words"), modelIdA);

        createPassThroughModel(modelIdB);
        putModelDefinition(modelIdB, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("these", "are", "my", "words"), modelIdB);

        // Start both deployments concurrently (rather than sequentially) so that, if the paired ml-cpp
        // artifact is present, this actually exercises the interesting race: two native controllers
        // creating their respective ml-child-ipc/<deploymentId>/ directories at close to the same time.
        Future<Response> startA = executorService.submit(() -> startWithDeploymentId(modelIdA, deploymentIdA));
        Future<Response> startB = executorService.submit(() -> startWithDeploymentId(modelIdB, deploymentIdB));

        try {
            startA.get(60, TimeUnit.SECONDS);
            startB.get(60, TimeUnit.SECONDS);

            // Each deployment must report its own deployment id in its stats - i.e. the two processes
            // were not conflated with one another.
            assertDeploymentIdInStats(modelIdA, deploymentIdA);
            assertDeploymentIdInStats(modelIdB, deploymentIdB);

            // Independent inference against each deployment must return the correct, uncorrupted result.
            // Cross-talk between two colliding IPC channels would be expected to corrupt or misroute at
            // least one of these.
            Response inferenceA = infer("my words", deploymentIdA);
            assertThat(
                EntityUtils.toString(inferenceA.getEntity()),
                equalTo("{\"inference_results\":[{\"predicted_value\":[[1.0,1.0]]}]}")
            );
            Response inferenceB = infer("my words", deploymentIdB);
            assertThat(
                EntityUtils.toString(inferenceB.getEntity()),
                equalTo("{\"inference_results\":[{\"predicted_value\":[[1.0,1.0]]}]}")
            );
        } finally {
            stopQuietly(deploymentIdA);
            stopQuietly(deploymentIdB);
        }
    }

    /**
     * The full V5 case - booting a real, enforced Sandbox2 child and asserting that {@code open()}/
     * {@code write()} calls outside the permitted set actually fail inside the sandboxed process -
     * requires the paired ml-cpp artifact with the new controller-protocol version bundled. That
     * artifact is not available in this checkout. Rather than a silently-passing no-op, this stub is
     * skipped with an explicit reason and, should it ever run without the paired artifact by mistake
     * (e.g. the {@code @AwaitsFix} annotation being dropped), fails loudly instead of asserting nothing.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/ml-cpp/pull/3188")
    public void testEnforcedSandboxRejectsDisallowedFileAccess() {
        throw new UnsupportedOperationException(
            "testEnforcedSandboxRejectsDisallowedFileAccess requires a paired ml-cpp artifact with the "
                + "Sandbox2 controller-protocol-version bundled; it is not implemented against the "
                + "currently bundled native controller. Do not remove @AwaitsFix without also implementing "
                + "real assertions here."
        );
    }

    @SuppressWarnings("unchecked")
    private void assertDeploymentIdInStats(String modelId, String expectedDeploymentId) throws IOException {
        Response statsResponse = getTrainedModelStats(modelId);
        Map<String, Object> responseMap = entityAsMap(statsResponse);
        List<Map<String, Object>> stats = (List<Map<String, Object>>) responseMap.get("trained_model_stats");
        assertThat(responseMap.toString(), stats, notNullValue());
        Object deploymentId = XContentMapValues.extractValue("deployment_stats.deployment_id", stats.get(0));
        assertThat(responseMap.toString(), deploymentId, equalTo(expectedDeploymentId));
    }

    private void stopQuietly(String deploymentId) {
        try {
            stopDeployment(deploymentId, true, false);
        } catch (IOException e) {
            // best-effort cleanup; the deployment may never have started successfully
            logger.warn("failed to stop deployment [" + deploymentId + "] during test cleanup", e);
        }
    }
}
