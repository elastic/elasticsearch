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
 * <p><b>Important caveat for this test class as a whole:</b> the seccomp sandbox itself, the
 * {@code --disableSandbox} CLI flag, and the {@code $TMPDIR/ml-child-ipc/<deploymentId>/} directory
 * are all created/interpreted on the native (ml-cpp) side. The ml-cpp artifact bundled in this
 * checkout at the time these tests were written does not yet contain the paired controller-protocol
 * change (a separate, not-yet-integrated ml-cpp PR), and
 * {@code NativePyTorchProcessFactory#createProcess} unconditionally requests the isolated child IPC
 * directory for every PyTorch deployment on Linux (see {@code useIsolatedChildIpcDir=true} there),
 * independent of the {@code sandbox_enabled} setting value. That means on Linux CI, with today's
 * bundled native controller, none of the {@code startDeployment(...)} calls below can be expected to
 * actually succeed end-to-end - the controller does not know to create the {@code ml-child-ipc}
 * directory, so the child process will fail to connect. This is expected and out of scope for this
 * task; it is not something these tests can currently work around. The methods below are written so
 * that, once the paired ml-cpp artifact is available, they exercise real, meaningful behaviour rather
 * than trivially passing.
 */
public class PyTorchSandboxIT extends PyTorchModelRestTestCase {

    /**
     * With {@code xpack.ml.trained_models.sandbox_enabled} left at its (new, dark-launched) default of
     * {@code false}, {@code PyTorchBuilder} adds {@code --disableSandbox} to the child process command
     * line on Linux (see {@code PyTorchBuilder#buildCommand}). This asserts that a deployment started
     * under that default configuration reaches a healthy state and serves inference correctly.
     */
    public void testDefaultSandboxDisabledEmitsDisableSandboxFlag() throws IOException {
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

            // Diagnostic-surface limitation: there is currently no REST/notification-visible signal that
            // distinguishes "started with the seccomp sandbox disabled via --disableSandbox" from
            // "started with the sandbox enabled". The only related observability added so far
            // (commit "Improve ML Sandbox2 failure observability in Java layer") raises *connect
            // failures* to WARN and includes pipe paths/timeout in the exception message - it does not
            // emit anything for the successful, degraded/legacy-seccomp-mode path exercised here. So this
            // test can only assert the functional outcome (deployment starts, inference succeeds); it
            // cannot assert that the child actually ran with seccomp disabled versus enforced. That
            // assertion needs either a paired ml-cpp diagnostic (e.g. a stats field surfacing the sandbox
            // mode) or the real V5 enforced-sandbox behavioural test (see the @Ignore stub below).
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
     * {@code --disableSandbox} entirely - i.e. the command line is identical to what it was before the
     * Sandbox2 kill-switch existed at all. Because omitting a flag the current bundled ml-cpp
     * controller never knew about is a no-op from the controller's point of view, a deployment started
     * with the setting {@code true} is expected to behave exactly like a deployment started before this
     * feature existed: it should start (modulo the isolated-child-IPC-directory caveat documented on the
     * class, which is independent of this setting and blocks *every* PyTorch deployment on Linux with
     * today's bundled controller, sandboxed or not).
     *
     * <p>What this test does NOT and cannot verify with today's bundled native controller: that leaving
     * the sandbox enabled results in "no automatic fallback" to a disabled/degraded sandbox under any
     * condition (e.g. seccomp being unsupported in the runtime environment). That guarantee is enforced
     * (or not) entirely in ml-cpp and requires the paired artifact with the new controller-protocol
     * version to exercise for real.
     */
    public void testExplicitSandboxEnabledOmitsDisableSandboxFlag() throws IOException {
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

            // Matches current bundled ml-cpp behaviour: omitting --disableSandbox is indistinguishable,
            // from the controller's perspective, from never having added the flag at all, so this call is
            // expected to succeed exactly as it did before Sandbox2 existed (again, modulo the isolated
            // child-IPC-directory caveat documented at the class level, which applies regardless of this
            // setting).
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
     * {@code NativePyTorchProcessFactory} passes {@code task.getDeploymentId()} as the {@code jobId} that
     * seeds {@code NamedPipeHelper#getChildIpcDirectoryPrefix}, i.e. each deployment's IPC pipes should
     * live under a directory keyed by its own deployment id (
     * {@code $TMPDIR/ml-child-ipc/<deploymentId>/}), so two concurrently-running deployments must not
     * collide.
     *
     * <p><b>Harness limitation:</b> {@code PyTorchModelRestTestCase} extends {@code ESRestTestCase} and
     * only has a REST client to the test cluster - there is no hook in this IT harness to list a node's
     * {@code $TMPDIR} contents or otherwise introspect the filesystem, so the isolated-directory paths
     * themselves cannot be asserted directly from here. The closest available check is functional: start
     * two deployments with distinct deployment ids concurrently and confirm both come up healthy and
     * each serves inference correctly and independently - if their IPC directories/pipes collided, we
     * would expect one or both deployments to fail to start, misroute pipe traffic, or return the wrong
     * model's result.
     */
    public void testChildIpcPathsIsolatedPerDeployment() throws Exception {
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
     * (e.g. the {@code @Ignore} annotation being dropped), fails loudly instead of asserting nothing.
     */
    @org.junit.Ignore("requires ml-cpp build with PR E controller-protocol-version>=1 bundled")
    public void testEnforcedSandboxRejectsDisallowedFileAccess() {
        throw new UnsupportedOperationException(
            "testEnforcedSandboxRejectsDisallowedFileAccess requires a paired ml-cpp artifact with the "
                + "Sandbox2 controller-protocol-version bundled; it is not implemented against the "
                + "currently bundled native controller. Do not remove @Ignore without also implementing "
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
