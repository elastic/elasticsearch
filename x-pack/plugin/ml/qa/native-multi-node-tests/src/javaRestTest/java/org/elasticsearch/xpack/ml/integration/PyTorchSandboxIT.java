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
 * https://github.com/elastic/ml-cpp/pull/3188):
 *
 * <ul>
 *   <li>{@code sandbox_enabled=false} (the default): {@code PyTorchBuilder#buildCommand} emits
 *   {@code --disableSandbox} on Linux (see {@code DISABLE_SANDBOX_ARG}).</li>
 *   <li>{@code sandbox_enabled=true}: {@code PyTorchBuilder#buildCommand} emits {@code --requireSandbox}
 *   on Linux instead (see {@code REQUIRE_SANDBOX_ARG}) - Elasticsearch always sends exactly one of the
 *   two tokens on Linux, never neither, per the two-token contract ml-cpp#3188 defines. It also makes
 *   {@code NativePyTorchProcessFactory#createProcess} request the isolated
 *   {@code $TMPDIR/ml-child-ipc/<deploymentId>/} directory (see {@code useIsolatedChildIpcDir=sandboxEnabled}
 *   there).</li>
 * </ul>
 *
 * <p>The native controller bundled in this checkout, until the paired artifact lands, has no parsing
 * for either token at all - it forwards whichever one Elasticsearch sends verbatim to
 * {@code pytorch_inference}, which aborts on the unrecognized CLI option. Both settings values are
 * therefore equally blocked, for the same reason.
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
 *
 * <p>{@link #testConcurrentDeploymentsDoNotCollideUnderIsolatedChildIpcDir} additionally covers the
 * {@code sandbox_enabled=true} / isolated-child-IPC-directory concurrency case, mirroring
 * {@link #testConcurrentDeploymentsDoNotCollideUnderLegacyPipeNaming}'s {@code sandbox_enabled=false}
 * coverage. {@link #testEnforcedSandboxRejectsDisallowedFileAccess} is intentionally left without a
 * black-box REST assertion - see its own javadoc for why - and instead points at the real proof of
 * that property in ml-cpp's own unit test suite.
 */
public class PyTorchSandboxIT extends PyTorchModelRestTestCase {

    /**
     * With {@code xpack.ml.trained_models.sandbox_enabled} left at its (new, dark-launched) default of
     * {@code false}, {@code PyTorchBuilder} adds {@code --disableSandbox} to the child process command
     * line on Linux (see {@code PyTorchBuilder#buildCommand}). The native controller bundled in this
     * checkout does not recognize that token and aborts the child process on startup - this default
     * path is blocked on the paired ml-cpp artifact just like the explicit-enable path is. This
     * asserts that, once that artifact is bundled, a deployment started under the default configuration
     * reaches a healthy state and serves inference correctly.
     */
    public void testDefaultSandboxDisabledStartsAndInfersSuccessfully() throws IOException {
        String modelId = "sandbox_default_disabled";
        createPassThroughModel(modelId);
        putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);

        startDeployment(modelId, AllocationStatus.State.STARTED);
        try {
            Response inference = infer("my words", modelId);
            assertThat(EntityUtils.toString(inference.getEntity()), equalTo("{\"inference_results\":[{\"predicted_value\":[[1.0,1.0]]}]}"));

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
     * settings update. With the setting {@code true}, {@code PyTorchBuilder} sends
     * {@code --requireSandbox} instead of {@code --disableSandbox}, and
     * {@code NativePyTorchProcessFactory} requests the isolated child IPC directory (see the class
     * javadoc), which today's bundled ml-cpp controller does not create - so this test is expected to
     * fail end-to-end until the paired ml-cpp artifact lands.
     *
     * <p>Limitation: "no automatic fallback" to a disabled/degraded sandbox is enforced entirely in
     * ml-cpp and cannot be verified here without the paired controller-protocol artifact.
     */
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
            assertThat(EntityUtils.toString(inference.getEntity()), equalTo("{\"inference_results\":[{\"predicted_value\":[[1.0,1.0]]}]}"));
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
    public void testConcurrentDeploymentsDoNotCollideUnderLegacyPipeNaming() throws Exception {
        String modelIdA = "sandbox_legacy_pipe_naming_a";
        String modelIdB = "sandbox_legacy_pipe_naming_b";
        String deploymentIdA = "sandbox_legacy_pipe_naming_dep_a";
        String deploymentIdB = "sandbox_legacy_pipe_naming_dep_b";

        createPassThroughModel(modelIdA);
        putModelDefinition(modelIdA, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("these", "are", "my", "words"), modelIdA);

        createPassThroughModel(modelIdB);
        putModelDefinition(modelIdB, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
        putVocabulary(List.of("these", "are", "my", "words"), modelIdB);

        // Start both deployments concurrently (rather than sequentially) so that, if the paired ml-cpp
        // artifact is present, this actually exercises the interesting race: two native controllers
        // creating their respective flat-named pipe sets (no per-deployment ml-child-ipc directory at
        // this sandbox_enabled=false default - see the class javadoc) in $TMPDIR at close to the same time.
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
     * Mirrors {@link #testConcurrentDeploymentsDoNotCollideUnderLegacyPipeNaming}, but with
     * {@code xpack.ml.trained_models.sandbox_enabled} set to {@code true}, so that
     * {@code NativePyTorchProcessFactory#createProcessPipes} requests the isolated per-child IPC
     * directory ({@code $TMPDIR/ml-child-ipc/<deploymentId>/}) instead of the legacy flat pipe naming
     * (see the class javadoc). That directory is keyed on the deployment id, so two concurrently
     * started deployments get two disjoint directories by construction - this proves that isolation
     * actually holds when two children race to create/populate their respective directories at close
     * to the same time, not just that the naming scheme looks disjoint on paper.
     *
     * <p>Limitation: as with the legacy-pipe-naming test, the REST-only IT harness cannot list a
     * node's {@code $TMPDIR} to assert directory paths directly, so this checks the same functional
     * proxy - two concurrent deployments both starting healthy and serving correct, uncorrupted
     * inference independently, each reporting its own deployment id.
     */
    public void testConcurrentDeploymentsDoNotCollideUnderIsolatedChildIpcDir() throws Exception {
        Request clusterSettings = new Request("PUT", "_cluster/settings");
        clusterSettings.setJsonEntity("""
            {"persistent" : {
                    "xpack.ml.trained_models.sandbox_enabled": true
                }}""");
        client().performRequest(clusterSettings);

        String modelIdA = "sandbox_isolated_ipc_dir_a";
        String modelIdB = "sandbox_isolated_ipc_dir_b";
        String deploymentIdA = "sandbox_isolated_ipc_dir_dep_a";
        String deploymentIdB = "sandbox_isolated_ipc_dir_dep_b";

        try {
            createPassThroughModel(modelIdA);
            putModelDefinition(modelIdA, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
            putVocabulary(List.of("these", "are", "my", "words"), modelIdA);

            createPassThroughModel(modelIdB);
            putModelDefinition(modelIdB, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
            putVocabulary(List.of("these", "are", "my", "words"), modelIdB);

            // Start both deployments concurrently so that, with sandboxing enabled, this exercises two
            // native controllers each creating and populating their own isolated ml-child-ipc/<jobId>/
            // directory under $TMPDIR at close to the same time.
            Future<Response> startA = executorService.submit(() -> startWithDeploymentId(modelIdA, deploymentIdA));
            Future<Response> startB = executorService.submit(() -> startWithDeploymentId(modelIdB, deploymentIdB));

            startA.get(60, TimeUnit.SECONDS);
            startB.get(60, TimeUnit.SECONDS);

            // Each deployment must report its own deployment id in its stats - i.e. the two processes
            // were not conflated with one another despite sharing the same $TMPDIR/ml-child-ipc parent.
            assertDeploymentIdInStats(modelIdA, deploymentIdA);
            assertDeploymentIdInStats(modelIdB, deploymentIdB);

            // Independent inference against each deployment must return the correct, uncorrupted result.
            // Cross-talk between two colliding isolated IPC directories would be expected to corrupt or
            // misroute at least one of these.
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
            Request reset = new Request("PUT", "_cluster/settings");
            reset.setJsonEntity("""
                {"persistent" : {
                        "xpack.ml.trained_models.sandbox_enabled": null
                    }}""");
            client().performRequest(reset);
        }
    }

    /**
     * The full proof that a real, enforced Sandbox2 child actually has {@code open()}/{@code write()}
     * calls outside its permitted set denied by the kernel is exercised, mechanism by mechanism,
     * against a dedicated probe binary in ml-cpp's own unit test suite - see
     * {@code lib/sandbox/unittest/CPytorchInferenceSandboxPolicyMechanismTest_Linux.cc}, in particular
     * {@code testMinimizedPolicyEnforcesEveryMechanism}, which asserts
     * {@code mechanism=host_read_etc_shadow outcome=denied} and
     * {@code mechanism=external_egress outcome=denied} (among others) from inside a real Sandbox2
     * policy built by {@code buildPytorchInferenceFilesystemPolicy}.
     *
     * <p>That level of proof is not reproducible as a black-box assertion from this REST-only
     * integration test. {@code pytorch_inference}'s only file I/O surface reachable from Elasticsearch
     * is the small set of named pipes ({@code --input}/{@code --output}/{@code --restore}/
     * {@code --logPipe}) that Elasticsearch itself constructs from a deployment id that is already
     * restricted to {@code [a-z0-9_.-]} (see {@code MlStrings#isValidId}), so path-traversal via a
     * deployment id is not possible in the first place, and there is no REST-exposed lever that lets a
     * test redirect the sandboxed process's file I/O anywhere else. Provoking a genuine disallowed
     * {@code open()}/{@code write()} from Java would require either filesystem access to the node's
     * {@code $TMPDIR} from the test JVM (to stage e.g. a symlink at the isolated child IPC directory
     * before the deployment starts) - which the {@code ElasticsearchCluster} test-cluster abstraction
     * used by this module does not expose - or a deliberate test-only hook inside the production
     * {@code pytorch_inference} binary, which would needlessly widen a security-critical binary's
     * surface purely for testability.
     *
     * <p>This is therefore intentionally left as a documented gap rather than a fabricated assertion:
     * an end-to-end REST proof of syscall-level denial is not achievable through this test harness as
     * it exists today. See the class javadoc and the two tests above for what this class does cover
     * end-to-end (both sandbox routing tokens, and isolation between concurrently-running deployments
     * under both pipe-naming schemes). A future enhancement to the test-cluster framework that exposes
     * a node's working/tmp directory to REST ITs would unlock this; alternatively a narrowly-scoped,
     * build-flag-gated debug hook in {@code pytorch_inference} could accept a single "attempt a
     * disallowed open()" self-test instruction - either would need its own design discussion before
     * landing here.
     */
    public void testEnforcedSandboxRejectsDisallowedFileAccess() {
        // Intentionally not implemented as a black-box REST assertion - see the javadoc above for why,
        // and see ml-cpp's CPytorchInferenceSandboxPolicyMechanismTest_Linux for the real proof of this
        // property at the mechanism level.
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
