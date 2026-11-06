/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker

import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.process.ExecOperations
import org.gradle.process.ExecResult
import org.gradle.process.ExecSpec
import org.gradle.process.JavaExecSpec

import spock.lang.Specification

/**
 * Exercises the pull/retry and local-availability logic of {@link DockerImagePullSupport} directly, without a real
 * Docker registry. The Gradle {@code ExecOperations}/{@code ExecSpec}/{@code ExecResult} types are framework
 * interfaces with large surfaces, so they are replaced by minimal hand-written fakes (below) that record the docker
 * command line and return programmable results. This lets the tests drive the exact scenarios the retry logic exists
 * for: reuse of an already-cached image, a platform mismatch forcing a pull, recovery after a transient pull failure,
 * and exhaustion of all attempts.
 */
class DockerImagePullSupportSpec extends Specification {

    private static final String DOCKER = "docker"
    private static final String IMAGE = "ghcr.io/example/builder:1.2.3"

    def "already-cached image for the requested platform is not pulled again"() {
        given:
        def ops = new FakeExecOperations()
        ops.onInspectOsArch = { "linux/amd64" }

        when:
        DockerImagePullSupport.ensureImageAvailable(ops, DOCKER, IMAGE, "linux/amd64")

        then:
        ops.pullInvocations == 0
        ops.commands.any { it.contains("inspect") }
    }

    def "a locally present image of the wrong platform triggers a pull"() {
        given:
        def ops = new FakeExecOperations()
        // The store holds the amd64 variant, but we need arm64, so a pull must happen.
        ops.onInspectOsArch = { "linux/amd64" }

        when:
        DockerImagePullSupport.ensureImageAvailable(ops, DOCKER, IMAGE, "linux/arm64")

        then:
        ops.pullInvocations == 1
        def pull = ops.commands.find { it.contains("pull") }
        pull.containsAll(["pull", "--platform", "linux/arm64", IMAGE])
    }

    def "a missing image is pulled once when the pull succeeds"() {
        given:
        def ops = new FakeExecOperations()
        ops.inspectExitValue = 1 // not present locally

        when:
        DockerImagePullSupport.ensureImageAvailable(ops, DOCKER, IMAGE, null)

        then:
        ops.pullInvocations == 1
    }

    def "pull recovers after a transient failure and stops retrying once it succeeds"() {
        given:
        def ops = new FakeExecOperations()
        ops.pullFailuresBeforeSuccess = 2

        when:
        // initialBackoffMs = 0 so the test does not actually sleep between attempts
        DockerImagePullSupport.pullImage(ops, DOCKER, IMAGE, "linux/amd64", 5, 0L)

        then:
        ops.pullInvocations == 3
    }

    def "pull fails with a GradleException once all attempts are exhausted"() {
        given:
        def ops = new FakeExecOperations()
        ops.pullFailuresBeforeSuccess = Integer.MAX_VALUE // always fail

        when:
        DockerImagePullSupport.pullImage(ops, DOCKER, IMAGE, "linux/amd64", 3, 0L)

        then:
        def e = thrown(GradleException)
        e.message.contains(IMAGE)
        ops.pullInvocations == 3
    }

    def "resolveLocalImageId returns the inspected image id"() {
        given:
        def ops = new FakeExecOperations()
        ops.onInspectId = { "sha256:deadbeef" }

        expect:
        DockerImagePullSupport.resolveLocalImageId(ops, DOCKER, IMAGE) == "sha256:deadbeef"
    }

    def "resolveLocalImageId returns null when the image is absent"() {
        given:
        def ops = new FakeExecOperations()
        ops.inspectExitValue = 1

        expect:
        DockerImagePullSupport.resolveLocalImageId(ops, DOCKER, IMAGE) == null
    }

    /**
     * Minimal {@link ExecOperations} fake. Builds a recording {@link ExecSpec} (via Groovy map coercion, since the
     * interface is large and only a handful of methods are exercised) and interprets the recorded docker command line
     * to decide whether the call is an {@code image inspect} (answered from {@link #onInspectOsArch}/
     * {@link #onInspectId} and {@link #inspectExitValue}) or a {@code pull} (which fails
     * {@link #pullFailuresBeforeSuccess} times before succeeding, mirroring a flaky registry). Only {@link #exec} is
     * used by the code under test; {@link #javaexec} is unsupported.
     */
    static class FakeExecOperations implements ExecOperations {
        Closure<String> onInspectOsArch = { "linux/amd64" }
        Closure<String> onInspectId = { "sha256:abc123" }
        int inspectExitValue = 0
        int pullFailuresBeforeSuccess = 0
        int pullInvocations = 0
        List<List<Object>> commands = []

        @Override
        ExecResult exec(Action<? super ExecSpec> action) {
            List<Object> recordedArgs = []
            OutputStream[] standardOutput = [null]
            ExecSpec spec = [
                executable       : { Object value -> null },
                getExecutable    : { -> "docker" },
                args             : { Object... a -> recordedArgs.addAll(a as List); null },
                setStandardOutput: { OutputStream out -> standardOutput[0] = out; null },
                setErrorOutput   : { OutputStream out -> null },
                setIgnoreExitValue: { boolean ignore -> null },
                environment      : { String name, Object value -> null }
            ] as ExecSpec
            action.execute(spec)
            commands << recordedArgs.collect()
            if (recordedArgs.contains("inspect")) {
                if (inspectExitValue == 0 && standardOutput[0] != null) {
                    String value = recordedArgs.contains("{{.Id}}") ? onInspectId() : onInspectOsArch()
                    standardOutput[0].write((value + "\n").getBytes("UTF-8"))
                }
                return new FakeExecResult(inspectExitValue)
            }
            // pull
            pullInvocations++
            if (pullInvocations <= pullFailuresBeforeSuccess) {
                throw new RuntimeException("simulated pull failure #" + pullInvocations)
            }
            return new FakeExecResult(0)
        }

        @Override
        ExecResult javaexec(Action<? super JavaExecSpec> action) {
            throw new UnsupportedOperationException("not used")
        }
    }

    /** Minimal {@link ExecResult} fake exposing a fixed exit value. */
    static class FakeExecResult implements ExecResult {
        private final int exitValue

        FakeExecResult(int exitValue) {
            this.exitValue = exitValue
        }

        @Override
        int getExitValue() {
            return exitValue
        }

        @Override
        ExecResult assertNormalExitValue() {
            return this
        }

        @Override
        ExecResult rethrowFailure() {
            return this
        }
    }
}
