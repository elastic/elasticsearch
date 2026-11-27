/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.arrowinterop;

import org.elasticsearch.test.fixtures.testcontainers.DockerAvailability;
import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * A container with an official Apache Arrow client ({@code pyarrow} or {@code apache-arrow} for Node.js), that
 * fetches ES|QL Arrow responses from a running Elasticsearch node.
 * <p>
 * The client POSTs a JSON body to the given URL asking for an Arrow stream, decodes the response with its
 * Arrow library, and prints a JSON description of the schema and values on stdout (see {@code fetch_arrow.py}
 * and {@code fetch_arrow.mjs} for the exact shape). Tests parse that JSON and assert against the values they
 * expect, so that expectations live in Java only.
 * <p>
 * Both clients normalize their output (type names, timestamps as epoch millis, big integers as strings, etc.)
 * so that they print identical JSON for the same stream: a test needs a single table of expected values for
 * all languages. The conventions are documented in the scripts, which must be kept in sync.
 * <p>
 * The container is meant to be used as a JUnit {@code @ClassRule}: it is started once for a test class (which
 * builds its image and skips the class if Docker isn't available), idles, and each {@link #fetch} runs the client in
 * it with {@code docker exec}.
 * <p>
 * The images are always built locally from the Dockerfiles in {@code docker/<language>/}; they are not
 * published. Docker's layer cache makes subsequent builds cheap.
 * <p>
 * Since the node under test runs on the Docker host, use {@link #HOST_ADDRESS} to reach it. This requires its port to be
 * exposed with {@link #exposeHostPort} <em>before</em> the container starts. The port is relayed by Testcontainers, so the node
 * can keep listening on the loopback interface only.
 */
public final class ArrowInteropContainer extends DockerEnvironmentAwareTestContainer {

    public enum Language {
        PYTHON("python"),
        NODE("node");

        private final String directory;

        Language(String directory) {
            this.directory = directory;
        }

        /** Dockerfile build context, relative to the classpath root. */
        private String resourceDir() {
            return "docker/" + directory + "/";
        }

        private String imageName() {
            return "elasticsearch-test/arrow-interop-" + directory + ":1";
        }

        private String[] contextFiles() {
            return switch (this) {
                case PYTHON -> new String[] { "Dockerfile", "requirements.txt", "fetch_arrow.py" };
                case NODE -> new String[] { "Dockerfile", "package.json", "package-lock.json", "fetch_arrow.mjs" };
            };
        }

        /** The command that runs the client. */
        private String[] clientCommand() {
            return switch (this) {
                case PYTHON -> new String[] { "python", "/app/fetch_arrow.py" };
                case NODE -> new String[] { "node", "/app/fetch_arrow.mjs" };
            };
        }
    }

    /**
     * Host name under which a container reaches the ports of the Docker host (where the Elasticsearch node under test runs)
     * that were exposed with {@link #exposeHostPort}.
     */
    public static final String HOST_ADDRESS = "host.testcontainers.internal";

    /** A client that doesn't terminate in this time is killed, and its {@link #fetch} fails. */
    private static final int CLIENT_TIMEOUT_SECONDS = 120;

    /** Exit code of the {@code timeout} command when it had to kill its command. */
    private static final int TIMEOUT_EXIT_CODE = 124;

    private final Language language;

    /**
     * For caching images in CI agent images only: {@code CacheCacheableTestFixtures} calls this by reflection, then
     * {@link #cache()}, which builds the images of all languages, so the language here is irrelevant.
     */
    protected ArrowInteropContainer() {
        this(Language.PYTHON);
    }

    public ArrowInteropContainer(Language language) {
        super(image(language));
        this.language = language;
        // Keep the container running, as clients are run in it
        withCommand("sleep", "infinity");
    }

    /**
     * Makes a port of the Docker host reachable from the containers that start after this call, as {@link #HOST_ADDRESS} and that
     * port. Testcontainers relays it through a helper container, so that the service doesn't have to listen on a non-loopback
     * interface and be reachable from the network.
     */
    public static void exposeHostPort(int port) {
        Testcontainers.exposeHostPorts(port);
    }

    public Language language() {
        return language;
    }

    private static ImageFromDockerfile image(Language language) {
        // Don't delete the image on exit so that Docker's layer cache avoids re-installing the Arrow library every time
        var image = new ImageFromDockerfile(language.imageName(), false);
        for (String file : language.contextFiles()) {
            image.withFileFromClasspath(file, language.resourceDir() + file);
        }
        return image;
    }

    /**
     * Builds the images of all languages, without running them, so that they are in the Docker image store of the CI
     * agent images (including their base images and the Arrow libraries installed by the Dockerfiles). The tests
     * rebuild the images, which then only reuses the cached layers instead of needing the network. A Dockerfile or
     * dependency change invalidates the affected layers until the agent images are rebuilt.
     */
    @Override
    public void cache() {
        try {
            DockerAvailability.assumeDockerIsAvailable();
            for (Language language : Language.values()) {
                LOGGER.info("Building image {}", language.imageName());
                image(language).get();
            }
        } catch (RuntimeException e) {
            LOGGER.warn("Error while caching container images.", e);
        }
    }

    /**
     * Runs the Arrow client in the (started) container: POSTs {@code jsonBody} to {@code url} and returns what the
     * client printed on stdout. The URL must be reachable from inside the container, e.g.
     * {@code "http://" + HOST_ADDRESS + ":" + port + "/_query?format=arrow"}.
     *
     * @throws AssertionError if the client failed, with its output as the message
     */
    public String fetch(String url, String jsonBody) {
        ExecResult result;
        try {
            result = execInContainer(
                StandardCharsets.UTF_8,
                ExecConfig.builder()
                    // "timeout" kills a client that hangs, which would otherwise block the test until the suite times out
                    .command(withTimeout(language.clientCommand()))
                    .envVars(Map.of("ARROW_URL", url, "ARROW_BODY", jsonBody))
                    .build()
            );
        } catch (IOException e) {
            throw new AssertionError("Failed to run the Arrow " + language + " client", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while running the Arrow " + language + " client", e);
        }

        if (result.getExitCode() != 0) {
            // The client reports errors on stderr
            throw new AssertionError(
                "Arrow "
                    + language
                    + " client failed"
                    + (result.getExitCode() == TIMEOUT_EXIT_CODE ? " (timeout after " + CLIENT_TIMEOUT_SECONDS + "s)" : "")
                    + ", exit code "
                    + result.getExitCode()
                    + ":\n"
                    + result.getStderr()
            );
        }
        return result.getStdout();
    }

    private static String[] withTimeout(String[] command) {
        String[] withTimeout = new String[command.length + 2];
        withTimeout[0] = "timeout";
        withTimeout[1] = String.valueOf(CLIENT_TIMEOUT_SECONDS);
        System.arraycopy(command, 0, withTimeout, 2, command.length);
        return withTimeout;
    }
}
