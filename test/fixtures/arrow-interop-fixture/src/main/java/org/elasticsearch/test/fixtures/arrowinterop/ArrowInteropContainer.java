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
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.time.Duration;

/**
 * A one-shot container that runs an official Apache Arrow client ({@code pyarrow} or {@code apache-arrow}
 * for Node.js) against a running Elasticsearch node.
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
 * The images are always built locally from the Dockerfiles in {@code docker/<language>/}; they are not
 * published. Docker's layer cache makes subsequent builds cheap.
 * <p>
 * Since the node under test runs on the Docker host, use {@link #HOST_ADDRESS} to reach it, and make sure it
 * listens on a non-loopback interface.
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
    }

    /** Host name under which a container reaches the Docker host (where the Elasticsearch node under test runs). */
    public static final String HOST_ADDRESS = "host.docker.internal";

    private static final Duration CLIENT_TIMEOUT = Duration.ofMinutes(2);

    /**
     * for packer caching only
     * see CacheCacheableTestFixtures.
     */
    protected ArrowInteropContainer() {
        this(Language.PYTHON, "", "{}");
    }

    private ArrowInteropContainer(Language language, String url, String jsonBody) {
        super(image(language));
        withEnv("ARROW_URL", url);
        withEnv("ARROW_BODY", jsonBody);
        // On Linux, host.docker.internal is not defined by default (it is on Docker Desktop)
        withExtraHost(HOST_ADDRESS, "host-gateway");
        // The client runs and exits: wait for it to finish, and fail if its exit code is not 0
        setStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(CLIENT_TIMEOUT));
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
     * Runs the Arrow client of the given language: POSTs {@code jsonBody} to {@code url} and returns what the
     * client printed on stdout. The URL must be reachable from inside the container, e.g.
     * {@code "http://" + HOST_ADDRESS + ":" + port + "/_query?format=arrow"}.
     *
     * The test is skipped (JUnit assumption) if Docker is not available, except on CI where it is expected to be.
     *
     * @throws AssertionError if the client failed, with its output as the message
     */
    public static String fetch(Language language, String url, String jsonBody) {
        // Checked first, as an assumption failure must not be reported as a client failure by the catch below
        DockerAvailability.assumeDockerIsAvailable();
        var container = new ArrowInteropContainer(language, url, jsonBody);
        try {
            container.start();
            return container.getLogs(OutputFrame.OutputType.STDOUT);
        } catch (RuntimeException e) {
            // Includes the startup check failing on a non-zero exit code. The client reports errors on stderr.
            throw new AssertionError(
                "Arrow " + language + " client failed: " + e.getMessage() + "\n" + container.getLogs(OutputFrame.OutputType.STDERR),
                e
            );
        } finally {
            container.stop();
        }
    }
}
