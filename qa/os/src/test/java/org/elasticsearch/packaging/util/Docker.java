/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.fluent.Request;
import org.elasticsearch.core.CheckedRunnable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.elasticsearch.packaging.util.DockerRun.getImageName;
import static org.elasticsearch.packaging.util.FileMatcher.p444;
import static org.elasticsearch.packaging.util.FileMatcher.p555;
import static org.elasticsearch.packaging.util.FileMatcher.p664;
import static org.elasticsearch.packaging.util.FileMatcher.p770;
import static org.elasticsearch.packaging.util.FileMatcher.p775;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Utilities for running packaging tests against the Elasticsearch Docker images.
 */
public class Docker {
    private static final Log logger = LogFactory.getLog(Docker.class);

    static final Shell sh = new Shell();
    private static final DockerShell dockerShell = new DockerShell();
    public static final int STARTUP_SLEEP_INTERVAL_MILLISECONDS = 1000;
    public static final int STARTUP_ATTEMPTS_MAX = 10;

    /**
     * Tracks the currently running Docker image. An earlier implementation used a fixed container name,
     * but that appeared to cause problems with repeatedly destroying and recreating containers with
     * the same name.
     */
    private static String containerId = null;

    /**
     * Checks whether the required Docker image exists. If not, the image is loaded from disk. No check is made
     * to see whether the image is up-to-date.
     * @param distribution details about the docker image to potentially load.
     */
    public static void ensureImageIsLoaded(Distribution distribution) {
        Shell.Result result = sh.run("docker image ls --format '{{.Repository}}' " + getImageName(distribution));

        final long count = Arrays.stream(result.stdout.split("\n")).map(String::trim).filter(s -> s.isEmpty() == false).count();

        if (count != 0) {
            return;
        }

        logger.info("Loading Docker image: " + distribution.path);
        sh.run("docker load -i " + distribution.path);
    }

    /**
     * Runs an Elasticsearch Docker container, and checks that it has started up
     * successfully.
     *
     * @param distribution details about the docker image being tested
     * @return an installation that models the running container
     */
    public static Installation runContainer(Distribution distribution) {
        return runContainer(distribution, DockerRun.builder());
    }

    /**
     * Runs an Elasticsearch Docker container, and checks that it has started up
     * successfully.
     *
     * @param distribution details about the docker image being tested
     * @param builder the command to run
     * @return an installation that models the running container
     */
    public static Installation runContainer(Distribution distribution, DockerRun builder) {
        executeDockerRun(distribution, builder);

        waitForElasticsearchToStart();

        return Installation.ofContainer(dockerShell, distribution);
    }

    /**
     * Similar to {@link #runContainer(Distribution, DockerRun)} in that it runs an Elasticsearch Docker
     * container, expect that the container expecting it to exit e.g. due to configuration problem.
     *
     * @param distribution details about the docker image being tested.
     * @param builder the command to run
     * @return the docker logs of the container
     */
    public static Shell.Result runContainerExpectingFailure(Distribution distribution, DockerRun builder) {
        executeDockerRun(distribution, builder);

        waitForElasticsearchToExit();

        return getContainerLogs();
    }

    private static void executeDockerRun(Distribution distribution, DockerRun builder) {
        removeContainer();

        final String command = builder.distribution(distribution).build();

        logger.info("Running command: " + command);
        containerId = sh.run(command).stdout.trim();
    }

    /**
     * Waits for the Elasticsearch process to start executing in the container.
     * This is called every time a container is started.
     */
    public static void waitForElasticsearchToStart() {
        boolean isElasticsearchRunning = false;
        int attempt = 0;

        String psOutput = null;

        do {
            try {
                // Give the container a chance to crash out
                Thread.sleep(STARTUP_SLEEP_INTERVAL_MILLISECONDS);

                psOutput = dockerShell.run("ps -ww ax").stdout;

                if (psOutput.contains("org.elasticsearch.bootstrap.Elasticsearch")) {
                    isElasticsearchRunning = true;
                    break;
                }
            } catch (Exception e) {
                logger.warn("Caught exception while waiting for ES to start", e);
            }
        } while (attempt++ < STARTUP_ATTEMPTS_MAX);

        if (isElasticsearchRunning == false) {
            final Shell.Result dockerLogs = getContainerLogs();
            fail(
                "Elasticsearch container did not start successfully.\n\nps output:\n"
                    + psOutput
                    + "\n\nStdout:\n"
                    + dockerLogs.stdout
                    + "\n\nStderr:\n"
                    + dockerLogs.stderr
            );
        }
    }

    /**
     * Waits for the Elasticsearch container to exit.
     */
    private static void waitForElasticsearchToExit() {
        boolean isElasticsearchRunning = true;
        int attempt = 0;

        do {
            try {
                // Give the container a chance to exit out
                Thread.sleep(1000);

                if (sh.run("docker ps --quiet --no-trunc").stdout.contains(containerId) == false) {
                    isElasticsearchRunning = false;
                    break;
                }
            } catch (Exception e) {
                logger.warn("Caught exception while waiting for ES to exit", e);
            }
        } while (attempt++ < 5);

        if (isElasticsearchRunning) {
            final Shell.Result dockerLogs = getContainerLogs();
            fail("Elasticsearch container didn't exit.\n\nStdout:\n" + dockerLogs.stdout + "\n\nStderr:\n" + dockerLogs.stderr);
        }
    }

    /**
     * Removes the currently running container.
     */
    public static void removeContainer() {
        if (containerId != null) {
            try {
                // Remove the container, forcibly killing it if necessary
                logger.debug("Removing container " + containerId);
                final String command = "docker rm -f " + containerId;
                final Shell.Result result = sh.runIgnoreExitCode(command);

                if (result.isSuccess() == false) {
                    boolean isErrorAcceptable = result.stderr.contains("removal of container " + containerId + " is already in progress")
                        || result.stderr.contains("Error: No such container: " + containerId);

                    // I'm not sure why we're already removing this container, but that's OK.
                    if (isErrorAcceptable == false) {
                        throw new RuntimeException("Command was not successful: [" + command + "] result: " + result);
                    }
                }
            } finally {
                // Null out the containerId under all circumstances, so that even if the remove command fails
                // for some reason, the other tests will still proceed. Otherwise they can get stuck, continually
                // trying to remove a non-existent container ID.
                containerId = null;
            }
        }
    }

    /**
     * Copies a file from the container into the local filesystem
     * @param from the file to copy in the container
     * @param to the location to place the copy
     */
    public static void copyFromContainer(Path from, Path to) {
        final String script = "docker cp " + containerId + ":" + from + " " + to;
        logger.debug("Copying file from container with: " + script);
        sh.run(script);
    }

    /**
     * Extends {@link Shell} so that executed commands happen in the currently running Docker container.
     */
    public static class DockerShell extends Shell {
        @Override
        protected String[] getScriptCommand(String script) {
            assert containerId != null;

            List<String> cmd = new ArrayList<>();
            cmd.add("docker");
            cmd.add("exec");
            cmd.add("--tty");

            env.forEach((key, value) -> cmd.add("--env " + key + "=\"" + value + "\""));

            cmd.add(containerId);
            cmd.add(script);

            return super.getScriptCommand(String.join(" ", cmd));
        }
    }

    /**
     * Checks whether a path exists in the Docker container.
     * @param path the path that ought to exist
     * @return whether the path exists
     */
    public static boolean existsInContainer(Path path) {
        return existsInContainer(path.toString());
    }

    /**
     * Checks whether a path exists in the Docker container.
     * @param path the path that ought to exist
     * @return whether the path exists
     */
    public static boolean existsInContainer(String path) {
        logger.debug("Checking whether file " + path + " exists in container");
        final Shell.Result result = dockerShell.runIgnoreExitCode("test -e " + path);

        return result.isSuccess();
    }

    /**
     * Run privilege escalated shell command on the local file system via a bind mount inside a Docker container.
     * @param shellCmd The shell command to execute on the localPath e.g. `mkdir /containerPath/dir`.
     * @param localPath The local path where shellCmd will be executed on (inside a container).
     * @param containerPath The path to mount localPath inside the container.
     */
    private static void executePrivilegeEscalatedShellCmd(String shellCmd, Path localPath, Path containerPath) {
        final List<String> args = new ArrayList<>();

        args.add("docker run");

        // Don't leave orphaned containers
        args.add("--rm");

        // Mount localPath to a known location inside the container, so that we can execute shell commands on it later
        args.add("--volume \"" + localPath.getParent() + ":" + containerPath.getParent() + "\"");

        // Use a lightweight musl libc based small image
        args.add("alpine");

        // And run inline commands via the POSIX shell
        args.add("/bin/sh -c \"" + shellCmd + "\"");

        final String command = String.join(" ", args);
        logger.info("Running command: " + command);
        sh.run(command);
    }

    /**
     * Create a directory with specified uid/gid using Docker backed privilege escalation.
     * @param localPath The path to the directory to create.
     * @param uid The numeric id for localPath
     * @param gid The numeric id for localPath
     */
    public static void mkDirWithPrivilegeEscalation(Path localPath, int uid, int gid) {
        final Path containerBasePath = Paths.get("/mount");
        final Path containerPath = containerBasePath.resolve(Paths.get("/").relativize(localPath));
        final List<String> args = new ArrayList<>();

        args.add("mkdir " + containerPath.toAbsolutePath());
        args.add("&&");
        args.add("chown " + uid + ":" + gid + " " + containerPath.toAbsolutePath());
        args.add("&&");
        args.add("chmod 0770 " + containerPath.toAbsolutePath());
        final String command = String.join(" ", args);
        executePrivilegeEscalatedShellCmd(command, localPath, containerPath);

        final PosixFileAttributes dirAttributes = FileUtils.getPosixFileAttributes(localPath);
        final Map<String, Integer> numericPathOwnership = FileUtils.getNumericUnixPathOwnership(localPath);
        assertThat(localPath + " has wrong uid", numericPathOwnership.get("uid"), equalTo(uid));
        assertThat(localPath + " has wrong gid", numericPathOwnership.get("gid"), equalTo(gid));
        assertThat(localPath + " has wrong permissions", dirAttributes.permissions(), equalTo(p770));
    }

    /**
     * Delete a directory using Docker backed privilege escalation.
     * @param localPath The path to the directory to delete.
     */
    public static void rmDirWithPrivilegeEscalation(Path localPath) {
        final Path containerBasePath = Paths.get("/mount");
        final Path containerPath = containerBasePath.resolve(localPath.getParent().getFileName());
        final List<String> args = new ArrayList<>();

        args.add("cd " + containerBasePath.toAbsolutePath());
        args.add("&&");
        args.add("rm -rf " + localPath.getFileName());
        final String command = String.join(" ", args);
        executePrivilegeEscalatedShellCmd(command, localPath, containerPath);
    }

    /**
     * Change the ownership of a path using Docker backed privilege escalation.
     * @param localPath The path to the file or directory to change.
     * @param ownership the ownership to apply. Can either be just the user, or the user and group, separated by a colon (":"),
     *                  or just the group if prefixed with a colon.
     */
    public static void chownWithPrivilegeEscalation(Path localPath, String ownership) {
        final Path containerBasePath = Paths.get("/mount");
        final Path containerPath = containerBasePath.resolve(localPath.getParent().getFileName());
        final List<String> args = new ArrayList<>();

        args.add("cd " + containerBasePath.toAbsolutePath());
        args.add("&&");
        args.add("chown -R " + ownership + " " + localPath.getFileName());
        final String command = String.join(" ", args);
        executePrivilegeEscalatedShellCmd(command, localPath, containerPath);
    }

    /**
     * Checks that the specified path's permissions and ownership match those specified.
     * <p>
     * The implementation supports multiple files being matched by the path, via bash expansion, although
     * it is expected that only the final part of the path will contain expansions.
     *
     * @param path the path to check, possibly with e.g. a wildcard (<code>*</code>)
     * @param expectedUser the file's expected user
     * @param expectedGroup the file's expected group
     * @param expectedPermissions the unix permissions that the path ought to have
     */
    public static void assertPermissionsAndOwnership(
        Path path,
        String expectedUser,
        String expectedGroup,
        Set<PosixFilePermission> expectedPermissions
    ) {
        logger.debug("Checking permissions and ownership of [" + path + "]");

        final Shell.Result result = dockerShell.run("bash -c 'stat --format=\"%n %U %G %A\" '" + path);

        final Path parent = path.getParent();

        Arrays.asList(result.stdout.split("\\n")).forEach(line -> {
            final String[] components = line.split("\\s+");

            final String filename = components[0];
            final String username = components[1];
            final String group = components[2];
            final String permissions = components[3];

            // The final substring() is because we don't check the directory bit, and we
            // also don't want any SELinux security context indicator.
            Set<PosixFilePermission> actualPermissions = fromString(permissions.substring(1, 10));

            String fullPath = filename.startsWith("/") ? filename : parent + "/" + filename;

            assertEquals("Permissions of " + fullPath + " are wrong", expectedPermissions, actualPermissions);
            assertThat("File owner of " + fullPath + " is wrong", username, equalTo(expectedUser));
            assertThat("File group of " + fullPath + " is wrong", group, equalTo(expectedGroup));
        });
    }

    /**
     * Waits for up to 20 seconds for a path to exist in the container.
     * @param path the path to await
     */
    public static void waitForPathToExist(Path path) throws InterruptedException {
        int attempt = 0;

        do {
            if (existsInContainer(path)) {
                return;
            }

            Thread.sleep(1000);
        } while (attempt++ < 20);

        fail(path + " failed to exist after 5000ms");
    }

    /**
     * Perform a variety of checks on an installation.
     * @param es the installation to verify
     */
    public static void verifyContainerInstallation(Installation es) {
        // Ensure the `elasticsearch` user and group exist.
        // These lines will both throw an exception if the command fails
        dockerShell.run("id elasticsearch");
        dockerShell.run("getent group elasticsearch");

        final Shell.Result passwdResult = dockerShell.run("getent passwd elasticsearch");
        final String homeDir = passwdResult.stdout.trim().split(":")[5];
        assertThat("elasticsearch user's home directory is incorrect", homeDir, equalTo("/usr/share/elasticsearch"));

        assertPermissionsAndOwnership(es.home, "root", "root", p775);

        Stream.of(es.bundledJdk, es.lib, es.modules).forEach(dir -> assertPermissionsAndOwnership(dir, "root", "root", p555));

        // You can't install plugins that include configuration when running as `elasticsearch` and the `config`
        // dir is owned by `root`, because the installed tries to manipulate the permissions on the plugin's
        // config directory.
        Stream.of(es.bin, es.config, es.logs, es.config.resolve("jvm.options.d"), es.data, es.plugins)
            .forEach(dir -> assertPermissionsAndOwnership(dir, "elasticsearch", "root", p775));

        Stream.of(es.bin, es.bundledJdk.resolve("bin"), es.modules.resolve("x-pack-ml/platform/linux-*/bin"))
            .forEach(binariesPath -> assertPermissionsAndOwnership(binariesPath.resolve("*"), "root", "root", p555));

        Stream.of("elasticsearch.yml", "jvm.options", "log4j2.properties", "role_mapping.yml", "roles.yml", "users", "users_roles")
            .forEach(configFile -> assertPermissionsAndOwnership(es.config(configFile), "root", "root", p664));

        Stream.of("LICENSE.txt", "NOTICE.txt", "README.asciidoc")
            .forEach(doc -> assertPermissionsAndOwnership(es.home.resolve(doc), "root", "root", p444));

        assertThat(dockerShell.run(es.bin("elasticsearch-keystore") + " list").stdout, containsString("keystore.seed"));

        // nc is useful for checking network issues
        // zip/unzip are installed to help users who are working with certificates.
        Stream.of("nc", "unzip", "zip")
            .forEach(
                cliBinary -> assertTrue(
                    cliBinary + " ought to be available.",
                    dockerShell.runIgnoreExitCode("bash -c  'hash " + cliBinary + "'").isSuccess()
                )
            );
    }

    public static void waitForElasticsearch(Installation installation) throws Exception {
        withLogging(() -> ServerUtils.waitForElasticsearch(installation));
    }

    public static void waitForElasticsearch(String status, String index, Installation installation, String username, String password)
        throws Exception {
        withLogging(() -> ServerUtils.waitForElasticsearch(status, index, installation, username, password));
    }

    /**
     * Runs the provided closure, and captures logging information if an exception is thrown.
     * @param r the closure to run
     * @throws Exception any exception encountered while running the closure are propagated.
     */
    private static <E extends Exception> void withLogging(CheckedRunnable<E> r) throws Exception {
        try {
            r.run();
        } catch (Exception e) {
            final Shell.Result logs = getContainerLogs();
            logger.warn("Elasticsearch container failed to start.\n\nStdout:\n" + logs.stdout + "\n\nStderr:\n" + logs.stderr);
            throw e;
        }
    }

    /**
     * @return The ID of the container that this class will be operating on.
     */
    public static String getContainerId() {
        return containerId;
    }

    /**
     * Performs an HTTP GET to <code>http://localhost:9200/</code> with the supplied path.
     * @param path the path to fetch, which must start with <code>/</code>
     * @return the parsed response
     */
    public static JsonNode getJson(String path) throws Exception {
        path = Objects.requireNonNull(path).trim();
        if (path.isEmpty()) {
            throw new IllegalArgumentException("path must be supplied");
        }
        if (path.startsWith("/") == false) {
            throw new IllegalArgumentException("path must start with /");
        }
        final String pluginsResponse = makeRequest(Request.Get("http://localhost:9200" + path));

        ObjectMapper mapper = new ObjectMapper();

        return mapper.readTree(pluginsResponse);
    }

    /**
     * Fetches all the labels for a Docker image
     * @param distribution required to derive the image name
     * @return a mapping from label name to value
     */
    public static Map<String, String> getImageLabels(Distribution distribution) throws Exception {
        final JsonNode jsonNode = getImageInspectionJson(distribution).at("/Config/Labels");

        Map<String, String> labels = new HashMap<>();

        jsonNode.fieldNames().forEachRemaining(field -> labels.put(field, jsonNode.get(field).asText()));

        return labels;
    }

    /**
     * Fetches the <code>HEALTHCHECK</code> command for a Docker image
     * @param distribution required to derive the image name
     * @return a list of values from `docker inspect`, or null if there is no healthcheck defined
     */
    public static List<String> getImageHealthcheck(Distribution distribution) throws Exception {
        final JsonNode jsonNode = getImageInspectionJson(distribution).at("/Config/Healthcheck/Test");

        if (jsonNode.isMissingNode()) {
            return null;
        }

        List<String> healthcheck = new ArrayList<>(jsonNode.size());

        for (JsonNode node : jsonNode) {
            healthcheck.add(node.textValue());
        }

        return healthcheck;
    }

    private static JsonNode getImageInspectionJson(Distribution distribution) throws Exception {
        String labelsJson = sh.run("docker inspect " + getImageName(distribution)).stdout;
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(labelsJson).get(0);
    }

    public static Shell.Result getContainerLogs() {
        return sh.run("docker logs " + containerId);
    }

    /**
     * Restarts the current docker container.
     */
    public static void restartContainer() {
        sh.run("docker restart " + containerId);
    }
}
