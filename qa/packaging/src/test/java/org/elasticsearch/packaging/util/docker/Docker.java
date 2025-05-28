/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.util.docker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.fluent.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Distribution.Packaging;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;

import java.io.FileNotFoundException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.elasticsearch.packaging.test.PackagingTestCase.assertBusy;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.Directory;
import static org.elasticsearch.packaging.util.FileMatcher.p444;
import static org.elasticsearch.packaging.util.FileMatcher.p555;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileMatcher.p664;
import static org.elasticsearch.packaging.util.FileMatcher.p770;
import static org.elasticsearch.packaging.util.FileMatcher.p775;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.elasticsearch.packaging.util.docker.DockerFileMatcher.file;
import static org.elasticsearch.packaging.util.docker.DockerRun.getImageName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Utilities for running packaging tests against the Elasticsearch Docker images.
 */
public class Docker {
    private static final Log logger = LogFactory.getLog(Docker.class);

    public static final Shell sh = new Shell();
    public static final DockerShell dockerShell = new DockerShell();
    public static final int STARTUP_SLEEP_INTERVAL_MILLISECONDS = 1000;
    public static final int STARTUP_ATTEMPTS_MAX = 30;

    private static final String ELASTICSEARCH_FULL_CLASSNAME = "org.elasticsearch.bootstrap.Elasticsearch";
    private static final String FIND_ELASTICSEARCH_PROCESS = "for pid in $(ps -eo pid,comm | grep java | awk '\\''{print $1}'\\''); "
        + "do cmdline=$(tr \"\\0\" \" \" < /proc/$pid/cmdline 2>/dev/null); [[ $cmdline == *"
        + ELASTICSEARCH_FULL_CLASSNAME
        + "* ]] && echo \"$pid: $cmdline\"; done";
    // The length of the command exceeds what we can use for COLUMNS so we use a pipe to detect the process we're looking for

    /**
     * Tracks the currently running Docker image. An earlier implementation used a fixed container name,
     * but that appeared to cause problems with repeatedly destroying and recreating containers with
     * the same name.
     */
    static String containerId = null;

    /**
     * Checks whether the required Docker image exists. If not, the image is loaded from disk. No check is made
     * to see whether the image is up-to-date.
     * @param distribution details about the docker image to potentially load.
     */
    public static void ensureImageIsLoaded(Distribution distribution) {
        final long count = sh.run("docker image ls --format '{{.Repository}}' " + getImageName(distribution)).stdout().lines().count();

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
     * Runs an Elasticsearch Docker container without removing any existing containers first,
     * and checks that it has started up successfully.
     *
     * @param distribution details about the docker image being tested
     * @param builder the command to run
     * @param restPort the port to expose the REST endpoint on
     * @param transportPort the port to expose the transport endpoint on
     * @return an installation that models the running container
     */
    public static Installation runAdditionalContainer(Distribution distribution, DockerRun builder, int restPort, int transportPort) {
        // TODO Maybe revisit this as part of https://github.com/elastic/elasticsearch/issues/79688
        final String command = builder.distribution(distribution)
            .extraArgs("--publish", transportPort + ":9300", "--publish", restPort + ":9200")
            .build();
        logger.info("Running command: " + command);
        containerId = sh.run(command).stdout().trim();
        waitForElasticsearchToStart();
        return Installation.ofContainer(dockerShell, distribution, restPort);
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
        containerId = sh.run(command).stdout().trim();
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
                // Give the container enough time for security auto-configuration or a chance to crash out
                Thread.sleep(STARTUP_SLEEP_INTERVAL_MILLISECONDS);
                psOutput = dockerShell.run("bash -c '" + FIND_ELASTICSEARCH_PROCESS + " | wc -l'").stdout();
                if (psOutput.contains("1")) {
                    isElasticsearchRunning = true;
                    break;
                }
            } catch (Exception e) {
                logger.warn("Caught exception while waiting for ES to start", e);
            }
        } while (attempt++ < STARTUP_ATTEMPTS_MAX);

        if (isElasticsearchRunning == false) {
            final Shell.Result dockerLogs = getContainerLogs();
            fail(String.format(Locale.ROOT, """
                Elasticsearch container did not start successfully.

                ps output:
                %s

                Stdout:
                %s

                Stderr:
                %s

                Thread dump:
                %s\
                """, psOutput, dockerLogs.stdout(), dockerLogs.stderr(), getThreadDump()));
        }
    }

    /**
     * @return output of jstack for currently running Java process
     */
    private static String getThreadDump() {
        try {
            String pid = dockerShell.run("/usr/share/elasticsearch/jdk/bin/jps | grep -v 'Jps' | awk '{print $1}'").stdout();
            if (pid.isEmpty() == false) {
                return dockerShell.run("/usr/share/elasticsearch/jdk/bin/jstack " + Integer.parseInt(pid)).stdout();
            }
        } catch (Exception e) {
            logger.error("Failed to get thread dump", e);
        }

        return "";
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
                Thread.sleep(2000);

                if (sh.run("docker ps --quiet --no-trunc").stdout().contains(containerId) == false) {
                    isElasticsearchRunning = false;
                    break;
                }
            } catch (Exception e) {
                logger.warn("Caught exception while waiting for ES to exit", e);
            }
        } while (attempt++ < 60);

        if (isElasticsearchRunning) {
            final Shell.Result dockerLogs = getContainerLogs();
            fail(String.format(Locale.ROOT, """
                Elasticsearch container didn't exit.

                stdout():
                %s

                Stderr:
                %s\
                """, dockerLogs.stdout(), dockerLogs.stderr()));
        }
    }

    /**
     * Removes the container with a given id
     */
    public static void removeContainer(String containerId) {
        if (containerId != null) {
            // Remove the container, forcibly killing it if necessary
            logger.debug("Removing container " + containerId);
            final String command = "docker rm -f " + containerId;
            final Shell.Result result = sh.runIgnoreExitCode(command);

            if (result.isSuccess() == false) {
                boolean isErrorAcceptable = result.stderr().contains("removal of container " + containerId + " is already in progress")
                    || result.stderr().contains("Error: No such container: " + containerId);

                // I'm not sure why we're already removing this container, but that's OK.
                if (isErrorAcceptable == false) {
                    throw new RuntimeException("Command was not successful: [" + command + "] result: " + result);
                }
            }

        }
    }

    /**
     * Removes the currently running container.
     */
    public static void removeContainer() {
        try {
            removeContainer(containerId);
        } finally {
            // Null out the containerId under all circumstances, so that even if the remove command fails
            // for some reason, the other tests will still proceed. Otherwise they can get stuck, continually
            // trying to remove a non-existent container ID.
            containerId = null;
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
     * Finds a file or dir in the container and returns its path ( in the container ). If there are multiple matches for the given
     * pattern, only the first is returned.
     *
     * @param base The base path in the container to start the search from
     * @param type The type we're looking for , d for directories or f for files.
     * @param pattern the pattern (case insensitive) that matches the file/dir name
     * @return a Path pointing to the file/directory in the container
     */
    public static Path findInContainer(Path base, String type, String pattern) throws InvalidPathException {
        logger.debug("Trying to look for " + pattern + " ( " + type + ") in " + base + " in the container");
        final String script = "docker exec " + containerId + " find " + base + " -type " + type + " -iname " + pattern;
        final Shell.Result result = sh.run(script);
        if (result.isSuccess() && Strings.isNullOrEmpty(result.stdout()) == false) {
            String path = result.stdout();
            if (path.split(System.lineSeparator()).length > 1) {
                path = path.split(System.lineSeparator())[1];
            }
            return Path.of(path);
        }
        return null;
    }

    /**
     * Run privilege escalated shell command on the local file system via a bind mount inside a Docker container.
     * @param shellCmd The shell command to execute on the localPath e.g. `mkdir /containerPath/dir`.
     * @param localPath The local path where shellCmd will be executed on (inside a container).
     * @param containerPath The path to mount localPath inside the container.
     */
    private static void executePrivilegeEscalatedShellCmd(String shellCmd, Path localPath, Path containerPath) {
        final String image = "alpine:3.13";
        ensureImageIsPulled(image);

        final List<String> args = new ArrayList<>();

        args.add("docker run");

        // Don't leave orphaned containers
        args.add("--rm");

        // Mount localPath to a known location inside the container, so that we can execute shell commands on it later
        args.add("--volume \"" + localPath.getParent() + ":" + containerPath.getParent() + "\"");

        // Use a lightweight musl libc based small image
        args.add(image);

        // And run inline commands via the POSIX shell
        args.add("/bin/sh -c \"" + shellCmd + "\"");

        final String command = String.join(" ", args);
        logger.info("Running command: " + command);
        sh.run(command);
    }

    private static void ensureImageIsPulled(String image) {
        // Don't pull if the image already exists. This does also mean that we never refresh it, but that
        // isn't an issue in CI.
        if (sh.runIgnoreExitCode("docker image inspect -f '{{ .Id }}' " + image).isSuccess()) {
            return;
        }

        Shell.Result result = null;
        int i = 0;
        while (true) {
            result = sh.runIgnoreExitCode("docker pull " + image);
            if (result.isSuccess()) {
                return;
            }

            if (++i == 3) {
                throw new RuntimeException("Failed to pull Docker image [" + image + "]: " + result);
            }

            try {
                Thread.sleep(10_000L);
            } catch (InterruptedException e) {
                // ignore
            }
        }
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
        dockerShell.run("grep -E '^elasticsearch:' /etc/group");

        final Shell.Result passwdResult = dockerShell.run("grep -E '^elasticsearch:' /etc/passwd");
        final String homeDir = passwdResult.stdout().trim().split(":")[5];
        assertThat("elasticsearch user's home directory is incorrect", homeDir, equalTo("/usr/share/elasticsearch"));

        assertThat(es.home, file(Directory, "root", "root", p775));

        Stream.of(es.bundledJdk, es.lib, es.modules).forEach(dir -> assertThat(dir, file(Directory, "root", "root", p555)));

        // You couldn't install plugins that include configuration when running as `elasticsearch` if the `config`
        // dir is owned by `root`, because the installed tries to manipulate the permissions on the plugin's
        // config directory.
        Stream.of(es.bin, es.config, es.logs, es.config.resolve("jvm.options.d"), es.data, es.plugins)
            .forEach(dir -> assertThat(dir, file(Directory, "elasticsearch", "root", p775)));

        final String arch = dockerShell.run("arch").stdout().trim();

        Stream.of(es.bin, es.bundledJdk.resolve("bin"), es.modules.resolve("x-pack-ml/platform/linux-" + arch + "/bin"))
            .forEach(
                binariesPath -> listContents(binariesPath).forEach(
                    binFile -> assertThat(binariesPath.resolve(binFile), file("root", "root", p555))
                )
            );

        Stream.of("jvm.options", "log4j2.properties", "role_mapping.yml", "roles.yml", "users", "users_roles")
            .forEach(configFile -> assertThat(es.config(configFile), file("root", "root", p664)));
        // We write to the elasticsearch.yml and elasticsearch.keystore in AutoConfigureNode so it gets owned by elasticsearch.
        assertThat(es.config("elasticsearch.yml"), file("elasticsearch", "root", p664));
        assertThat(es.config("elasticsearch.keystore"), file("elasticsearch", "root", p660));

        Stream.of("LICENSE.txt", "NOTICE.txt", "README.asciidoc")
            .forEach(doc -> assertThat(es.home.resolve(doc), file("root", "root", p444)));

        assertThat(dockerShell.run(es.bin("elasticsearch-keystore") + " list").stdout(), containsString("keystore.seed"));

        // nc is useful for checking network issues
        // zip/unzip are installed to help users who are working with certificates.
        Stream.of("nc", "unzip", "zip")
            .forEach(
                cliBinary -> assertTrue(
                    cliBinary + " ought to be available.",
                    dockerShell.runIgnoreExitCode("bash -c  'hash " + cliBinary + "'").isSuccess()
                )
            );

        if (es.distribution.packaging == Packaging.DOCKER_CLOUD_ESS) {
            verifyCloudContainerInstallation(es);
        }
    }

    private static void verifyCloudContainerInstallation(Installation es) {
        final String pluginArchive = "/opt/plugins/archive";
        final List<String> plugins = listContents(pluginArchive);

        if (es.distribution.packaging == Packaging.DOCKER_CLOUD_ESS) {
            assertThat("ESS image should come with plugins in " + pluginArchive, plugins, not(empty()));

            final List<String> repositoryPlugins = plugins.stream()
                .filter(p -> p.matches("^repository-(?:s3|gcs|azure)$"))
                .collect(Collectors.toList());
            // Assert on equality to that the error reports the unexpected values.
            assertThat(
                "ESS image should not have repository plugins in " + pluginArchive,
                repositoryPlugins,
                equalTo(Collections.emptyList())
            );
        } else {
            assertThat("Cloud image should not have any plugins in " + pluginArchive, plugins, empty());
        }

        // Cloud uses `wget` to install plugins / bundles.
        Stream.of("wget")
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

    public static void waitForElasticsearch(Installation installation, String username, String password) {
        waitForElasticsearch(installation, username, password, null);
    }

    /**
     * Waits for the Elasticsearch cluster status to turn green.
     *
     * @param installation the installation to check
     * @param username the username to authenticate with
     * @param password the password to authenticate with
     * @param caCert the CA cert to trust
     */
    public static void waitForElasticsearch(Installation installation, String username, String password, Path caCert) {
        try {
            withLogging(() -> ServerUtils.waitForElasticsearch("green", null, installation, username, password, caCert));
        } catch (Exception e) {
            throw new AssertionError(
                "Failed to check whether Elasticsearch had started. This could be because "
                    + "authentication isn't working properly. Check the container logs",
                e
            );
        }
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
            logger.warn("Elasticsearch container failed to start.\n\nStdout:\n" + logs.stdout() + "\n\nStderr:\n" + logs.stderr());
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
        path = Objects.requireNonNull(path, "path can not be null").trim();
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
     * Fetches the resource from the specified {@code path} on {@code http(s)://localhost:9200}, using
     * the supplied authentication credentials.
     *
     * @param path the path to fetch
     * @param user the user to authenticate with
     * @param password the password to authenticate with
     * @param caCert CA cert to trust, if non-null use the https URL
     * @return a parsed JSON response
     * @throws Exception if something goes wrong
     */
    public static JsonNode getJson(String path, String user, String password, @Nullable Path caCert) throws Exception {
        path = Objects.requireNonNull(path, "path can not be null").trim();
        if (path.isEmpty()) {
            throw new IllegalArgumentException("path must be supplied");
        }
        if (path.startsWith("/") == false) {
            throw new IllegalArgumentException("path must start with /");
        }

        final String pluginsResponse;
        if (caCert == null) {
            pluginsResponse = makeRequest(Request.Get("http://localhost:9200" + path), user, password, null);
        } else {
            pluginsResponse = makeRequest(Request.Get("https://localhost:9200" + path), user, password, caCert);
        }

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
        String labelsJson = sh.run("docker inspect " + getImageName(distribution)).stdout();
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(labelsJson).get(0);
    }

    public static Shell.Result getContainerLogs() {
        return getContainerLogs(containerId);
    }

    public static Shell.Result getContainerLogs(String containerId) {
        return sh.run("docker logs " + containerId);
    }

    /**
     * Restarts the current docker container.
     */
    public static void restartContainer() {
        sh.run("docker restart " + containerId);
    }

    static PosixFileAttributes getAttributes(Path path) throws FileNotFoundException {
        final Shell.Result result = dockerShell.runIgnoreExitCode("stat -c \"%U %G %A\" " + path);
        if (result.isSuccess() == false) {
            throw new FileNotFoundException(path + " does not exist");
        }

        final String[] components = result.stdout().split("\\s+");

        final String permissions = components[2];
        final String fileType = permissions.substring(0, 1);

        // The final substring() is because we don't check the directory bit, and we
        // also don't want any SELinux security context indicator.
        Set<PosixFilePermission> posixPermissions = fromString(permissions.substring(1, 10));

        final DockerFileAttributes attrs = new DockerFileAttributes();
        attrs.owner = components[0];
        attrs.group = components[1];
        attrs.permissions = posixPermissions;
        attrs.isDirectory = fileType.equals("d");
        attrs.isSymbolicLink = fileType.equals("l");

        return attrs;
    }

    /**
     * Returns a list of the file contents of the supplied path.
     * @param path the path to list
     * @return the listing
     */
    public static List<String> listContents(String path) {
        return dockerShell.run("ls -1 --color=never " + path).stdout().lines().collect(Collectors.toList());
    }

    /**
     * Returns a list of the file contents of the supplied path.
     * @param path the path to list
     * @return the listing
     */
    public static List<String> listContents(Path path) {
        return listContents(path.toString());
    }

    /**
     * Waits for an Elasticsearch node start by looking at the cluster logs. This is useful if the
     * container is not available on an external port, or authentication is in force and credentials
     * are not available.
     * @param containerId the container to check
     */
    public static void waitForNodeStarted(String containerId) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        // Some lines are not JSON, filter those out
        assertBusy(() -> assertTrue(getContainerLogs(containerId).stdout().lines().filter(line -> line.startsWith("{")).map(line -> {
            try {
                return mapper.readTree(line);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        })
            .anyMatch(
                json -> json.get("message").textValue().contains("started")
                    && json.get("log.logger").textValue().equals("org.elasticsearch.node.Node")
            )), 60, TimeUnit.SECONDS);
    }

    /**
     * Runs a readiness probe on a given port
     * @param port
     * @return the ready status
     */
    public static boolean readinessProbe(int port) {
        Shell.Result result = dockerShell.runIgnoreExitCode("nc -z localhost " + port);
        return result.exitCode() == 0;
    }
}
