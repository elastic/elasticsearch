package org.elasticsearch.gradle;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contains utilities for checking whether Docker is installed, is executable,
 * has a recent enough version, and appears to be functional. The Elasticsearch build
 * requires Docker &gt;= 17.05 as it uses a multi-stage build.
 */
public class DockerUtils {
    /**
     * Defines the possible locations of the Docker CLI. These will be searched in order.
     */
    public static String[] DOCKER_BINARIES = { "/usr/bin/docker", "/usr/local/bin/docker" };

    /**
     * Searches the entries in {@link #DOCKER_BINARIES} for the Docker CLI. This method does
     * not check whether the Docker installation appears usable, see {@link #getDockerAvailability()}
     * instead.
     *
     * @return the path to a CLI, if available.
     */
    public static Optional<String> getDockerPath() {
        // Check if the Docker binary exists
        return List.of(DOCKER_BINARIES)
            .stream()
            .filter(path -> new File(path).exists())
            .findFirst();
    }

    /**
     * Searches for a functional Docker installation, and returns information about the search.
     * @return the results of the search.
     */
    public static DockerAvailability getDockerAvailability() {
        final DockerAvailability dockerAvailability = new DockerAvailability();

        // Check if the Docker binary exists
        final Optional<String> dockerBinary = getDockerPath();

        if (dockerBinary.isEmpty()) {
            return dockerAvailability;
        }

        dockerAvailability.path = dockerBinary.get();

        // Since we use a multi-stage Docker build, check the Docker version since 17.05
        final Result versionResult = runCommand(dockerAvailability.path, "--version");

        if (versionResult.isSuccess() == false) {
            dockerAvailability.lastCommand = versionResult;
            return dockerAvailability;
        }

        final String version = extractVersion(versionResult.stdout.trim());
        dockerAvailability.version = version;

        if (checkVersion(version) == false) {
            return dockerAvailability;
        }

        dockerAvailability.isVersionHighEnough = true;

        // The Docker binary executes, so check that we can execute a privileged command
        final Result imagesResult = runCommand(dockerAvailability.path, "images");

        if (imagesResult.isSuccess() == false) {
            dockerAvailability.lastCommand = versionResult;
            return dockerAvailability;
        }

        dockerAvailability.isAvailable = true;
        return dockerAvailability;
    }

    /**
     * This class represents the results of a Docker search from {@link #getDockerAvailability()}}.
     */
    public static class DockerAvailability {
        private boolean isAvailable = false;
        private boolean isVersionHighEnough = false;
        private String path = null;
        private String version = null;
        private Result lastCommand = null;

        /**
         * Indicates whether Docker is available and meets the required criteria.
         * @return true if, and only if, Docker is:
         * <ul>
         *     <li>Installed</li>
         *     <li>Executable</li>
         *     <li>Is at least version 17.05</li>
         *     <li>Can execute a command that requires privileges</li>
         * </ul>
         */
        public boolean isAvailable() {
            return isAvailable;
        }

        /**
         * @return true if the installed Docker version is &gt;= 17.05
         */
        public boolean isVersionHighEnough() {
            return isVersionHighEnough;
        }

        /**
         * @return the path to the Docker CLI, or null
         */
        public String getPath() {
            return path;
        }

        /**
         * @return The installed Docker version, or null
         */
        public String getVersion() {
            return version;
        }

        /**
         * @return Information about the last command executes while probing Docker, or null.
         */
        public Result getLastCommand() {
            return lastCommand;
        }
    }

    // package-private for testing
    /**
     * Given the string that <code>docker --version</code> prints, extract the docker version from it.
     * @param dockerVersion the version string to parse
     * @return the Docker version
     */
    static String extractVersion(String dockerVersion) {
        final Pattern dockerVersionPattern = Pattern.compile("Docker version (\\d+\\.\\d+\\.\\d+(?:-[a-zA-Z0-9]+)?), build [0-9a-f]{7,40}");

        final Matcher matcher = dockerVersionPattern.matcher(dockerVersion);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Can't extract version from: [" + dockerVersion + "]");
        }

        return matcher.group(1);
    }

    // package-private for testing
    /**
     * Checks whether the supplied Docker version is &gt;= 17.05
     * @param dockerVersion the version to check
     * @return true if the version is high enough
     */
    static boolean checkVersion(String dockerVersion) {
        final String[] majorMinor = dockerVersion.split("\\.");

        return (Integer.parseInt(majorMinor[0]) > 17
            || (Integer.parseInt(majorMinor[0]) == 17 && Integer.parseInt(majorMinor[1]) >= 5));
    }

    /**
     * Runs a command and captures the exit code, standard output and standard error.
     * @param args the command and any arguments to execute
     * @return a object that captures the result of running the command. If an exception occurring
     * while running the command, or the process was killed after reaching the 10s timeout,
     * then the exit code will be -1.
     */
    private static Result runCommand(String... args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("Cannot execute with no command");
        }

        final ProcessBuilder command = new ProcessBuilder().command(args);
        try {
            final Process process = command.start();

            if (process.waitFor(10, TimeUnit.SECONDS) == false) {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }

                Result result = new Result(-1,
                    IOUtils.toString(process.getInputStream()),
                    IOUtils.toString(process.getErrorStream()));

                throw new IllegalStateException(
                    "Timed out running shell command: " + command + "\n" +
                        "Result:\n" + result
                );
            }

            String stdout = IOUtils.toString(process.getInputStream());
            String stderr = IOUtils.toString(process.getErrorStream());

            return new Result(process.exitValue(), stdout, stderr);
        } catch (Exception e) {
            return new Result(-1, "", e.getMessage());
        }
    }

    /**
     * This class models the result of running a command. It captures the exit code, standard output and standard error.
     */
    public static class Result {
        public final int exitCode;
        public final String stdout;
        public final String stderr;

        Result(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        public boolean isSuccess() {
            return exitCode == 0;
        }

        public String toString() {
            return "exitCode = [" + exitCode + "] " + "stdout = [" + stdout.trim() + "] " + "stderr = [" + stderr.trim() + "]";
        }
    }
}
