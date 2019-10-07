package org.elasticsearch.gradle;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Docker {
    public static String[] DOCKER_BINARIES = { "/usr/bin/docker", "/usr/local/bin/docker" };

    public static class DockerAvailability {
        private boolean isAvailable = false;
        private boolean isVersionHighEnough = false;
        private String path = null;
        private String version = null;
        private Result lastCommand = null;

        public boolean isAvailable() {
            return isAvailable;
        }

        public boolean isVersionHighEnough() {
            return isVersionHighEnough;
        }

        public String getPath() {
            return path;
        }

        public String getVersion() {
            return version;
        }

        public Result getLastCommand() {
            return lastCommand;
        }
    }

    public static Optional<String> getDockerPath() {
        // Check if the Docker binary exists
        return List.of(DOCKER_BINARIES)
            .stream()
            .filter(path -> new File(path).exists())
            .findFirst();
    }

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

    // package-private for testing
    static String extractVersion(String dockerVersion) {
        final Pattern dockerVersionPattern = Pattern.compile("Docker version (\\d+\\.\\d+\\.\\d+(?:-[a-zA-Z0-9]+)?), build [0-9a-f]{7,40}");

        final Matcher matcher = dockerVersionPattern.matcher(dockerVersion);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Can't extract version from: [" + dockerVersion + "]");
        }

        return matcher.group(1);
    }

    // package-private for testing
    static boolean checkVersion(String dockerVersion) {
        final String[] majorMinor = dockerVersion.split("\\.");

        return (Integer.parseInt(majorMinor[0]) > 17
            || (Integer.parseInt(majorMinor[0]) == 17 && Integer.parseInt(majorMinor[1]) >= 5));
    }

    private static Result runCommand(String... args) {
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

    public static class Result {
        public final int exitCode;
        public final String stdout;
        public final String stderr;

        public Result(int exitCode, String stdout, String stderr) {
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
