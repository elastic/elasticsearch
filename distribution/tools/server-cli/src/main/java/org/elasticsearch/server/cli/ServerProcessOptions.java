/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds options and arguments needed to start a server process
 */
public class ServerProcessOptions {
    private final String command;
    private final List<String> jvmOptions;
    private final List<String> otherOptions;
    private final Map<String, String> environment;
    private final ServerArgs serverArgs;
    private final ServerProcess.ProcessStarter processStarter;

    private ServerProcessOptions(
        String command,
        List<String> jvmOptions,
        List<String> otherOptions,
        Map<String, String> environment,
        ServerArgs serverArgs,
        ServerProcess.ProcessStarter processStarter
    ) {
        this.command = command;
        this.jvmOptions = jvmOptions;
        this.otherOptions = otherOptions;
        this.environment = environment;
        this.serverArgs = serverArgs;
        this.processStarter = processStarter;
    }

    String getCommand() {
        return command;
    }

    List<String> getJvmOptions() {
        return jvmOptions;
    }

    List<String> getOtherOptions() {
        return otherOptions;
    }

    Map<String, String> getEnvironment() {
        return environment;
    }

    ServerArgs getServerArgs() {
        return serverArgs;
    }

    ServerProcess.ProcessStarter getProcessStarter() {
        return processStarter;
    }

    /**
     * A Builder to assemble, check and create options and arguments needed to start a server process
     */
    public static class Builder {
        private final ProcessInfo processInfo;
        private final ServerArgs serverArgs;

        private ServerProcess.OptionsBuilder optionsBuilder = JvmOptionsParser::determineJvmOptions;
        private ServerProcess.ProcessStarter processStarter = ProcessBuilder::start;

        private Builder(ProcessInfo processInfo, ServerArgs serverArgs) {
            this.processInfo = processInfo;
            this.serverArgs = serverArgs;
        }

        Builder withOptionsBuilder(ServerProcess.OptionsBuilder optionsBuilder) {
            this.optionsBuilder = optionsBuilder;
            return this;
        }

        Builder withProcessStarter(ServerProcess.ProcessStarter processStarter) {
            this.processStarter = processStarter;
            return this;
        }

        /**
         * Builds the ServerProcessOptions by using the provided arguments, process info, environment and options builder.
         * The environment and temp directories are checked and, if needed, adjusted/created to be compliant with what the
         * server process is expecting.
         *
         * @return ServerProcessOptions
         * @throws UserException    if there is a problem with the configuration (e.g. files or directory permissions)
         */
        public ServerProcessOptions build() throws UserException {
            try {
                Map<String, String> envVars = new HashMap<>(processInfo.envVars());

                Path tempDir = setupTempDir(processInfo, envVars.remove("ES_TMPDIR"));
                if (envVars.containsKey("LIBFFI_TMPDIR") == false) {
                    envVars.put("LIBFFI_TMPDIR", tempDir.toString());
                }

                List<String> jvmOptions = optionsBuilder.getJvmOptions(
                    serverArgs,
                    serverArgs.configDir(),
                    tempDir,
                    envVars.remove("ES_JAVA_OPTS")
                );
                // also pass through distribution type
                jvmOptions.add("-Des.distribution.type=" + processInfo.sysprops().get("es.distribution.type"));

                Path esHome = processInfo.workingDir();
                Path javaHome = PathUtils.get(processInfo.sysprops().get("java.home"));

                boolean isWindows = processInfo.sysprops().get("os.name").startsWith("Windows");
                String command = javaHome.resolve("bin").resolve("java" + (isWindows ? ".exe" : "")).toString();

                List<String> otherOptions = new ArrayList<>();
                otherOptions.add("--module-path");
                otherOptions.add(esHome.resolve("lib").toString());
                // Special circumstances require some modules (not depended on by the main server module) to be explicitly added:
                otherOptions.add("--add-modules=jdk.net"); // needed to reflectively set extended socket options
                // we control the module path, which may have additional modules not required by server
                otherOptions.add("--add-modules=ALL-MODULE-PATH");
                otherOptions.add("-m");
                otherOptions.add("org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch");

                return new ServerProcessOptions(command, jvmOptions, otherOptions, envVars, serverArgs, processStarter);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Returns the java.io.tmpdir Elasticsearch should use, creating it if necessary.
         *
         * <p> On non-Windows OS, this will be created as a subdirectory of the default temporary directory.
         * Note that this causes the created temporary directory to be a private temporary directory.
         */
        private static Path setupTempDir(ProcessInfo processInfo, String tmpDirOverride) throws UserException, IOException {
            final Path path;
            if (tmpDirOverride != null) {
                path = Paths.get(tmpDirOverride);
                if (Files.exists(path) == false) {
                    throw new UserException(ExitCodes.CONFIG, "Temporary directory [" + path + "] does not exist or is not accessible");
                }
                if (Files.isDirectory(path) == false) {
                    throw new UserException(ExitCodes.CONFIG, "Temporary directory [" + path + "] is not a directory");
                }
            } else {
                if (processInfo.sysprops().get("os.name").startsWith("Windows")) {
                    /*
                     * On Windows, we avoid creating a unique temporary directory per invocation lest
                     * we pollute the temporary directory. On other operating systems, temporary directories
                     * will be cleaned automatically via various mechanisms (e.g., systemd, or restarts).
                     */
                    path = Paths.get(processInfo.sysprops().get("java.io.tmpdir"), "elasticsearch");
                    Files.createDirectories(path);
                } else {
                    path = createTempDirectory("elasticsearch-");
                }
            }
            return path;
        }

        @SuppressForbidden(reason = "Files#createTempDirectory(String, FileAttribute...)")
        private static Path createTempDirectory(final String prefix, final FileAttribute<?>... attrs) throws IOException {
            return Files.createTempDirectory(prefix, attrs);
        }
    }

    /**
     * Returns a Builder instance
     *
     * @param processInfo     Info about the current process, for passing through to the subprocess.
     * @param serverArgs      Arguments to the server process.
     * @return A builder to construct a ServerProcessOptions object
     */
    public static ServerProcessOptions.Builder builder(ProcessInfo processInfo, ServerArgs serverArgs) {
        return new Builder(processInfo, serverArgs);
    }
}
