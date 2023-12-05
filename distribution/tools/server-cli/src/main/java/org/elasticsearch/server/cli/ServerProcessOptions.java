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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ServerProcessOptions {
    private final String command;
    private final List<String> jvmOptions;
    private final List<String> otherOptions;
    private final Map<String, String> environment;
    private final ServerArgs serverArgs;

    private ServerProcessOptions(
        String command,
        List<String> jvmOptions,
        List<String> otherOptions,
        Map<String, String> environment,
        ServerArgs serverArgs
    ) {
        this.command = command;
        this.jvmOptions = jvmOptions;
        this.otherOptions = otherOptions;
        this.environment = environment;
        this.serverArgs = serverArgs;
    }

    public String getCommand() {
        return command;
    }

    public List<String> getJvmOptions() {
        return jvmOptions;
    }

    public List<String> getOtherOptions() {
        return otherOptions;
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public ServerArgs getServerArgs() {
        return serverArgs;
    }

    public static ServerProcessOptions create(ProcessInfo processInfo, ServerProcess.OptionsBuilder optionsBuilder, ServerArgs args)
        throws IOException, UserException, InterruptedException {
        Map<String, String> envVars = new HashMap<>(processInfo.envVars());

        Path tempDir = setupTempDir(processInfo, envVars.remove("ES_TMPDIR"));
        if (envVars.containsKey("LIBFFI_TMPDIR") == false) {
            envVars.put("LIBFFI_TMPDIR", tempDir.toString());
        }

        List<String> jvmOptions = optionsBuilder.getJvmOptions(args, args.configDir(), tempDir, envVars.remove("ES_JAVA_OPTS"));
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

        return new ServerProcessOptions(command, jvmOptions, otherOptions, envVars, args);
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
