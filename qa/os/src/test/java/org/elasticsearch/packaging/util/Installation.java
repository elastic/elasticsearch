/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import org.elasticsearch.core.Nullable;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Represents an installation of Elasticsearch
 */
public class Installation {

    // in the future we'll run as a role user on Windows
    public static final String ARCHIVE_OWNER = Platforms.WINDOWS ? System.getenv("username") : "elasticsearch";

    private final Shell sh;
    public final Distribution distribution;
    public final Path home;
    public final Path bin; // this isn't a first-class installation feature but we include it for convenience
    public final Path lib; // same
    public final Path bundledJdk;
    public final Path config;
    public final Path data;
    public final Path logs;
    public final Path plugins;
    public final Path modules;
    public final Path pidDir;
    public final Path envFile;
    @Nullable
    private String elasticPassword; // auto-configured password upon installation
    public final int port;

    private Installation(
        Shell sh,
        Distribution distribution,
        Path home,
        Path config,
        Path data,
        Path logs,
        Path plugins,
        Path modules,
        Path pidDir,
        Path envFile,
        int port
    ) {
        this.sh = sh;
        this.distribution = distribution;
        this.home = home;
        this.bin = home.resolve("bin");
        this.lib = home.resolve("lib");
        this.bundledJdk = home.resolve("jdk");
        this.config = config;
        this.data = data;
        this.logs = logs;
        this.plugins = plugins;
        this.modules = modules;
        this.pidDir = pidDir;
        this.envFile = envFile;
        this.port = port;
        this.elasticPassword = null;
    }

    public static Installation ofArchive(Shell sh, Distribution distribution, Path home) {
        return new Installation(
            sh,
            distribution,
            home,
            home.resolve("config"),
            home.resolve("data"),
            home.resolve("logs"),
            home.resolve("plugins"),
            home.resolve("modules"),
            null,
            null,
            9200
        );
    }

    public static Installation ofPackage(Shell sh, Distribution distribution) {

        final Path envFile = (distribution.packaging == Distribution.Packaging.RPM)
            ? Paths.get("/etc/sysconfig/elasticsearch")
            : Paths.get("/etc/default/elasticsearch");

        return new Installation(
            sh,
            distribution,
            Paths.get("/usr/share/elasticsearch"),
            Paths.get("/etc/elasticsearch"),
            Paths.get("/var/lib/elasticsearch"),
            Paths.get("/var/log/elasticsearch"),
            Paths.get("/usr/share/elasticsearch/plugins"),
            Paths.get("/usr/share/elasticsearch/modules"),
            Paths.get("/var/run/elasticsearch"),
            envFile,
            9200
        );
    }

    public static Installation ofContainer(Shell sh, Distribution distribution, int port) {
        String root = "/usr/share/elasticsearch";
        return new Installation(
            sh,
            distribution,
            Paths.get(root),
            Paths.get(root + "/config"),
            Paths.get(root + "/data"),
            Paths.get(root + "/logs"),
            Paths.get(root + "/plugins"),
            Paths.get(root + "/modules"),
            null,
            null,
            port
        );
    }

    public static Installation ofContainer(Shell sh, Distribution distribution) {
        return ofContainer(sh, distribution, 9200);
    }

    /**
     * Returns the user that owns this installation.
     *
     * For packages this is root, and for archives it is the user doing the installation.
     */
    public String getOwner() {
        if (Platforms.WINDOWS) {
            // windows is always administrator, since there is no sudo
            return "BUILTIN\\Administrators";
        }
        return distribution.isArchive() ? ARCHIVE_OWNER : "root";
    }

    public Path bin(String executableName) {
        return bin.resolve(executableName);
    }

    public Path config(String configFileName) {
        return config.resolve(configFileName);
    }

    public Path config(Path configFileName) {
        return config.resolve(configFileName);
    }

    public String getElasticPassword() {
        return this.elasticPassword;
    }

    public void setElasticPassword(String password) {
        this.elasticPassword = password;
    }

    public Executables executables() {
        return new Executables();
    }

    public class Executable {
        public final Path path;

        private Executable(String name) {
            final String platformExecutableName = Platforms.WINDOWS ? name + ".bat" : name;
            this.path = bin(platformExecutableName);
        }

        @Override
        public String toString() {
            return path.toString();
        }

        public Shell.Result run(String args) {
            return run(args, null);
        }

        public Shell.Result run(String args, String input) {
            return run(args, input, false);
        }

        public Shell.Result run(String args, String input, boolean ignoreExitCode) {
            String command = path.toString();
            if (Platforms.WINDOWS) {
                command = "& '" + command + "'";
            } else {
                command = "\"" + command + "\"";
                if (distribution.isArchive()) {
                    command = "sudo -E -u " + ARCHIVE_OWNER + " " + command;
                }
            }

            if (input != null) {
                command = "echo \"" + input + "\" | " + command;
            }
            if (ignoreExitCode) {
                return sh.runIgnoreExitCode(command + " " + args);
            }
            return sh.run(command + " " + args);
        }
    }

    public class Executables {

        public final Executable elasticsearch = new Executable("elasticsearch");
        public final Executable pluginTool = new Executable("elasticsearch-plugin");
        public final Executable keystoreTool = new Executable("elasticsearch-keystore");
        public final Executable certutilTool = new Executable("elasticsearch-certutil");
        public final Executable certgenTool = new Executable("elasticsearch-certgen");
        public final Executable cronevalTool = new Executable("elasticsearch-croneval");
        public final Executable shardTool = new Executable("elasticsearch-shard");
        public final Executable nodeTool = new Executable("elasticsearch-node");
        public final Executable setupPasswordsTool = new Executable("elasticsearch-setup-passwords");
        public final Executable resetPasswordTool = new Executable("elasticsearch-reset-password");
        public final Executable createEnrollmentToken = new Executable("elasticsearch-create-enrollment-token");
        public final Executable nodeReconfigureTool = new Executable("elasticsearch-reconfigure-node");
        public final Executable sqlCli = new Executable("elasticsearch-sql-cli");
        public final Executable syskeygenTool = new Executable("elasticsearch-syskeygen");
        public final Executable usersTool = new Executable("elasticsearch-users");
        public final Executable serviceTokensTool = new Executable("elasticsearch-service-tokens");
    }
}
