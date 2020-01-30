/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.packaging.util;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Represents an installation of Elasticsearch
 */
public class Installation {

    // in the future we'll run as a role user on Windows
    public static final String ARCHIVE_OWNER = Platforms.WINDOWS
        ? System.getenv("username")
        : "elasticsearch";

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

    private Installation(Shell sh, Distribution distribution, Path home, Path config, Path data, Path logs,
                         Path plugins, Path modules, Path pidDir, Path envFile) {
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
            null
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
            envFile
        );
    }

    public static Installation ofContainer(Shell sh, Distribution distribution) {
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
            null
        );
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

    public Executables executables() {
        return new Executables();
    }

    public class Executable {
        public final Path path;

        private Executable(String name) {
            final String platformExecutableName = Platforms.WINDOWS
                ? name + ".bat"
                : name;
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
            String command = path + " " + args;
            if (distribution.isArchive() && Platforms.WINDOWS == false) {
                command = "sudo -E -u " + ARCHIVE_OWNER + " " + command;
            }
            if (input != null) {
                command = "echo \"" + input + "\" | " + command;
            }
            return sh.run(command);
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
        public final Executable sqlCli = new Executable("elasticsearch-sql-cli");
        public final Executable syskeygenTool = new Executable("elasticsearch-syskeygen");
        public final Executable usersTool = new Executable("elasticsearch-users");
    }
}
