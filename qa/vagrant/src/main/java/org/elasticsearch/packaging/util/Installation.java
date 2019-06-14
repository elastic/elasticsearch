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

    public Installation(Path home, Path config, Path data, Path logs, Path plugins, Path modules, Path pidDir, Path envFile) {
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

    public static Installation ofArchive(Path home) {
        return new Installation(
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

    public static Installation ofPackage(Distribution.Packaging packaging) {

        final Path envFile = (packaging == Distribution.Packaging.RPM)
            ? Paths.get("/etc/sysconfig/elasticsearch")
            : Paths.get("/etc/default/elasticsearch");

        return new Installation(
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

    public Path bin(String executableName) {
        return bin.resolve(executableName);
    }

    public Path config(String configFileName) {
        return config.resolve(configFileName);
    }

    public Executables executables() {
        return new Executables();
    }

    public class Executables {

        public final Path elasticsearch = platformExecutable("elasticsearch");
        public final Path elasticsearchPlugin = platformExecutable("elasticsearch-plugin");
        public final Path elasticsearchKeystore = platformExecutable("elasticsearch-keystore");
        public final Path elasticsearchCertutil = platformExecutable("elasticsearch-certutil");
        public final Path elasticsearchShard = platformExecutable("elasticsearch-shard");
        public final Path elasticsearchNode = platformExecutable("elasticsearch-node");
        public final Path elasticsearchSetupPasswords = platformExecutable("elasticsearch-setup-passwords");
        public final Path elasticsearchSyskeygen = platformExecutable("elasticsearch-syskeygen");
        public final Path elasticsearchUsers = platformExecutable("elasticsearch-users");

        private Path platformExecutable(String name) {
            final String platformExecutableName = Platforms.WINDOWS
                ? name + ".bat"
                : name;
            return bin(platformExecutableName);
        }
    }
}
