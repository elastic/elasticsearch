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

package org.elasticsearch.env;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;

import com.google.common.base.Charsets;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.common.Strings.cleanPath;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 * The environment of where things exists.
 */
public class Environment {

    private final Settings settings;

    private final Path homeFile;

    private final Path workFile;

    private final Path workWithClusterFile;

    private final Path[] dataFiles;

    private final Path[] dataWithClusterFiles;

    private final Path configFile;

    private final Path pluginsFile;

    private final Path logsFile;

    public Environment() {
        this(EMPTY_SETTINGS);
    }

    public Environment(Settings settings) {
        this.settings = settings;
        if (settings.get("path.home") != null) {
            homeFile = Paths.get(cleanPath(settings.get("path.home")));
        } else {
            homeFile = Paths.get(System.getProperty("user.dir"));
        }

        if (settings.get("path.conf") != null) {
            configFile = Paths.get(cleanPath(settings.get("path.conf")));
        } else {
            configFile = homeFile.resolve("config");
        }

        if (settings.get("path.plugins") != null) {
            pluginsFile = Paths.get(cleanPath(settings.get("path.plugins")));
        } else {
            pluginsFile = homeFile.resolve("plugins");
        }

        if (settings.get("path.work") != null) {
            workFile = Paths.get(cleanPath(settings.get("path.work")));
        } else {
            workFile = homeFile.resolve("work");
        }
        workWithClusterFile = workFile.resolve(ClusterName.clusterNameFromSettings(settings).value());

        String[] dataPaths = settings.getAsArray("path.data");
        if (dataPaths.length > 0) {
            dataFiles = new Path[dataPaths.length];
            dataWithClusterFiles = new Path[dataPaths.length];
            for (int i = 0; i < dataPaths.length; i++) {
                dataFiles[i] = Paths.get(dataPaths[i]);
                dataWithClusterFiles[i] = dataFiles[i].resolve(ClusterName.clusterNameFromSettings(settings).value());
            }
        } else {
            dataFiles = new Path[]{homeFile.resolve("data")};
            dataWithClusterFiles = new Path[]{homeFile.resolve("data").resolve(ClusterName.clusterNameFromSettings(settings).value())};
        }

        if (settings.get("path.logs") != null) {
            logsFile = Paths.get(cleanPath(settings.get("path.logs")));
        } else {
            logsFile = homeFile.resolve("logs");
        }
    }

    /**
     * The settings used to build this environment.
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * The home of the installation.
     */
    public Path homeFile() {
        return homeFile;
    }

    /**
     * The work location, path to temp files.
     *
     * Note, currently, we don't use it in ES at all, we should strive to see if we can keep it like that,
     * but if we do, we have the infra for it.
     */
    public Path workFile() {
        return workFile;
    }

    /**
     * The work location with the cluster name as a sub directory.
     *
     * Note, currently, we don't use it in ES at all, we should strive to see if we can keep it like that,
     * but if we do, we have the infra for it.
     */
    public Path workWithClusterFile() {
        return workWithClusterFile;
    }

    /**
     * The data location.
     */
    public Path[] dataFiles() {
        return dataFiles;
    }

    /**
     * The data location with the cluster name as a sub directory.
     */
    public Path[] dataWithClusterFiles() {
        return dataWithClusterFiles;
    }

    /**
     * The config location.
     */
    public Path configFile() {
        return configFile;
    }

    public Path pluginsFile() {
        return pluginsFile;
    }

    public Path logsFile() {
        return logsFile;
    }

    public String resolveConfigAndLoadToString(String path) throws FailedToResolveConfigException, IOException {
        return Streams.copyToString(Files.newBufferedReader(resolveConfig(path), Charsets.UTF_8));
    }

    public Path resolveConfig(String path) throws FailedToResolveConfigException {
        String origPath = path;
        // first, try it as a path on the file system
        Path f1 = Paths.get(path);
        if (Files.exists(f1)) {
            return f1;
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        // next, try it relative to the config location
        Path f2 = configFile.resolve(path);
        if (Files.exists(f2)) {
            return f2;
        }
        try {
            // try and load it from the classpath directly
            URL resource = settings.getClassLoader().getResource(path);
            if (resource != null) {
                return Paths.get(resource.toURI());
            }
            // try and load it from the classpath with config/ prefix
            if (!path.startsWith("config/")) {
                resource = settings.getClassLoader().getResource("config/" + path);
                if (resource != null) {
                    return Paths.get(resource.toURI());
                }
            }
        } catch (URISyntaxException ex) {
            throw new FailedToResolveConfigException("Failed to resolve config path [" + origPath + "], tried file path [" + f1 + "], path file [" + f2 + "], and classpath", ex);
        }
        throw new FailedToResolveConfigException("Failed to resolve config path [" + origPath + "], tried file path [" + f1 + "], path file [" + f2 + "], and classpath");
    }
}
