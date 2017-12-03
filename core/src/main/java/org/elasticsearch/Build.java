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

package org.elasticsearch;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * Information about a build of Elasticsearch.
 */
public class Build {
    /**
     * The current build of Elasticsearch. Filled with information scanned at
     * startup from the jar.
     */
    public static final Build CURRENT;

    static {
        final String shortHash;
        final String date;
        final boolean isSnapshot;

        final String esPrefix = "elasticsearch-" + Version.CURRENT;
        final URL url = getElasticsearchCodeSourceLocation();
        final String urlStr = url == null ? "" : url.toString();
        if (urlStr.startsWith("file:/") && (urlStr.endsWith(esPrefix + ".jar") || urlStr.endsWith(esPrefix + "-SNAPSHOT.jar"))) {
            try (JarInputStream jar = new JarInputStream(FileSystemUtils.openFileURLStream(url))) {
                Manifest manifest = jar.getManifest();
                shortHash = manifest.getMainAttributes().getValue("Change");
                date = manifest.getMainAttributes().getValue("Build-Date");
                isSnapshot = "true".equals(manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Snapshot"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // not running from the official elasticsearch jar file (unit tests, IDE, uber client jar, shadiness)
            shortHash = "Unknown";
            date = "Unknown";
            final String buildSnapshot = System.getProperty("build.snapshot");
            if (buildSnapshot != null) {
                try {
                    Class.forName("com.carrotsearch.randomizedtesting.RandomizedContext");
                } catch (final ClassNotFoundException e) {
                    // we are not in tests but build.snapshot is set, bail hard
                    throw new IllegalStateException("build.snapshot set to [" + buildSnapshot + "] but not running tests");
                }
                isSnapshot = Booleans.parseBoolean(buildSnapshot);
            } else {
                isSnapshot = true;
            }
        }
        if (shortHash == null) {
            throw new IllegalStateException("Error finding the build shortHash. " +
                "Stopping Elasticsearch now so it doesn't run in subtly broken ways. This is likely a build bug.");
        }
        if (date == null) {
            throw new IllegalStateException("Error finding the build date. " +
                "Stopping Elasticsearch now so it doesn't run in subtly broken ways. This is likely a build bug.");
        }

        CURRENT = new Build(shortHash, date, isSnapshot);
    }

    private final boolean isSnapshot;

    /**
     * The location of the code source for Elasticsearch
     *
     * @return the location of the code source for Elasticsearch which may be null
     */
    static URL getElasticsearchCodeSourceLocation() {
        final CodeSource codeSource = Build.class.getProtectionDomain().getCodeSource();
        return codeSource == null ? null : codeSource.getLocation();
    }

    private final String shortHash;
    private final String date;

    public Build(String shortHash, String date, boolean isSnapshot) {
        this.shortHash = shortHash;
        this.date = date;
        this.isSnapshot = isSnapshot;
    }

    public String shortHash() {
        return shortHash;
    }

    public String date() {
        return date;
    }

    public static Build readBuild(StreamInput in) throws IOException {
        String hash = in.readString();
        String date = in.readString();
        boolean snapshot = in.readBoolean();
        return new Build(hash, date, snapshot);
    }

    public static void writeBuild(Build build, StreamOutput out) throws IOException {
        out.writeString(build.shortHash());
        out.writeString(build.date());
        out.writeBoolean(build.isSnapshot());
    }

    public boolean isSnapshot() {
        return isSnapshot;
    }

    @Override
    public String toString() {
        return "[" + shortHash + "][" + date + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Build build = (Build) o;

        if (isSnapshot != build.isSnapshot) {
            return false;
        }
        if (!shortHash.equals(build.shortHash)) {
            return false;
        }
        return date.equals(build.date);

    }

    @Override
    public int hashCode() {
        int result = (isSnapshot ? 1 : 0);
        result = 31 * result + shortHash.hashCode();
        result = 31 * result + date.hashCode();
        return result;
    }
}
