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
import java.util.Objects;
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

    public enum Flavor {

        DEFAULT("default"),
        OSS("oss"),
        UNKNOWN("unknown");

        final String displayName;

        Flavor(final String displayName) {
            this.displayName = displayName;
        }

        public String displayName() {
            return displayName;
        }

        public static Flavor fromDisplayName(final String displayName) {
            switch (displayName) {
                case "default":
                    return Flavor.DEFAULT;
                case "oss":
                    return Flavor.OSS;
                case "unknown":
                    return Flavor.UNKNOWN;
                default:
                    throw new IllegalStateException("unexpected distribution flavor [" + displayName + "]; your distribution is broken");
            }
        }

    }

    public enum Type {

        DEB("deb"),
        RPM("rpm"),
        TAR("tar"),
        ZIP("zip"),
        UNKNOWN("unknown");

        final String displayName;

        public String displayName() {
            return displayName;
        }

        Type(final String displayName) {
            this.displayName = displayName;
        }

        public static Type fromDisplayName(final String displayName) {
            switch (displayName) {
                case "deb":
                    return Type.DEB;
                case "rpm":
                    return Type.RPM;
                case "tar":
                    return Type.TAR;
                case "zip":
                    return Type.ZIP;
                case "unknown":
                    return Type.UNKNOWN;
                default:
                    throw new IllegalStateException("unexpected distribution type [" + displayName + "]; your distribution is broken");
            }
        }
    }

    static {
        final Flavor flavor;
        final Type type;
        final String shortHash;
        final String date;
        final boolean isSnapshot;
        final String version;

        flavor = Flavor.fromDisplayName(System.getProperty("es.distribution.flavor", "unknown"));
        type = Type.fromDisplayName(System.getProperty("es.distribution.type", "unknown"));

        final String esPrefix = "elasticsearch-" + Version.CURRENT;
        final URL url = getElasticsearchCodeSourceLocation();
        final String urlStr = url == null ? "" : url.toString();
        if (urlStr.startsWith("file:/") && (
            urlStr.endsWith(esPrefix + ".jar") ||
            urlStr.matches("(.*)" + esPrefix + "(-)?((alpha|beta|rc)[0-9]+)?(-SNAPSHOT)?.jar")
        )) {
            try (JarInputStream jar = new JarInputStream(FileSystemUtils.openFileURLStream(url))) {
                Manifest manifest = jar.getManifest();
                shortHash = manifest.getMainAttributes().getValue("Change");
                date = manifest.getMainAttributes().getValue("Build-Date");
                isSnapshot = "true".equals(manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Snapshot"));
                version = manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Version");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // not running from the official elasticsearch jar file (unit tests, IDE, uber client jar, shadiness)
            shortHash = "Unknown";
            date = "Unknown";
            version = Version.CURRENT.toString();
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
        if (version == null) {
            throw new IllegalStateException("Error finding the build version. " +
                "Stopping Elasticsearch now so it doesn't run in subtly broken ways. This is likely a build bug.");
        }

        CURRENT = new Build(flavor, type, shortHash, date, isSnapshot, version);
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

    private final Flavor flavor;
    private final Type type;
    private final String shortHash;
    private final String date;
    private final String version;

    public Build(
        final Flavor flavor, final Type type, final String shortHash, final String date, boolean isSnapshot,
        String version
    ) {
        this.flavor = flavor;
        this.type = type;
        this.shortHash = shortHash;
        this.date = date;
        this.isSnapshot = isSnapshot;
        this.version = version;
    }

    public String shortHash() {
        return shortHash;
    }

    public String date() {
        return date;
    }

    public static Build readBuild(StreamInput in) throws IOException {
        final Flavor flavor;
        final Type type;
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            flavor = Flavor.fromDisplayName(in.readString());
        } else {
            flavor = Flavor.OSS;
        }
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            type = Type.fromDisplayName(in.readString());
        } else {
            type = Type.UNKNOWN;
        }
        String hash = in.readString();
        String date = in.readString();
        boolean snapshot = in.readBoolean();

        final String version;
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            version = in.readString();
        } else {
            version = in.getVersion().toString();
        }
        return new Build(flavor, type, hash, date, snapshot, version);
    }

    public static void writeBuild(Build build, StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeString(build.flavor().displayName());
        }
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeString(build.type().displayName());
        }
        out.writeString(build.shortHash());
        out.writeString(build.date());
        out.writeBoolean(build.isSnapshot());
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeString(build.getQualifiedVersion());
        }
    }

    /**
     * Get the version as considered at build time
     *
     * Offers a way to get the fully qualified version as configured by the build.
     * This will be the same as {@link Version} for production releases, but may include on of the qualifier ( e.x alpha1 )
     * or -SNAPSHOT for others.
     *
     * @return the fully qualified build
     */
    public String getQualifiedVersion() {
        return version;
    }

    public Flavor flavor() {
        return flavor;
    }

    public Type type() {
        return type;
    }

    public boolean isSnapshot() {
        return isSnapshot;
    }

    /**
     * Provides information about the intent of the build
     *
     * @return true if the build is intended for production use
     */
    public boolean isProductionRelease() {
        return version.matches("[0-9]+\\.[0-9]+\\.[0-9]+");
    }

    @Override
    public String toString() {
        return "[" + flavor.displayName() + "][" + type.displayName + "][" + shortHash + "][" + date + "][" + version +"]";
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

        if (!flavor.equals(build.flavor)) {
            return false;
        }

        if (!type.equals(build.type)) {
            return false;
        }

        if (isSnapshot != build.isSnapshot) {
            return false;
        }
        if (!shortHash.equals(build.shortHash)) {
            return false;
        }
        if (version.equals(build.version) == false) {
            return false;
        }
        return date.equals(build.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flavor, type, isSnapshot, shortHash, date, version);
    }

}
