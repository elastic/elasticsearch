/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Booleans;

import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * Information about a build of Elasticsearch.
 */
public record Build(Type type, String hash, String date, boolean isSnapshot, String version) {

    private static final Build CURRENT;

    /**
     * The current build of Elasticsearch. Filled with information scanned at
     * startup from the jar.
     */
    public static Build current() {
        return CURRENT;
    }

    public enum Type {

        DEB("deb"),
        DOCKER("docker"),
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

        public static Type fromDisplayName(final String displayName, final boolean strict) {
            switch (displayName) {
                case "deb":
                    return Type.DEB;
                case "docker":
                    return Type.DOCKER;
                case "rpm":
                    return Type.RPM;
                case "tar":
                    return Type.TAR;
                case "zip":
                    return Type.ZIP;
                case "unknown":
                    return Type.UNKNOWN;
                default:
                    if (strict) {
                        throw new IllegalStateException("unexpected distribution type [" + displayName + "]; your distribution is broken");
                    } else {
                        return Type.UNKNOWN;
                    }
            }
        }

    }

    static {
        final Type type;
        final String hash;
        final String date;
        final boolean isSnapshot;
        final String version;

        // these are parsed at startup, and we require that we are able to recognize the values passed in by the startup scripts
        type = Type.fromDisplayName(System.getProperty("es.distribution.type", "unknown"), true);

        final String esPrefix = "elasticsearch-" + Version.CURRENT;
        final URL url = getElasticsearchCodeSourceLocation();
        final String urlStr = url == null ? "" : url.toString();
        if (urlStr.startsWith("file:/")
            && (urlStr.endsWith(esPrefix + ".jar") || urlStr.matches("(.*)" + esPrefix + "(-)?((alpha|beta|rc)[0-9]+)?(-SNAPSHOT)?.jar"))) {
            try (JarInputStream jar = new JarInputStream(FileSystemUtils.openFileURLStream(url))) {
                Manifest manifest = jar.getManifest();
                hash = manifest.getMainAttributes().getValue("Change");
                date = manifest.getMainAttributes().getValue("Build-Date");
                isSnapshot = "true".equals(manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Snapshot"));
                version = manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Version");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // not running from the official elasticsearch jar file (unit tests, IDE, uber client jar, shadiness)
            hash = "unknown";
            date = "unknown";
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
        if (hash == null) {
            throw new IllegalStateException(
                "Error finding the build hash. "
                    + "Stopping Elasticsearch now so it doesn't run in subtly broken ways. This is likely a build bug."
            );
        }
        if (date == null) {
            throw new IllegalStateException(
                "Error finding the build date. "
                    + "Stopping Elasticsearch now so it doesn't run in subtly broken ways. This is likely a build bug."
            );
        }
        if (version == null) {
            throw new IllegalStateException(
                "Error finding the build version. "
                    + "Stopping Elasticsearch now so it doesn't run in subtly broken ways. This is likely a build bug."
            );
        }

        CURRENT = new Build(type, hash, date, isSnapshot, version);
    }

    /**
     * The location of the code source for Elasticsearch
     *
     * @return the location of the code source for Elasticsearch which may be null
     */
    static URL getElasticsearchCodeSourceLocation() {
        final CodeSource codeSource = Build.class.getProtectionDomain().getCodeSource();
        return codeSource == null ? null : codeSource.getLocation();
    }

    public static Build readBuild(StreamInput in) throws IOException {
        final Type type;
        // be lenient when reading on the wire, the enumeration values from other versions might be different than what we know
        if (in.getTransportVersion().before(TransportVersion.V_8_3_0)) {
            // this was the flavor, which is always the default distribution now
            in.readString();
        }
        // be lenient when reading on the wire, the enumeration values from other versions might be different than what we know
        type = Type.fromDisplayName(in.readString(), false);
        String hash = in.readString();
        String date = in.readString();
        boolean snapshot = in.readBoolean();

        final String version;
        version = in.readString();
        return new Build(type, hash, date, snapshot, version);
    }

    public static void writeBuild(Build build, StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersion.V_8_3_0)) {
            // this was the flavor, which is always the default distribution now
            out.writeString("default");
        }
        out.writeString(build.type().displayName());
        out.writeString(build.hash());
        out.writeString(build.date());
        out.writeBoolean(build.isSnapshot());
        out.writeString(build.qualifiedVersion());
    }

    /**
     * Get the version as considered at build time
     * <p>
     * Offers a way to get the fully qualified version as configured by the build.
     * This will be the same as {@link Version} for production releases, but may include on of the qualifier ( e.x alpha1 )
     * or -SNAPSHOT for others.
     *
     * @return the fully qualified build
     */
    public String qualifiedVersion() {
        return version;
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
        return "[" + type.displayName + "][" + hash + "][" + date + "][" + version + "]";
    }
}
