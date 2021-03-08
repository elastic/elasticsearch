/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import java.nio.file.Path;
import java.util.Locale;

public class Distribution {

    public final Path path;
    public final Packaging packaging;
    public final Platform platform;
    public final Flavor flavor;
    public final boolean hasJdk;
    public final String version;

    public Distribution(Path path) {
        this.path = path;
        String filename = path.getFileName().toString();

        if (filename.endsWith(".gz")) {
            this.packaging = Packaging.TAR;
        } else if (filename.endsWith(".docker.tar")) {
            this.packaging = filename.contains("-from-context") ? Packaging.DOCKER_FROM_CONTEXT : Packaging.DOCKER;
        } else if (filename.endsWith(".ubi.tar")) {
            this.packaging = filename.contains("-from-context") ? Packaging.DOCKER_UBI_FROM_CONTEXT : Packaging.DOCKER_UBI;
        } else if (filename.endsWith(".ironbank.tar")) {
            this.packaging = Packaging.DOCKER_IRON_BANK_FROM_CONTEXT;
        } else {
            int lastDot = filename.lastIndexOf('.');
            this.packaging = Packaging.valueOf(filename.substring(lastDot + 1).toUpperCase(Locale.ROOT));
        }

        this.platform = filename.contains("windows") ? Platform.WINDOWS : Platform.LINUX;
        this.flavor = filename.contains("oss") ? Flavor.OSS : Flavor.DEFAULT;
        this.hasJdk = filename.contains("no-jdk") == false;
        String version = filename.split("-", 3)[1];
        if (filename.contains("-SNAPSHOT")) {
            version += "-SNAPSHOT";
        }
        this.version = version;
    }

    public boolean isDefault() {
        return flavor.equals(Flavor.DEFAULT);
    }

    public boolean isOSS() {
        return flavor.equals(Flavor.OSS);
    }

    public boolean isArchive() {
        return packaging == Packaging.TAR || packaging == Packaging.ZIP;
    }

    public boolean isPackage() {
        return packaging == Packaging.RPM || packaging == Packaging.DEB;
    }

    /**
     * @return whether this distribution is packaged as a Docker image.
     */
    public boolean isDocker() {
        switch (packaging) {
            case DOCKER:
            case DOCKER_FROM_CONTEXT:
            case DOCKER_UBI:
            case DOCKER_UBI_FROM_CONTEXT:
            case DOCKER_IRON_BANK_FROM_CONTEXT:
                return true;
        }
        return false;
    }

    public enum Packaging {

        TAR(".tar.gz", Platforms.LINUX || Platforms.DARWIN),
        ZIP(".zip", Platforms.WINDOWS),
        DEB(".deb", Platforms.isDPKG()),
        RPM(".rpm", Platforms.isRPM()),
        DOCKER(".docker.tar", Platforms.isDocker()),
        DOCKER_FROM_CONTEXT(".docker.tar", Platforms.isDocker()),
        DOCKER_UBI(".ubi.tar", Platforms.isDocker()),
        DOCKER_UBI_FROM_CONTEXT(".ubi.tar", Platforms.isDocker()),
        DOCKER_IRON_BANK_FROM_CONTEXT(".ironbank.tar", Platforms.isDocker());

        /** The extension of this distribution's file */
        public final String extension;

        /** Whether the distribution is intended for use on the platform the current JVM is running on */
        public final boolean compatible;

        Packaging(String extension, boolean compatible) {
            this.extension = extension;
            this.compatible = compatible;
        }
    }

    public enum Platform {
        LINUX,
        WINDOWS,
        DARWIN;

        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public enum Flavor {

        OSS("elasticsearch-oss"),
        DEFAULT("elasticsearch");

        public final String name;

        Flavor(String name) {
            this.name = name;
        }
    }
}
