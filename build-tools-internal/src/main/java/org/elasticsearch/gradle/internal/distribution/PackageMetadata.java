/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.Architecture;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

/**
 * Describes a deb or rpm package of Elasticsearch: the naming of the projects and tasks producing it and the
 * metadata that differs between the package formats and architectures.
 *
 * @param type the package format
 * @param architecture the architecture the package is built for
 */
public record PackageMetadata(Type type, Architecture architecture) {

    /**
     * The name of the installed package.
     */
    public static final String PACKAGE_NAME = "elasticsearch";

    /**
     * The supported package formats.
     */
    public enum Type {
        DEB,
        RPM;

        /**
         * The lower case name of the format, which is also the file extension of the package.
         */
        public String extension() {
            return name().toLowerCase(Locale.ROOT);
        }

        /**
         * The name of the task preparing the files shared by all architectures of this package type,
         * e.g. {@code processDebFiles}.
         */
        public String processFilesTaskName() {
            return "process" + capitalize(extension()) + "Files";
        }

        /**
         * The environment file sourced by the service before starting Elasticsearch, which the package installs as a
         * configuration file.
         */
        public String envFile() {
            return switch (this) {
                case DEB -> "/etc/default/elasticsearch";
                case RPM -> "/etc/sysconfig/elasticsearch";
            };
        }

        /**
         * Parses the lower case name of a format, e.g. {@code deb}.
         */
        public static Type fromExtension(String extension) {
            return Arrays.stream(values())
                .filter(t -> t.extension().equals(extension))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown package type [" + extension + "]"));
        }
    }

    public PackageMetadata {
        Objects.requireNonNull(type);
        Objects.requireNonNull(architecture);
    }

    /**
     * Creates the package metadata from the string identifiers used in the build scripts.
     *
     * @param type {@code deb} or {@code rpm}
     * @param architecture the JDK architecture classifier, {@code x64} or {@code aarch64}
     */
    public static PackageMetadata of(String type, String architecture) {
        return new PackageMetadata(Type.fromExtension(type), fromJavaClassifier(architecture));
    }

    /**
     * Parses the name of a package distribution project, e.g. {@code aarch64-deb} or {@code rpm}.
     */
    public static PackageMetadata fromProjectName(String projectName) {
        return Arrays.stream(Type.values())
            .flatMap(type -> Arrays.stream(Architecture.values()).map(architecture -> new PackageMetadata(type, architecture)))
            .filter(metadata -> metadata.projectName().equals(projectName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown package project [" + projectName + "]"));
    }

    /**
     * The name of the project publishing this package, e.g. {@code aarch64-deb}.
     */
    public String projectName() {
        String architecturePrefix = switch (architecture) {
            case X64 -> "";
            case AARCH64 -> architecture.javaClassifier + "-";
        };
        return architecturePrefix + type.extension();
    }

    /**
     * The name of the task building this package, e.g. {@code buildAarch64Deb}.
     */
    public String buildTaskName() {
        String architectureName = switch (architecture) {
            case X64 -> "";
            case AARCH64 -> "Aarch64";
        };
        return "build" + architectureName + capitalize(type.extension());
    }

    /**
     * The architecture as named by the package format.
     */
    public String packageArchitecture() {
        return switch (type) {
            case DEB -> switch (architecture) {
                case X64 -> "amd64";
                case AARCH64 -> "arm64";
            };
            case RPM -> switch (architecture) {
                case X64 -> "X86_64";
                case AARCH64 -> "aarch64";
            };
        };
    }

    /**
     * The package version, which may not contain {@code -} in either format.
     */
    public String packageVersion(String version) {
        return switch (type) {
            case DEB -> version.replace('-', '~');
            case RPM -> version.replace('-', '_');
        };
    }

    /**
     * The license declared in the package metadata.
     */
    public String license() {
        return switch (type) {
            case DEB -> "Elastic-License";
            case RPM -> "Elastic License";
        };
    }

    private static Architecture fromJavaClassifier(String classifier) {
        return Arrays.stream(Architecture.values())
            .filter(a -> a.javaClassifier.equals(classifier))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown architecture [" + classifier + "]"));
    }

    private static String capitalize(String s) {
        return s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1);
    }
}
