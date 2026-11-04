/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.internal.DockerBase;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A Docker image of Elasticsearch that the build produces, identified by the architecture it is built for and the
 * base image it is built on. Owns the naming conventions (tasks, images, tags, context archives and exported image
 * files) so that all places referring to the same image agree on them.
 *
 * @param architecture the architecture the image is built for
 * @param base the base image the image is built on
 */
public record DockerVariant(Architecture architecture, DockerBase base) {

    private static final String IMAGE_NAME = "elasticsearch";
    private static final String EXPORT_PROJECT_SUFFIX = "-export";

    public DockerVariant {
        Objects.requireNonNull(architecture);
        Objects.requireNonNull(base);
    }

    /**
     * All variants, ordered by architecture and then base.
     */
    public static List<DockerVariant> all() {
        List<DockerVariant> variants = new ArrayList<>();
        for (Architecture architecture : Architecture.values()) {
            for (DockerBase base : DockerBase.values()) {
                variants.add(new DockerVariant(architecture, base));
            }
        }
        return Collections.unmodifiableList(variants);
    }

    /**
     * A camel case name qualified with this variant, as used for the tasks and configurations operating on it, e.g.
     * {@code buildAarch64IronBankDockerImage} for prefix {@code build} and suffix {@code DockerImage}.
     */
    public String camelName(String prefix, String suffix) {
        String architectureName = switch (architecture) {
            case X64 -> "";
            case AARCH64 -> "Aarch64";
        };
        return prefix + architectureName + (base == DockerBase.DEFAULT ? "" : toCamel(base.name())) + suffix;
    }

    /**
     * The architecture suffix used in file names, {@code -aarch64} or empty for x64.
     */
    public String architectureSuffix() {
        return switch (architecture) {
            case X64 -> "";
            case AARCH64 -> "-aarch64";
        };
    }

    /**
     * The lower case name of the base, e.g. {@code cloud_ess}, as expanded into {@code docker_base} in the
     * Dockerfile templates.
     */
    public String dockerBaseName() {
        return base.name().toLowerCase(Locale.ROOT);
    }

    /**
     * The repository name of the image, e.g. {@code elasticsearch-wolfi}.
     */
    public String imageName() {
        return IMAGE_NAME + base.getSuffix();
    }

    /**
     * The architecture specific tag the image is always tagged with locally, e.g. {@code elasticsearch-wolfi:aarch64}.
     */
    public String localImageReference() {
        return imageName() + ":" + architecture.classifier;
    }

    /**
     * The tags applied to the built image. Images built for the given current architecture are additionally tagged
     * as {@code test}, with the version and with their fully qualified name.
     */
    public List<String> tags(String version, Architecture currentArchitecture) {
        String namespace = base == DockerBase.CLOUD_ESS ? "elasticsearch-ci" : "elasticsearch";
        List<String> tags = new ArrayList<>();
        tags.add(localImageReference());
        if (architecture == currentArchitecture) {
            tags.add(imageName() + ":test");
            tags.add(imageName() + ":" + version);
            tags.add("docker.elastic.co/" + namespace + "/" + imageName() + ":" + version);
        }
        return Collections.unmodifiableList(tags);
    }

    /**
     * The name of the Docker build context archive (without extension) and of the directory it is transformed into.
     */
    public String buildContextName(String version) {
        return imageName() + "-" + version + "-docker-build-context" + architectureSuffix();
    }

    /**
     * The directory within the build context the configuration files are placed in.
     */
    public String configDirectory() {
        return base == DockerBase.IRON_BANK ? "scripts" : "config";
    }

    /**
     * The directory within the build context the scripts are placed in.
     */
    public String binDirectory() {
        return base == DockerBase.IRON_BANK ? "scripts" : "bin";
    }

    /**
     * The build date to embed into the image. Outside of CI the build time is truncated to midnight so that the
     * Docker build cache is usable while developing the images.
     */
    public static String buildDate(ZonedDateTime buildDate, boolean ci) {
        return ci ? buildDate.toString() : buildDate.truncatedTo(ChronoUnit.DAYS).toString();
    }

    /**
     * The values expanded into the Dockerfile templates of this variant.
     *
     * @param version the Elasticsearch version
     * @param buildDate the build date, see {@link #buildDate(ZonedDateTime, boolean)}
     * @param gitRevision the git revision the image is built from
     * @param bcFipsVersion the version of the bundled BouncyCastle FIPS provider
     * @param publicationContext the publication context the image is built for, may be empty
     */
    public Map<String, Object> templateExpansions(
        String version,
        String buildDate,
        String gitRevision,
        String bcFipsVersion,
        String publicationContext
    ) {
        String[] versionParts = version.split("\\.");
        // LinkedHashMap rather than Map.of(): the base image is null for bases that are built programmatically
        Map<String, Object> expansions = new LinkedHashMap<>();
        expansions.put("arch", architecture.classifier);
        expansions.put("base_image", base.getImage());
        expansions.put("bin_dir", binDirectory());
        expansions.put("build_date", buildDate);
        expansions.put("config_dir", configDirectory());
        expansions.put("git_revision", gitRevision);
        expansions.put("license", base == DockerBase.IRON_BANK ? "Elastic License 2.0" : "Elastic-License-2.0");
        expansions.put("package_manager", base.getPackageManager());
        expansions.put("docker_base", dockerBaseName());
        expansions.put("docker_context", publicationContext);
        expansions.put("version", version);
        expansions.put("major_minor_version", versionParts[0] + "." + versionParts[1]);
        expansions.put("retry", ShellRetry.class);
        expansions.put("bc_fips_version", bcFipsVersion);
        return Collections.unmodifiableMap(expansions);
    }

    /**
     * Parses the name of a Docker export project, e.g. {@code ironbank-docker-aarch64-export}.
     */
    public static DockerVariant fromExportProjectName(String projectName) {
        return all().stream()
            .filter(v -> v.exportProjectName().equals(projectName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown docker export project [" + projectName + "]"));
    }

    /**
     * The name of the project exporting this variant's image, e.g. {@code ironbank-docker-aarch64-export}.
     */
    public String exportProjectName() {
        String basePrefix = base == DockerBase.DEFAULT ? "" : exportProjectBaseName(base) + "-";
        return basePrefix + "docker" + architectureSuffix() + EXPORT_PROJECT_SUFFIX;
    }

    /**
     * The name of the exported image artifact, e.g. {@code elasticsearch-aarch64-ironbank_test}.
     */
    public String exportArtifactName() {
        return IMAGE_NAME + architectureSuffix() + base.getSuffix() + "_test";
    }

    /**
     * The extension of the exported image file, e.g. {@code ironbank.tar}.
     */
    public String exportExtension() {
        return switch (base) {
            case DEFAULT -> "docker.tar";
            case IRON_BANK -> "ironbank.tar";
            case WOLFI -> "wolfi.tar";
            case CLOUD_ESS -> "cloud-ess.tar";
            case CLOUD_ESS_FIPS -> "cloud-ess-fips.tar";
        };
    }

    /**
     * The file name of the exported image, e.g. {@code elasticsearch-aarch64-ironbank_test_9.3.1.ironbank.tar}.
     */
    public String exportFileName(String version) {
        return exportArtifactName() + "_" + version + "." + exportExtension();
    }

    /**
     * The base name of the compressed exported image archive, e.g. {@code elasticsearch-ironbank-9.3.1-docker-image}.
     */
    public String exportArchiveBaseName(String version) {
        return imageName() + "-" + version + "-docker-image";
    }

    private static String exportProjectBaseName(DockerBase base) {
        return switch (base) {
            case DEFAULT -> "";
            case IRON_BANK -> "ironbank";
            case WOLFI -> "wolfi";
            case CLOUD_ESS -> "cloud-ess";
            case CLOUD_ESS_FIPS -> "cloud-ess-fips";
        };
    }

    private static String toCamel(String input) {
        return Arrays.stream(input.split("[^a-zA-Z0-9]"))
            .map(s -> s.substring(0, 1) + s.substring(1).toLowerCase(Locale.ROOT))
            .collect(Collectors.joining());
    }
}
