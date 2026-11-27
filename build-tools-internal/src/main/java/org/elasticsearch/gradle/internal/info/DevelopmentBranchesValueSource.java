/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.util.HttpUtils;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ValueSource;
import org.gradle.api.provider.ValueSourceParameters;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;

/**
 * Loads the development branches from {@code branches.json}, which by default is downloaded from the {@code main} branch on GitHub.
 * <p>
 * The remote file can change without any local change. When this value source is only queried at task execution time, Gradle
 * re-evaluates it on every build, including builds reusing the configuration cache. This allows tasks like {@code verifyVersions}
 * to always validate against the latest development branches.
 */
public abstract class DevelopmentBranchesValueSource
    implements
        ValueSource<List<DevelopmentBranch>, DevelopmentBranchesValueSource.Parameters> {

    private static final Logger LOGGER = Logging.getLogger(DevelopmentBranchesValueSource.class);

    static final String DEFAULT_BRANCHES_FILE_URL = "https://raw.githubusercontent.com/elastic/elasticsearch/main/branches.json";
    static final String BRANCHES_FILE_LOCATION_PROPERTY = "org.elasticsearch.build.branches-file-location";

    public interface Parameters extends ValueSourceParameters {
        /**
         * Location of the {@code branches.json} file, either an http(s) URL or a local file path.
         */
        Property<String> getLocation();

        /**
         * Local {@code branches.json} used instead of an http(s) {@link #getLocation()} when Gradle runs in offline mode.
         */
        Property<File> getOfflineFallbackFile();

        Property<Boolean> getOffline();
    }

    /**
     * Creates a provider for the development branches of the Elasticsearch workspace the given project belongs to.
     * The provider must only be queried at execution time to avoid making {@code branches.json} a configuration cache input.
     */
    public static Provider<List<DevelopmentBranch>> developmentBranches(Project project) {
        return project.getProviders().of(DevelopmentBranchesValueSource.class, spec -> {
            spec.getParameters()
                .getLocation()
                .set(project.getProviders().gradleProperty(BRANCHES_FILE_LOCATION_PROPERTY).orElse(DEFAULT_BRANCHES_FILE_URL));
            spec.getParameters()
                .getOfflineFallbackFile()
                .set(new File(Util.locateElasticsearchWorkspace(project.getGradle()), "branches.json"));
            spec.getParameters().getOffline().set(project.getGradle().getStartParameter().isOffline());
        });
    }

    @Override
    public List<DevelopmentBranch> obtain() {
        Parameters parameters = getParameters();
        return load(parameters.getLocation().get(), parameters.getOfflineFallbackFile().get(), parameters.getOffline().get());
    }

    /**
     * Loads the development branches eagerly. Used by {@link GlobalBuildInfoPlugin} at configuration time.
     */
    static List<DevelopmentBranch> load(String configuredBranchesFileLocation, File offlineFallbackFile, boolean offline) {
        String branchesFileLocation = configuredBranchesFileLocation;
        if (offline && isHttpLocation(configuredBranchesFileLocation)) {
            if (offlineFallbackFile.exists()) {
                LOGGER.warn(
                    "Gradle is running in offline mode; using local branches.json at [{}] instead of downloading from [{}].",
                    offlineFallbackFile,
                    configuredBranchesFileLocation
                );
                branchesFileLocation = offlineFallbackFile.getAbsolutePath();
            } else {
                throw new GradleException(
                    "Gradle is running in offline mode, but branches.json location ["
                        + configuredBranchesFileLocation
                        + "] is an http(s) URL and no local branches.json was found at ["
                        + offlineFallbackFile
                        + "]. Either disable offline mode, set -P"
                        + BRANCHES_FILE_LOCATION_PROPERTY
                        + "=<local file path>, or create branches.json at the workspace root."
                );
            }
        }
        LOGGER.info("Reading branches.json from {}", branchesFileLocation);
        byte[] branchesBytes;
        if (isHttpLocation(branchesFileLocation)) {
            try {
                branchesBytes = HttpUtils.readHttpBytesWithRetry(branchesFileLocation);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to download branches.json from: " + branchesFileLocation, e);
            }
        } else {
            try {
                branchesBytes = Files.readAllBytes(new File(branchesFileLocation).toPath());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read branches.json from: " + branchesFileLocation, e);
            }
        }

        var branchesFileParser = new BranchesFileParser(new ObjectMapper());
        return branchesFileParser.parse(branchesBytes);
    }

    private static boolean isHttpLocation(String branchesFileLocation) {
        return branchesFileLocation.startsWith("http://") || branchesFileLocation.startsWith("https://");
    }
}
