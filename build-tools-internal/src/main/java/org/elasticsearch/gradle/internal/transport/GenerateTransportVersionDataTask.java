/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import com.google.common.collect.Streams;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.transport.TransportVersionUtils.TransportVersionSetData;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.LATEST_SUFFIX;
import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.getTVSetDataFilePath;

/**
 * This task generates TransportVersionSetData data files that contain information about transport versions. These files
 * are added to the server project's resource directory at `server/src/main/resources/org/elasticsearch/transport/`.
 * They have the following format:
 * <pre>
 * Filename: my-transport-version-set.json  // Must be the same as the name of the transport version set.
 * {
 *   "name": "my-transport-version-set", // The name of the transport version set used for reference in the code.
 *   "ids": [
 *     9109000,  // The transport version introduced to the main branch.
 *     8841059   // The transport version backported to a previous release branch.
 *   ]
 * }
 * </pre>
 */
public abstract class GenerateTransportVersionDataTask extends DefaultTask {

    /**
     * Specifies the directory in which contains all TransportVersionSet data files.
     *
     * @return
     */
    @InputDirectory
    public abstract RegularFileProperty getDataFileDirectory();

    /**
     * Used to set the name of the TransportVersionSet for which a data file will be generated.
     */
    @Input
    public abstract Property<String> getTVSetName();

    /**
     * Used to set the `major.minor` release version for which the specific TransportVersion ID will be generated.
     * E.g.: "9.2", "8.18", etc.
     */
    @Input
    public abstract Property<String> getReleaseVersionForTV();

    @TaskAction
    public void generateTransportVersionData() {
        final var tvDataDir = Objects.requireNonNull(getDataFileDirectory().getAsFile().get());
        final var tvSetName = Objects.requireNonNull(getTVSetName().get());
        final var tvReleaseVersion = ReleaseVersion.fromString(Objects.requireNonNull(getReleaseVersionForTV().get()));

        // Get the latest transport version data for the local version.
        final var latestTVSetData = TransportVersionUtils.getLatestTVSetData(tvDataDir, tvReleaseVersion.toString());

        // Get the latest transport version data for the prior release version.
        final var priorReleaseVersion = getPriorReleaseVersion(tvDataDir, tvReleaseVersion);
        var priorLatestTVSetData = TransportVersionUtils.getLatestTVSetData(tvDataDir, priorReleaseVersion.toString());
        if (priorLatestTVSetData == null) {
            throw new GradleException(
                "The latest Transport Version ID for the prior release was not found at: "
                    + tvDataDir.getAbsolutePath()
                    + formatLatestTVSetFilename(priorReleaseVersion)
                    + " This is required."
            );
        }

        // Determine if it's a (major) version bump
        final var isVersionBump = latestTVSetData == null;
        final var isMajorVersionBump = isVersionBump && (tvReleaseVersion.major - priorReleaseVersion.major > 0);

        // Create the new version
        final var mainReleaseVersion = ReleaseVersion.of(VersionProperties.getElasticsearchVersion());
        final var isTVReleaseVersionMain = tvReleaseVersion.equals(mainReleaseVersion);
        final var tvIDToBump = isVersionBump ? priorLatestTVSetData.ids().getFirst() : latestTVSetData.ids().getFirst();
        int newVersion = bumpVersionNumber(tvIDToBump, tvReleaseVersion, isMajorVersionBump, isTVReleaseVersionMain);

        // Load the tvSetData for the specified name, if it exists
        final var tvSetDataFromFile = TransportVersionUtils.getTVSetData(tvDataDir, tvSetName);
        final var tvSetFileExists = tvSetDataFromFile != null;

        // Create/update the data file
        if (tvSetFileExists) {
            // This is not a new TVSet. We are creating a backport version for an existing TVSet.
            // Check to ensure that there isn't already a TV id for this release version (e.g., if this task has been run twice).
            var existingIDsForReleaseVersion = tvSetDataFromFile.ids().stream().filter(id -> {
                var priorLatestID = priorLatestTVSetData.ids().getFirst();
                return priorLatestID < id && id <= newVersion;
            }).toList();
            if (existingIDsForReleaseVersion.isEmpty() == false) {
                throw new GradleException(
                    "A transport version could not be created because one already exists for this release:"
                        + " Release version: "
                        + tvReleaseVersion
                        + " TransportVersion Id: "
                        + existingIDsForReleaseVersion.getFirst()
                        + " File: "
                        + getTVSetDataFilePath(tvDataDir, tvSetName)
                );
            }

            // Update the existing data file for the backport.
            new TransportVersionSetData(
                tvSetName,
                Streams.concat(tvSetDataFromFile.ids().stream(), Stream.of(newVersion)).sorted().toList().reversed()
            ).writeToDataDir(tvDataDir);
        } else {
            // Create a new data file for the case where this is a new TV
            new TransportVersionSetData(tvSetName, List.of(newVersion)).writeToDataDir(tvDataDir);
        }

        // Update the LATEST file.
        TransportVersionUtils.writeTVSetData(
            tvDataDir,
            formatLatestTVSetFilename(tvReleaseVersion),
            new TransportVersionSetData(tvSetName, List.of(newVersion))
        );
    }

    private static int bumpVersionNumber(
        int tvIDToBump,
        ReleaseVersion releaseVersion,
        boolean majorVersionBump,
        boolean isTVReleaseVersionMain
    ) {

        /* The TV format:
         *
         * M_NNN_S_PP
         *
         * M - The major version of Elasticsearch
         * NNN - The server version part
         * S - The subsidiary version part. It should always be 0 here, it is only used in subsidiary repositories.
         * PP - The patch version part
         */
        if (isTVReleaseVersionMain) {
            if (majorVersionBump) {
                // Bump the major version part, set all other parts to zero.
                return releaseVersion.major * 1_000_000; // TODO add check that this doesn't cause overflow out of server versions
            } else {
                // Bump the server version part if not a major bump.
                return tvIDToBump + 1000; // TODO add check that this doesn't cause overflow out of server versions
            }
        } else {
            // bump the patch version part
            return tvIDToBump + 1; // TODO add check that this doesn't cause overflow out of patch versions
        }
    }

    /**
     * Accepts a major.minor version string (e.g. "9.0") and returns the LATEST.json file of the
     * previous release string (e.g. "8.19-LATEST.json").
     */
    private static ReleaseVersion getPriorReleaseVersion(File tvDataDir, ReleaseVersion releaseVersion) {
        assert tvDataDir != null;
        assert tvDataDir.isDirectory();

        if (releaseVersion.minor > 0) {
            return new ReleaseVersion(releaseVersion.major, releaseVersion.minor - 1);
        }

        // If the minor is 0, we need to find the largest minor on the previous major
        var pattern = Pattern.compile("^(\\d+)\\.(\\d+)-LATEST\\.json$");
        var highestMinorOfPrevMajor = Arrays.stream(Objects.requireNonNull(tvDataDir.listFiles()))
            .filter(tvDataFile -> tvDataFile.getName().endsWith("-LATEST.json"))
            .flatMap(tvDataFile -> {
                var matcher = pattern.matcher(tvDataFile.getName());
                var fileMajor = Integer.parseInt(matcher.group(1));
                var fileMinor = Integer.parseInt(matcher.group(2));
                return fileMajor == releaseVersion.major - 1 ? Stream.of(fileMinor) : Stream.empty();
            })
            .sorted()
            .toList()
            .getLast();
        return new ReleaseVersion(releaseVersion.major - 1, highestMinorOfPrevMajor);
    }

    private static String formatLatestTVSetFilename(ReleaseVersion releaseVersion) {
        return releaseVersion.toString() + LATEST_SUFFIX;
    }

    private record ReleaseVersion(int major, int minor) {
        public static ReleaseVersion fromString(String string) {
            String[] versionParts = string.split("\\.");
            assert versionParts.length == 2;
            return new ReleaseVersion(Integer.parseInt(versionParts[0]), Integer.parseInt(versionParts[1]));
        }

        public static ReleaseVersion of(Version version) {
            return new ReleaseVersion(version.getMajor(), version.getMinor());
        }

        @Override
        public @NotNull String toString() {
            return major + "." + minor;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj instanceof ReleaseVersion) {
                ReleaseVersion that = (ReleaseVersion) obj;
                return major == that.major && minor == that.minor;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(major, minor);
        }
    }
}
