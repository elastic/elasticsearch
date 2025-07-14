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
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.elasticsearch.gradle.internal.transport.TransportVersionUtils.formatLatestTVSetFilename;

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
    public abstract Property<String> getReleaseVersionMajorMinor();


    @TaskAction
    public void generateTransportVersionData() {
        var tvDataDir = Objects.requireNonNull(getDataFileDirectory().getAsFile().get());
        var newTVName = Objects.requireNonNull(getTVSetName().get());
        var majorMinor = Objects.requireNonNull(getReleaseVersionMajorMinor().get());

        // Split version into major and minor
        String[] versionParts = majorMinor.split("\\.");
        assert versionParts.length == 2;
        var major = Integer.parseInt(versionParts[0]);
        var minor = Integer.parseInt(versionParts[1]);

        // Get the latest transport version data for the local version.
        var latestTVSetData = TransportVersionUtils.getLatestTVSetData(tvDataDir, majorMinor);

        // Get the latest transport version data for the prior release version.
        var priorLatestTVSetDataFileName = getPriorLatestTVSetFilename(tvDataDir, majorMinor);
        var priorLatestTVSetData = TransportVersionUtils.getLatestTVSetData(tvDataDir, priorLatestTVSetDataFileName);
        if (priorLatestTVSetData == null) {
            // TODO Can this ever be null?  No, must populate the data file for the latest branch we can no longer backport to.
        }

        // Bump the version number
        int nextVersion;
        if (latestTVSetData == null) {
            // TODO do a major or minor version bump here
            if (minor == 0) {
                // This is major bump
                nextVersion = major * 1_000_000;
            } else {
                // This is a minor bump.  Just increment as usual but from the prior version.
                assert priorLatestTVSetData != null;
                nextVersion = bumpVersionNumber(priorLatestTVSetData.ids.getFirst());
            }
        } else {
            nextVersion = bumpVersionNumber(latestTVSetData.ids.getFirst());
        }
        System.out.println("Latest transport version set: " + latestTVSetData.name + " with IDs: " + latestTVSetData.ids);


        // Load the tvSetData for the specified name.
        var tvSetDataFromFile = TransportVersionUtils.getTVSetData(tvDataDir, newTVName);

        // Create/update the data files
        if (tvSetDataFromFile == null) {
            // Create a new data file for the case where this is a new TV
            new TransportVersionUtils.TransportVersionSetData(newTVName, List.of(nextVersion)).writeToDataDir(tvDataDir);
        } else {
            // This is not a new TV.  We are backporting an existing TVSet.
            // Check to ensure that there isn't already a TV number for this change (e.g. if this task has been run twice).
            var existingIDsForReleaseVersion = tvSetDataFromFile.ids.stream().filter(id -> {
                var priorLatestID = priorLatestTVSetData.ids.getFirst();
                var latestID = latestTVSetData.ids.getFirst();
                return priorLatestID < id && id < latestID;
            }).toList();
            if (existingIDsForReleaseVersion.isEmpty() == false) {
                throw new GradleException("TransportVersion already exists for this release! Release version: " +
                    majorMinor + "TransportVersion Id: " + existingIDsForReleaseVersion.stream().findFirst());
            }

            // Update the existing data file for the backport.
            new TransportVersionUtils.TransportVersionSetData(
                newTVName,
                Streams.concat(tvSetDataFromFile.ids.stream(), Stream.of(nextVersion)).sorted().toList().reversed()
            ).writeToDataDir(tvDataDir);
        }

        // Update the LATEST file.
        TransportVersionUtils.writeTVSetData(
            tvDataDir,
            formatLatestTVSetFilename(majorMinor),
            new TransportVersionUtils.TransportVersionSetData(newTVName, List.of(nextVersion))
        );
    }


    // TODO account for bumping majors.  Need to make a new data file too.
    private static int bumpVersionNumber(int versionNumber) {
        var main = false; // TODO how do we know if we are on main?

        /*
         * M_NNN_S_PP
         *
         * M - The major version of Elasticsearch
         * NNN - The server version part
         * S - The subsidiary version part. It should always be 0 here, it is only used in subsidiary repositories.
         * PP - The patch version part
         */
        if (main) {
            // bump the server versin part
            return versionNumber + 1000; // TODO add check that this doesn't cause overflow out of server versions
        } else {
            // bump the patch version part
            return versionNumber + 1; // TODO add check that this doesn't cause overflow out of patch versions
        }
    }

    /**
     * Accepts a major.minor version string (e.g. "9.0") and returns the LATEST.json file of the
     * previous release string (e.g. "8.19-LATEST.json").
     */
    private static String getPriorLatestTVSetFilename(File tvDataDir, int major, int minor) {
        assert tvDataDir != null;
        assert tvDataDir.isDirectory();

        if (minor > 0) {
            return formatLatestTVSetFilename(major, minor - 1);
        }

        // If the minor is 0, we need to find the largest minor on the previous major
        var pattern = Pattern.compile("^(\\d+)\\.(\\d+)-LATEST\\.json$");
        var highestMinorOfPrevMajor = Arrays.stream(Objects.requireNonNull(tvDataDir.listFiles()))
            .filter(tvDataFile -> tvDataFile.getName().endsWith("-LATEST.json"))
            .flatMap(tvDataFile -> {
                var matcher = pattern.matcher(tvDataFile.getName());
                var localMajor = Integer.parseInt(matcher.group(1));
                var localMinor = Integer.parseInt(matcher.group(2));
                return localMajor == major - 1 ? Stream.of(localMinor) : Stream.empty();
            }).sorted().toList().getLast();

        return formatLatestTVSetFilename(major - 1, highestMinorOfPrevMajor);
    }
}
