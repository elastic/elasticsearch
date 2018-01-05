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

package org.elasticsearch.gradle

import org.gradle.api.GradleException

import java.util.regex.Matcher

/**
 * The collection of version constants declared in Version.java, for use in BWC testing.
 */
class VersionCollection {

    private final List<Version> versions
    private final boolean buildSnapshot = System.getProperty("build.snapshot", "true") == "true"

    /**
     * Construct a VersionCollection from the lines of the Version.java file.
     * @param versionLines The lines of the Version.java file.
     */
    VersionCollection(List<String> versionLines) {

        List<Version> versions = []

        for (final String line : versionLines) {
            final Matcher match = line =~ /\W+public static final Version V_(\d+)_(\d+)_(\d+)(_alpha\d+|_beta\d+|_rc\d+)? .*/
            if (match.matches()) {
                final Version foundVersion = new Version(
                        Integer.parseInt(match.group(1)), Integer.parseInt(match.group(2)),
                        Integer.parseInt(match.group(3)), (match.group(4) ?: '').replace('_', '-'), false, null)

                if (versions.size() > 0 && foundVersion.onOrBeforeIncludingSuffix(versions[-1])) {
                    throw new GradleException("Versions.java contains out of order version constants:" +
                            " ${foundVersion} should come before ${versions[-1]}")
                }

                // Only keep the last alpha/beta/rc in the series
                if (versions.size() > 0 && versions[-1].id == foundVersion.id) {
                    versions[-1] = foundVersion
                } else {
                    versions.add(foundVersion)
                }
            }
        }

        if (versions.empty) {
            throw new GradleException("Unexpectedly found no version constants in Versions.java");
        }

        /*
         * The tip of each minor series (>= 5.6) is unreleased, so they must be built from source (we set branch to non-null), and we set
         * the snapshot flag if and only if build.snapshot is true.
         */
        Version prevConsideredVersion = null
        boolean found6xSnapshot = false
        for (final int versionIndex = versions.size() - 1; versionIndex >= 0; versionIndex--) {
            final Version currConsideredVersion = versions[versionIndex]

            if (prevConsideredVersion == null
                    || currConsideredVersion.major != prevConsideredVersion.major
                    || currConsideredVersion.minor != prevConsideredVersion.minor) {

                // This is a snapshot version. Work out its branch. NB this doesn't name the current branch correctly, but this doesn't
                // matter as we don't BWC test against it.
                String branch = "${currConsideredVersion.major}.${currConsideredVersion.minor}"

                if (false == found6xSnapshot && currConsideredVersion.major == 6) {
                    // TODO needs generalising to deal with when 7.x is cut, and when 6.x is deleted, and so on...
                    branch = "6.x"
                    found6xSnapshot = true
                }

                versions[versionIndex] = new Version(
                        currConsideredVersion.major, currConsideredVersion.minor,
                        currConsideredVersion.revision, currConsideredVersion.suffix, buildSnapshot, branch)
            }

            if (currConsideredVersion.onOrBefore("5.6.0")) {
                break
            }

            prevConsideredVersion = currConsideredVersion
        }

        this.versions = Collections.unmodifiableList(versions)
    }

    /**
     * @return The list of versions read from the Version.java file
     */
    List<Version> getVersions() {
        return Collections.unmodifiableList(versions)
    }

    /**
     * @return The latest version in the Version.java file, which must be the current version of the system.
     */
    Version getCurrentVersion() {
        return versions[-1]
    }

    /**
     * @return The snapshot at the end of the previous minor series in the current major series, or null if this is the first minor series.
     */
    Version getBWCSnapshotForCurrentMajor() {
        return getLastSnapshotWithMajor(currentVersion.major)
    }

    /**
     * @return The snapshot at the end of the previous major series, which must not be null.
     */
    Version getBWCSnapshotForPreviousMajor() {
        Version version = getLastSnapshotWithMajor(currentVersion.major - 1)
        assert version != null : "getBWCSnapshotForPreviousMajor(): found no versions in the previous major"
        return version
    }

    private Version getLastSnapshotWithMajor(int targetMajor) {
        final String currentVersion = currentVersion.toString()
        final int snapshotIndex = versions.findLastIndexOf {
            it.major == targetMajor && it.before(currentVersion) && it.snapshot == buildSnapshot
        }
        return snapshotIndex == -1 ? null : versions[snapshotIndex]
    }

    private List<Version> versionsOnOrAfterExceptCurrent(Version minVersion) {
        final String minVersionString = minVersion.toString()
        return Collections.unmodifiableList(versions.findAll {
            it.onOrAfter(minVersionString) && it != currentVersion
        })
    }

    /**
     * @return All earlier versions that should be tested for index BWC with the current version.
     */
    List<Version> getVersionsIndexCompatibleWithCurrent() {
        final Version firstVersionOfCurrentMajor = versions.find { it.major >= currentVersion.major - 1 }
        return versionsOnOrAfterExceptCurrent(firstVersionOfCurrentMajor)
    }

    private Version getMinimumWireCompatibilityVersion() {
        final int firstIndexOfThisMajor = versions.findIndexOf { it.major == currentVersion.major }
        if (firstIndexOfThisMajor == 0) {
            return versions[0]
        }
        final Version lastVersionOfEarlierMajor = versions[firstIndexOfThisMajor - 1]
        return versions.find { it.major == lastVersionOfEarlierMajor.major && it.minor == lastVersionOfEarlierMajor.minor }
    }

    /**
     * @return All earlier versions that should be tested for wire BWC with the current version.
     */
    List<Version> getVersionsWireCompatibleWithCurrent() {
        return versionsOnOrAfterExceptCurrent(minimumWireCompatibilityVersion)
    }

    /**
     * `gradle check` does not run all BWC tests. This defines which tests it does run.
     * @return Versions to test for BWC during gradle check.
     */
    List<Version> getBasicIntegrationTestVersions() {
        // TODO these are the versions checked by `gradle check` for BWC tests. Their choice seems a litle arbitrary.
        List<Version> result = [BWCSnapshotForPreviousMajor, BWCSnapshotForCurrentMajor]
        return Collections.unmodifiableList(result.findAll { it != null })
    }
}
