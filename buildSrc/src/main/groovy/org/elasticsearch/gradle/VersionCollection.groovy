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

    VersionCollection(List<String> versionLines) {

        List<Version> versions = []

        for (final String line : versionLines) {
            final Matcher match = line =~ /\W+public static final Version V_(\d+)_(\d+)_(\d+)(_alpha\d+|_beta\d+|_rc\d+)? .*/
            if (match.matches()) {
                final Version foundVersion = new Version(
                        Integer.parseInt(match.group(1)), Integer.parseInt(match.group(2)),
                        Integer.parseInt(match.group(3)), (match.group(4) ?: '').replace('_', '-'), false)

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

        // The tip of each minor series (>= 5.6) is unreleased, so set their 'snapshot' flags
        Version prevConsideredVersion = null
        for (final int versionIndex = versions.size() - 1; versionIndex >= 0; versionIndex--) {
            final Version currConsideredVersion = versions[versionIndex]

            if (prevConsideredVersion == null
                    || currConsideredVersion.major != prevConsideredVersion.major
                    || currConsideredVersion.minor != prevConsideredVersion.minor) {

                versions[versionIndex] = new Version(
                        currConsideredVersion.major, currConsideredVersion.minor,
                        currConsideredVersion.revision, currConsideredVersion.suffix, true)
            }

            if (currConsideredVersion.onOrBefore("5.6.0")) {
                break
            }

            prevConsideredVersion = currConsideredVersion
        }

        this.versions = Collections.unmodifiableList(versions)

        if (getCurrentVersion().toString() != VersionProperties.elasticsearch) {
            throw new GradleException("The last version in Versions.java [${versions[-1]}] does not match " +
                    "VersionProperties.elasticsearch [${VersionProperties.elasticsearch}]")
        }
    }

    Version getCurrentVersion() {
        return versions[-1]
    }

    Version getBWCSnapshotForCurrentMajor() {
        return getLastSnapshotWithMajor(currentVersion.major)
    }

    Version getBWCSnapshotForPreviousMajor() {
        return getLastSnapshotWithMajor(currentVersion.major - 1)
    }

    private Version getLastSnapshotWithMajor(int targetMajor) {
        final def currentVersion = currentVersion.toString()
        final int snapshotIndex = versions.findLastIndexOf {
            it.major == targetMajor && it.before(currentVersion) && it.snapshot
        }
        return snapshotIndex == -1 ? null : versions[snapshotIndex]
    }

    private List<Version> versionsOnOrAfterExceptCurrent(Version minVersion) {
        final def minVersionString = minVersion.toString();
        return Collections.unmodifiableList(versions.findAll {
            it.onOrAfter(minVersionString) && it != currentVersion
        })
    }

    List<Version> getVersionsIndexCompatibleWithCurrent() {
        final def firstVersionOfCurrentMajor = versions.find { it.major >= currentVersion.major - 1 }
        return versionsOnOrAfterExceptCurrent(firstVersionOfCurrentMajor)
    }

    private Version getMinimumWireCompatibilityVersion() {
        final def firstIndexOfThisMajor = versions.findIndexOf { it.major == currentVersion.major }
        if (firstIndexOfThisMajor == 0) {
            return versions[0]
        }
        final def lastVersionOfEarlierMajor = versions[firstIndexOfThisMajor - 1]
        return versions.find { it.major == lastVersionOfEarlierMajor.major && it.minor == lastVersionOfEarlierMajor.minor }
    }

    List<Version> getVersionsWireCompatibleWithCurrent() {
        return versionsOnOrAfterExceptCurrent(minimumWireCompatibilityVersion)
    }

    List<Version> getBasicIntegrationTestVersions() {
        // TODO these are the versions checked by `gradle check` for BWC tests. Their choice seems a litle arbitrary.
        List<Version> result = []
        def v1 = BWCSnapshotForCurrentMajor
        if (v1 != null) { // TODO remove null check. Should never be null?
            result.add(v1)
            if (v1.revision == 0) {
                final def v2 = BWCSnapshotForPreviousMajor
                result.add(v2)
            }
        }

        return Collections.unmodifiableList(result);
    }
}
