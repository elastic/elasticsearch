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
import org.gradle.api.InvalidUserDataException

import java.util.regex.Matcher

/**
 * The collection of version constants declared in Version.java, for use in BWC testing.
 *
 *  if major+1 released: released artifacts from $version down to major-1.highestMinor.highestPatch, none of these should be snapshots, period.
 *  if major+1 unreleased:
 *  - if released:
 *   -- caveat 0: snapshot for the major-1.highestMinor.highestPatch
 *  - if unreleased:
 *   -- caveat 0: snapshot for the major-1.highestMinor.highestPatch
 *   -- caveat 1: every same major lower minor branch should also be tested if its released, and if not, its a snapshot. There should only be max 2 of these.
 *   -- caveat 2: the largest released minor branch before the unreleased minor should also be a snapshot
 *   -- caveat 3: if the current version is a different major than the previous rules apply to major - 1 of the current version
 *
 * Please note that the caveat's also correspond with the 4 types of snapshots.
 * - Caveat 0 - always maintenanceBugfixSnapshot.
 * - Caveat 1 - This is tricky. If caveat 3 applies, the highest matching value is nextMinorSnapshot, if there is another it is the stagedMinorSnapshot.
 *              If caveat 3 does not apply then the only possible value is the stagedMinorSnapshot.
 * - Caveat 2 - always nextBugfixSnapshot
 * - Caveat 3 - this only changes the applicability of Caveat 1
 *
 * Notes on terminology:
 * - The case for major+1 being released is accomplished through the isReleasableBranch value. If this is false, then the branch is no longer
 *   releasable, meaning not to test against any snapshots.
 * - Released is defined as having > 1 suffix-free version in a major.minor series. For instance, only 6.2.0 means unreleased, but a
 *   6.2.0 and 6.2.1 mean that 6.2.0 was released already.
 */
class VersionCollection {

    private final List<Version> versions
    Version nextMinorSnapshot
    Version stagedMinorSnapshot
    Version nextBugfixSnapshot
    Version maintenanceBugfixSnapshot
    final Version currentVersion
    private final TreeSet<Version> versionSet = new TreeSet<>()
    final List<String> snapshotProjectNames = ['next-minor-snapshot',
                                               'staged-minor-snapshot',
                                               'next-bugfix-snapshot',
                                               'maintenance-bugfix-snapshot']

    // When we roll 8.0 its very likely these will need to be extracted from this class
    private final boolean isReleasableBranch = true

    /**
     * Construct a VersionCollection from the lines of the Version.java file. The basic logic for the following is pretty straight forward.

     * @param versionLines The lines of the Version.java file.
     */
    VersionCollection(List<String> versionLines) {
        final boolean buildSnapshot = System.getProperty("build.snapshot", "true") == "true"

        List<Version> versions = []
        // This class should be converted wholesale to use the treeset

        for (final String line : versionLines) {
            final Matcher match = line =~ /\W+public static final Version V_(\d+)_(\d+)_(\d+)(_alpha\d+|_beta\d+|_rc\d+)? .*/
            if (match.matches()) {
                final Version foundVersion = new Version(
                        Integer.parseInt(match.group(1)), Integer.parseInt(match.group(2)),
                        Integer.parseInt(match.group(3)), (match.group(4) ?: '').replace('_', '-'), false)
                safeAddToSet(foundVersion)
            }
        }

        if (versionSet.empty) {
            throw new GradleException("Unexpectedly found no version constants in Versions.java")
        }

        // If the major version has been released, then remove all of the alpha/beta/rc versions that exist in the set
        versionSet.removeAll { it.suffix.isEmpty() == false && isMajorReleased(it, versionSet) }

        // set currentVersion
        Version lastVersion = versionSet.last()
        currentVersion = new Version(lastVersion.major, lastVersion.minor, lastVersion.revision, lastVersion.suffix, buildSnapshot)

        // remove all of the potential alpha/beta/rc from the currentVersion
        versionSet.removeAll {
            it.suffix.isEmpty() == false &&
            it.major == currentVersion.major &&
            it.minor == currentVersion.minor &&
            it.revision == currentVersion.revision }

        // re-add the currentVersion to the set
        versionSet.add(currentVersion)

        if (isReleasableBranch) {
            if (isReleased(currentVersion)) {
                // caveat 0 - if the minor has been released then it only has a maintenance version
                // go back 1 version to get the last supported snapshot version of the line, which is a maint bugfix
                Version highestMinor = getHighestPreviousMinor(currentVersion.major)
                maintenanceBugfixSnapshot = replaceAsSnapshot(highestMinor)
            } else {
                // caveat 3 - if our currentVersion is a X.0.0, we need to check X-1 minors to see if they are released
                if (currentVersion.minor == 0) {
                    for (Version version: getMinorTips(currentVersion.major - 1)) {
                        if (isReleased(version) == false) {
                            // caveat 1 - This should only ever contain 2 non released branches in flight. An example is 6.x is frozen,
                            // and 6.2 is cut but not yet released there is some simple logic to make sure that in the case of more than 2,
                            // it will bail. The order is that the minor snapshot is fulfilled first, and then the staged minor snapshot
                            if (nextMinorSnapshot == null) {
                                // it has not been set yet
                                nextMinorSnapshot = replaceAsSnapshot(version)
                            } else if (stagedMinorSnapshot == null) {
                                stagedMinorSnapshot = replaceAsSnapshot(version)
                            } else {
                                throw new GradleException("More than 2 snapshot version existed for the next minor and staged (frozen) minors.")
                            }
                        } else {
                            // caveat 2 - this is the last minor snap for this major, so replace the highest (last) one of these and break
                            nextBugfixSnapshot = replaceAsSnapshot(version)
                            // we only care about the largest minor here, so in the case of 6.1 and 6.0, it will only get 6.1
                            break
                        }
                    }
                    // caveat 0 - the last supported snapshot of the line is on a version that we don't support (N-2)
                    maintenanceBugfixSnapshot = null
                } else {
                    // caveat 3 did not apply. version is not a X.0.0, so we are somewhere on a X.Y line
                    // only check till minor == 0 of the major
                    for (Version version: getMinorTips(currentVersion.major)) {
                        if (isReleased(version) == false) {
                            // caveat 1 - This should only ever contain 0 or 1 branch in flight. An example is 6.x is frozen, and 6.2 is cut
                            // but not yet released there is some simple logic to make sure that in the case of more than 1, it will bail
                            if (stagedMinorSnapshot == null) {
                                stagedMinorSnapshot = replaceAsSnapshot(version)
                            } else {
                                throw new GradleException("More than 1 snapshot version existed for the staged (frozen) minors.")
                            }
                        } else {
                            // caveat 2 - this is the last minor snap for this major, so replace the highest (last) one of these and break
                            nextBugfixSnapshot = replaceAsSnapshot(version)
                            // we only care about the largest minor here, so in the case of 6.1 and 6.0, it will only get 6.1
                            break
                        }
                    }
                    // caveat 0 - now dip back 1 version to get the last supported snapshot version of the line
                    Version highestMinor = getHighestPreviousMinor(currentVersion.major)
                    maintenanceBugfixSnapshot = replaceAsSnapshot(highestMinor)
                }
            }
        }

        this.versions = Collections.unmodifiableList(versionSet.toList())
    }

    /**
     * @return The list of versions read from the Version.java file
     */
    List<Version> getVersions() {
        return versions
    }

    /**
     * Index compat supports 1 previous entire major version. For instance, any 6.x test for this would test all of 5 up to that 6.x version
     *
     * @return All earlier versions that should be tested for index BWC with the current version.
     */
    List<Version> getIndexCompatible() {
        int actualMajor = (currentVersion.major == 5 ? 2 : currentVersion.major - 1)
        return versionSet
            .tailSet(Version.fromString("${actualMajor}.0.0"))
            .headSet(currentVersion)
            .asList()
    }

    /**
     * Ensures the types of snapshot are not null and are also in the index compat list
     */
    List<Version> getSnapshotsIndexCompatible() {
        List<Version> compatSnapshots = []
        List<Version> allCompatVersions = getIndexCompatible()
        if (allCompatVersions.contains(nextMinorSnapshot)) {
            compatSnapshots.add(nextMinorSnapshot)
        }
        if (allCompatVersions.contains(stagedMinorSnapshot)) {
            compatSnapshots.add(stagedMinorSnapshot)
        }
        if (allCompatVersions.contains(nextBugfixSnapshot)) {
            compatSnapshots.add(nextBugfixSnapshot)
        }
        if (allCompatVersions.contains(maintenanceBugfixSnapshot)) {
            compatSnapshots.add(maintenanceBugfixSnapshot)
        }

        return compatSnapshots
    }

    /**
     * Wire compat supports the last minor of the previous major. For instance, any 6.x test would test 5.6 up to that 6.x version
     *
     * @return All earlier versions that should be tested for wire BWC with the current version.
     */
    List<Version> getWireCompatible() {
        // Get the last minor of the previous major
        Version lowerBound = getHighestPreviousMinor(currentVersion.major)
        return versionSet
            .tailSet(Version.fromString("${lowerBound.major}.${lowerBound.minor}.0"))
            .headSet(currentVersion)
            .toList()
    }

    /**
     * Ensures the types of snapshot are not null and are also in the wire compat list
     */
    List<Version> getSnapshotsWireCompatible() {
        List<Version> compatSnapshots = []
        List<Version> allCompatVersions = getWireCompatible()
        if (allCompatVersions.contains(nextMinorSnapshot)) {
            compatSnapshots.add(nextMinorSnapshot)
        }
        if (allCompatVersions.contains(stagedMinorSnapshot)) {
            compatSnapshots.add(stagedMinorSnapshot)
        }
        if (allCompatVersions.contains(nextBugfixSnapshot)) {
            compatSnapshots.add(nextBugfixSnapshot)
        }
        if (allCompatVersions.contains(maintenanceBugfixSnapshot)) {
            compatSnapshots.add(maintenanceBugfixSnapshot)
        }
        // There was no wire compat for the 2.x line
        compatSnapshots.removeAll {it.major == 2}

        return compatSnapshots
    }

    /**
     * Grabs the proper snapshot based on the name passed in. These names should correspond with gradle project names under bwc. If you
     * are editing this if/else it is only because you added another project under :distribution:bwc. Do not modify this method or its
     * reasoning for throwing the exception unless you are sure that it will not harm :distribution:bwc.
     */
    Version getSnapshotForProject(String snapshotProjectName) {
        if (snapshotProjectName == 'next-minor-snapshot') {
            return nextMinorSnapshot
        } else if (snapshotProjectName == 'staged-minor-snapshot') {
            return stagedMinorSnapshot
        } else if (snapshotProjectName == 'maintenance-bugfix-snapshot') {
            return maintenanceBugfixSnapshot
        } else if (snapshotProjectName == 'next-bugfix-snapshot') {
            return nextBugfixSnapshot
        } else {
            throw new InvalidUserDataException("Unsupported project name ${snapshotProjectName}")
        }
    }

    /**
     * Uses basic logic about our releases to determine if this version has been previously released
     */
    private boolean isReleased(Version version) {
        return version.revision > 0
    }

    /**
     * Validates that the count of non suffixed (alpha/beta/rc) versions in a given major to major+1 is greater than 1.
     * This means that there is more than just a major.0.0 or major.0.0-alpha in a branch to signify it has been prevously released.
     */
    private boolean isMajorReleased(Version version, TreeSet<Version> items) {
        return items
            .tailSet(Version.fromString("${version.major}.0.0"))
            .headSet(Version.fromString("${version.major + 1}.0.0"))
            .count { it.suffix.isEmpty() }  // count only non suffix'd versions as actual versions that may be released
            .intValue() > 1
    }

    /**
     * Gets the largest version previous major version based on the nextMajorVersion passed in.
     * If you have a list [5.0.2, 5.1.2, 6.0.1, 6.1.1] and pass in 6 for the nextMajorVersion, it will return you 5.1.2
     */
    private Version getHighestPreviousMinor(Integer nextMajorVersion) {
        SortedSet<Version> result = versionSet.headSet(Version.fromString("${nextMajorVersion}.0.0"))
        return result.isEmpty() ? null : result.last()
    }

    /**
     * Helper function for turning a version into a snapshot version, removing and readding it to the tree
     */
    private Version replaceAsSnapshot(Version version) {
        versionSet.remove(version)
        Version snapshotVersion = new Version(version.major, version.minor, version.revision, version.suffix, true)
        safeAddToSet(snapshotVersion)
        return snapshotVersion
    }

    /**
     * Safely adds a value to the treeset, or bails if the value already exists.
     * @param version
     */
    private void safeAddToSet(Version version) {
        if (versionSet.add(version) == false) {
            throw new GradleException("Versions.java contains duplicate entries for ${version}")
        }
    }

    /**
     * Gets the entire set of major.minor.* given those parameters.
     */
    private SortedSet<Version> getMinorSetForMajor(Integer major, Integer minor) {
        return versionSet
            .tailSet(Version.fromString("${major}.${minor}.0"))
            .headSet(Version.fromString("${major}.${minor + 1}.0"))
    }

    /**
     * Gets the entire set of major.* to the currentVersion
     */
    private SortedSet<Version> getMajorSet(Integer major) {
        return versionSet
            .tailSet(Version.fromString("${major}.0.0"))
            .headSet(currentVersion)
    }

    /**
     * Gets the tip of each minor set and puts it in a list.
     *
     * examples:
     *  [1.0.0, 1.1.0, 1.1.1, 1.2.0, 1.3.1] will return [1.0.0, 1.1.1, 1.2.0, 1.3.1]
     *  [1.0.0, 1.0.1, 1.0.2, 1.0.3, 1.0.4] will return [1.0.4]
     */
    private List<Version> getMinorTips(Integer major) {
        TreeSet<Version> majorSet = getMajorSet(major)
        List<Version> minorList = new ArrayList<>()
        for (int minor = majorSet.last().minor; minor >= 0; minor--) {
            TreeSet<Version> minorSetInMajor = getMinorSetForMajor(major, minor)
            minorList.add(minorSetInMajor.last())
        }
        return minorList
    }
}
