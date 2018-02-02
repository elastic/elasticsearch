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

    static void main(String[] args) throws Exception {

        VersionCollection vc = new VersionCollection(new File('server/src/main/java/org/elasticsearch/Version.java').readLines('UTF-8'))

        println("indx-" + vc.versionsIndexCompatibleWithCurrent)
        println("wire-" + vc.versionsWireCompatibleWithCurrent)

        println("indx-" + vc.snapshotVersionsIndexCompatibleWithCurrent)
        println("wire-" + vc.snapshotVersionsWireCompatibleWithCurrent)

        println("Version: ${vc.currentVersion} NMS: ${vc.nextMinorSnapshot}, SMS: ${vc.stagedMinorSnapshot}, " +
                "NBS: ${vc.nextBugfixSnapshot}, MBS: ${vc.maintenanceBugfixSnapshot}")
    }

    private final List<Version> versions
    Version nextMinorSnapshot
    Version stagedMinorSnapshot
    Version nextBugfixSnapshot
    Version maintenanceBugfixSnapshot
    final Version currentVersion

    private final boolean buildSnapshot = System.getProperty("build.snapshot", "true") == "true"

    /**
     * Construct a VersionCollection from the lines of the Version.java file.
     * @param versionLines The lines of the Version.java file.
     */
    VersionCollection(List<String> versionLines) {

        List<Version> versions = []
        // This class should be converted wholesale to use the treeset
        TreeSet<Version> versionSet = new TreeSet<>()

        for (final String line : versionLines) {
            final Matcher match = line =~ /\W+public static final Version V_(\d+)_(\d+)_(\d+)(_alpha\d+|_beta\d+|_rc\d+)? .*/
            if (match.matches()) {
                final Version foundVersion = new Version(
                        Integer.parseInt(match.group(1)), Integer.parseInt(match.group(2)),
                        Integer.parseInt(match.group(3)), (match.group(4) ?: '').replace('_', '-'), false)
                if (versionSet.add(foundVersion) == false) {
                    throw new GradleException("Versions.java contains duplicate entries for ${foundVersion}")
                }
            }
        }

        if (versionSet.empty) {
            throw new GradleException("Unexpectedly found no version constants in Versions.java");
        }

        // prune released alpha/beta/rc out, as well as any between the currentVersion and the next version thats not the same x.y.z
        versionSet.removeAll { it.suffix.isEmpty() == false && isMajorReleased(it, versionSet) }
        // grab the last alpha/beta/rc of the current set as the one to be used as the actual version
        Version lastVersion = versionSet.last()
        currentVersion = new Version(lastVersion.major, lastVersion.minor, lastVersion.revision, lastVersion.suffix, buildSnapshot)
        // remove all of the potential alpha/beta/rc on the currentVersion version
        versionSet.removeAll {
            it.suffix.isEmpty() == false &&
            it.major == currentVersion.major &&
            it.minor == currentVersion.minor &&
            it.revision == currentVersion.revision }

        // readd the removed currentVersion but with the proper buildSnapshot value
        versionSet.add(currentVersion)

        // This needs to be extracted out in such a way that its easy to change for old branches
        boolean isReleasableBranch = true

        // dont look at this, its horrible, it needs major refactoring
        // dont look at this, its horrible, it needs major refactoring
        // dont look at this, its horrible, it needs major refactoring
        // dont look at this, its horrible, it needs major refactoring
        // dont look at this, its horrible, it needs major refactoring
        // dont look at this, its horrible, it needs major refactoring
        if (isReleasableBranch) {
            // check if the minor has been released
            if (isReleased(currentVersion)) {
                // if the minor has been released then it only has a maintenance version
                // now dip back 1 version to get the last supported snapshot version of the line
                Version highestMinor = versionSet.floor(Version.fromString("${currentVersion.major - 1}.99.0"))
                versionSet.remove(highestMinor);
                Version nextVersion = new Version(highestMinor.major, highestMinor.minor, highestMinor.revision, highestMinor.suffix, true)
                maintenanceBugfixSnapshot = nextVersion
                versionSet.add(nextVersion)
            } else {
                // caveat, if our currentVersion is a X.0.0, we need to check X-1 minors to see if they are released
                if (currentVersion.minor == 0) {
                    TreeSet previousMajorSet = versionSet
                            .tailSet(Version.fromString("${currentVersion.major - 1}.0.0"))
                            .headSet(currentVersion)
                    // for each minor in this set, if its unreleased, it should be a snapshot, if it has been releasd, just grab the first released branch
                    for (int minor = previousMajorSet.last().minor; minor >= 0; minor--) {
                        TreeSet minorSet = versionSet
                                .tailSet(Version.fromString("${currentVersion.major - 1}.${minor}.0"))
                                .headSet(Version.fromString("${currentVersion.major - 1}.${minor + 1}.0"))
                        if (minorSet.size() == 1) {
                            // if only 1 minor, its a snapshot
                            Version minorVersion = minorSet.first()
                            versionSet.remove(minorVersion)
                            // This should only ever contain 2 branches in flight. An example is 6.x is frozen, and 6.2 is cut but not yet released
                            // there is some simple logic to make sure that in the case of more than 2, it will bail
                            Version nextVersion = new Version(minorVersion.major, minorVersion.minor, minorVersion.revision, minorVersion.suffix, true)
                            if (nextMinorSnapshot == null) {
                                // it has not been set yet
                                nextMinorSnapshot = nextVersion
                            } else if (stagedMinorSnapshot == null) {
                                stagedMinorSnapshot = nextVersion
                            } else {
                                throw new GradleException("More than 2 snapshot version existed for the next minor and staged (frozen) minors.")
                            }
                            versionSet.add(nextVersion)
                        } else {
                            // this is the last minor snap for this major, so replace the top one of these and break
                            Version minorVersion = minorSet.last()
                            versionSet.remove(minorVersion)
                            Version nextVersion = new Version(minorVersion.major, minorVersion.minor, minorVersion.revision, minorVersion.suffix, true)
                            nextBugfixSnapshot = nextVersion
                            versionSet.add(nextVersion)
                            break;
                        }
                    }
                    // now dip back 2 versions to get the last supported snapshot version of the line
                    Version highestMinor = versionSet.floor(Version.fromString("${currentVersion.major - 2}.99.0"))
                    versionSet.remove(highestMinor);
                    Version nextVersion = new Version(highestMinor.major, highestMinor.minor, highestMinor.revision, highestMinor.suffix, true)
                    maintenanceBugfixSnapshot = nextVersion
                    versionSet.add(nextVersion)
                } else {
                    // our version is not a X.0.0, so we are somewhere on a X.Y line
                    // only check till minor == 0 of the major
                    for (int minor = currentVersion.minor - 1; minor >= 0; minor--) {
                        TreeSet minorSet = versionSet
                                .tailSet(Version.fromString("${currentVersion.major}.${minor}.0"))
                                .headSet(Version.fromString("${currentVersion.major}.${minor + 1}.0"))
                        if (minorSet.size() == 1) {
                            // if only 1 minor, its a snapshot
                            Version minorVersion = minorSet.first()
                            versionSet.remove(minorVersion)
                            // This should only ever contain 0 or 1 branch in flight. An example is 6.x is frozen, and 6.2 is cut but not yet released
                            // there is some simple logic to make sure that in the case of more than 1, it will bail
                            Version nextVersion = new Version(minorVersion.major, minorVersion.minor, minorVersion.revision, minorVersion.suffix, true)
                            if (stagedMinorSnapshot == null) {
                                stagedMinorSnapshot = nextVersion
                            } else {
                                throw new GradleException("More than 1 snapshot version existed for the staged (frozen) minors.")
                            }
                            versionSet.add(nextVersion)
                        } else {
                            // this is the last minor snap for this major, so replace the top one of these and break
                            Version minorVersion = minorSet.last()
                            versionSet.remove(minorVersion)
                            Version nextVersion = new Version(minorVersion.major, minorVersion.minor, minorVersion.revision, minorVersion.suffix, true)
                            nextBugfixSnapshot = nextVersion
                            versionSet.add(nextVersion)
                            break;
                        }
                    }
                    // now dip back 1 version to get the last supported snapshot version of the line
                    Version highestMinor = versionSet.floor(Version.fromString("${currentVersion.major - 1}.99.0"))
                    versionSet.remove(highestMinor);
                    Version nextVersion = new Version(highestMinor.major, highestMinor.minor, highestMinor.revision, highestMinor.suffix, true)
                    maintenanceBugfixSnapshot = nextVersion
                    versionSet.add(nextVersion)
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

    List<Version> getSnapshotVersionsIndexCompatibleWithCurrent() {
        List<Version> compatSnapshots = []
        List<Version> allCompatVersions = getVersionsIndexCompatibleWithCurrent()
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

        return compatSnapshots;
    }
    /**
     * @return All earlier versions that should be tested for wire BWC with the current version.
     */
    List<Version> getVersionsWireCompatibleWithCurrent() {
        return versionsOnOrAfterExceptCurrent(minimumWireCompatibilityVersion)
    }

    List<Version> getSnapshotVersionsWireCompatibleWithCurrent() {
        List<Version> compatSnapshots = []
        List<Version> allCompatVersions = getVersionsWireCompatibleWithCurrent()
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

        return compatSnapshots;
    }

    /**
     * Uses basic logic about our releases to determine if this version has been previously released
     * @param version
     * @return
     */
    private boolean isReleased(Version version) {
        return version.revision > 0 || (version.revision > 1 && currentVersion.equals(Version.fromString("5.1.2")))
    }

    private boolean isMajorReleased(Version version, TreeSet<Version> items) {
        return items
            .tailSet(Version.fromString("${version.major}.0.0"))
            .headSet(Version.fromString("${version.major + 1}.0.0"))
            .count { it.suffix.isEmpty() }  // count only non suffix'd versions as actual versions that may be released
            .intValue() > 1
    }
}
