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
package org.elasticsearch.gradle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

/**
 * Parse the Java source file containing the versions declarations and use the known rules to figure out which are all
 * the version the current one is wire and index compatible with.
 * On top of this, figure out which of these are unreleased and provide the branch they can be built from.
 * <p>
 * Note that in this context, currentVersion is the unreleased version this build operates on.
 * At any point in time there will surely be four such unreleased versions being worked on,
 * thus currentVersion will be one of these:
 * - the unreleased <b>major</b>, a+1.0.0 on the `master` branch
 * - the unreleased <b>minor</b>,  a.b.0 ( b != 0) on the `a.x` branch
 * - the unreleased <b>maintenance</b>, a.b.c (c != 0) on the `a.b` branch
 * - the unreleased <b>bugfix</b>, a-1.d.e ( d != 0, e != 0) on the `(a-1).d` branch
 * In addition to these, there will be a fifth one when a minor reaches feature freeze, we call this the <i>staged</i>
 * version:
 * - the unreleased <b>staged</b>, a.b-2.0 (b > 2) on the `a.(b-2)` branch
 * Each build is only concerned with possible unreleased versions before it, as those are the ones that need to be tested
 * for backwards compatibility. We never look forward, and don't add forward facing version number to branches of previous
 * version.
 * <p>
 * The build know the current version, and we parse server code to find the rest making sure that these match.
 * We can reliably figure out which the unreleased versions are due to the convention of always adding the next unreleased
 * version number to server in all branches when a version is released.
 * This convention is enforced by checking the versions we consider to be unreleased against an
 * authoritative source (maven central).
 */
public class VersionCollection {

    private static final Pattern LINE_PATTERN = Pattern.compile(
        "\\W+public static final Version V_(\\d+)_(\\d+)_(\\d+)(_alpha\\d+|_beta\\d+|_rc\\d+)? .*"
    );

    private final Version currentVersion;
    private final Map<Integer, List<Version>> groupByMajor;

    public class UnreleasedVersionDescription {
        private final Version version;
        private final String branch;
        private final String gradleProjectName;

        UnreleasedVersionDescription(Version version, String branch, String gradleProjectName) {
            this.version = version;
            this.branch = branch;
            this.gradleProjectName = gradleProjectName;
        }

        public Version getVersion() {
            return version;
        }

        public String getBranch() {
            return branch;
        }

        public String getGradleProjectName() {
            return gradleProjectName;
        }
    }

    public VersionCollection(List<String> versionLines) {
        this(versionLines, VersionProperties.getElasticsearch());
    }

    protected VersionCollection(List<String> versionLines, Version currentVersionProperty) {
        groupByMajor = versionLines.stream()
            .map(LINE_PATTERN::matcher)
            .filter(Matcher::matches)
            .map(match -> new Version(
                Integer.parseInt(match.group(1)),
                Integer.parseInt(match.group(2)),
                Integer.parseInt(match.group(3)),
                (match.group(4) == null ? "" : match.group(4)).replace('_', '-'),
                false
            ))
            .filter(version -> version.getSuffix().isEmpty() || version.equals(currentVersionProperty))
            .collect(Collectors.groupingBy(Version::getMajor, Collectors.toList()));

        if (groupByMajor.isEmpty()) {
            throw new IllegalArgumentException("Could not parse any versions");
        }

        currentVersion = getLatestVersionByKey(
            groupByMajor,
            groupByMajor.keySet().stream().max(Integer::compareTo)
                .orElseThrow(() -> new IllegalStateException("Unexpected number of versions in collection"))
        );

        assertCurrentVersionMatchesParsed(currentVersionProperty);

        assertNoOlderThanTwoMajors();

        markUnreleasedAsSnapshot();
    }

    private void markUnreleasedAsSnapshot() {
        getUnreleased()
            .forEach(unreleased -> {
                groupByMajor.get(unreleased.getMajor()).remove(unreleased);
                groupByMajor.get(unreleased.getMajor()).add(
                    new Version(
                        unreleased.getMajor(), unreleased.getMinor(), unreleased.getRevision(),
                        unreleased.getSuffix(), true
                    )
                );
            });
    }

    private void assertNoOlderThanTwoMajors() {
        Set<Integer> majors = groupByMajor.keySet();
        if (majors.size() != 2) {
            throw new IllegalStateException(
                "Expected exactly 2 majors in parsed versions but found: " + majors
            );
        }
    }

    private void assertCurrentVersionMatchesParsed(Version currentVersionProperty) {
        if (currentVersionProperty.equals(currentVersion) == false) {
            throw new IllegalStateException(
                "Parsed versions latest version does not match the one configured in build properties. " +
                    "Parsed latest version is " + currentVersion + " but the build has " +
                    currentVersionProperty
            );
        }
    }

    public void forPreviousUnreleased(Consumer<UnreleasedVersionDescription> consumer) {
        getUnreleased().stream()
            .filter(version -> version.equals(currentVersion) == false)
            .forEach(version -> consumer.accept(
                new UnreleasedVersionDescription(
                    version,
                    getBranchFor(version),
                    getGradleProjectNameFor(version)
                )
            ));
    }

    private String getGradleProjectNameFor(Version version) {
        if (version.equals(currentVersion)) {
            throw new IllegalArgumentException("The Gradle project to build " + version + " is the current build.");
        }
        Map<Integer, List<Version>> releasedMajorGroupedByMinor = getReleasedMajorGroupedByMinor();

        if (version.getRevision() == 0) {
            if (releasedMajorGroupedByMinor
                .get(releasedMajorGroupedByMinor.keySet().stream().max(Integer::compareTo).orElse(0))
                .contains(version)) {
                return "minor";
            } else {
                return "staged";
            }
        } else {
            if (releasedMajorGroupedByMinor
                .getOrDefault(version.getMinor(), emptyList())
                .contains(version)) {
                return "maintenance";
            } else {
                return "bugfix";
            }
        }
    }

    private String getBranchFor(Version version) {
        switch (getGradleProjectNameFor(version)) {
            case "minor":
                return version.getMajor() + ".x";
            case "staged":
            case "maintenance":
            case "bugfix":
                return version.getMajor() + "." + version.getMinor();
            default:
                throw new IllegalStateException("Unexpected Gradle project name");
        }
    }

    public List<Version> getUnreleased() {
        List<Version> unreleased = new ArrayList<>();
        // The current version is being worked, is always unreleased
        unreleased.add(currentVersion);

        // the tip of the previous major is unreleased for sure, be it a minor or a bugfix
        unreleased.add(getLatestVersionByKey(this.groupByMajor, currentVersion.getMajor() - 1));

        final Map<Integer, List<Version>> groupByMinor = getReleasedMajorGroupedByMinor();
        int greatestMinor = groupByMinor.keySet().stream().max(Integer::compareTo).orElse(0);

        // the last bugfix for this minor series is always unreleased
        unreleased.add(getLatestVersionByKey(groupByMinor, greatestMinor));

        if (groupByMinor.get(greatestMinor).size() == 1) {
            // we found an unreleased minor
            unreleased.add(getLatestVersionByKey(groupByMinor, greatestMinor - 1));
            if (groupByMinor.getOrDefault(greatestMinor - 1, emptyList()).size() == 1) {
                // we found that the previous minor is staged but not yet released
                // in this case, the minor before that has a bugfix
                unreleased.add(getLatestVersionByKey(groupByMinor, greatestMinor - 2));
            }
        }

        return unmodifiableList(
            unreleased.stream()
                .sorted()
                .distinct()
                .collect(Collectors.toList())
        );
    }

    private Version getLatestVersionByKey(Map<Integer, List<Version>> groupByMajor, int key) {
        return groupByMajor.getOrDefault(key, emptyList()).stream()
            .max(Version::compareTo)
            .orElseThrow(() -> new IllegalStateException("Unexpected number of versions in collection"));
    }

    private Map<Integer, List<Version>> getReleasedMajorGroupedByMinor() {
        List<Version> currentMajorVersions = groupByMajor.get(currentVersion.getMajor());
        List<Version> previousMajorVersions = groupByMajor.get(currentVersion.getMajor() - 1);

        final Map<Integer, List<Version>> groupByMinor;
        if (currentMajorVersions.size() == 1) {
            // Current is an unreleased major: x.0.0 so we have to look for other unreleased versions in the previous major
            groupByMinor = previousMajorVersions.stream()
                .collect(Collectors.groupingBy(Version::getMinor, Collectors.toList()));
        } else {
            groupByMinor = currentMajorVersions.stream()
                .collect(Collectors.groupingBy(Version::getMinor, Collectors.toList()));
        }
        return groupByMinor;
    }

    public void compareToAuthoritative(List<Version> authoritativeReleasedVersions) {
        Set<Version> notReallyReleased = new HashSet<>(getReleased());
        notReallyReleased.removeAll(authoritativeReleasedVersions);
        if (notReallyReleased.isEmpty() == false) {
            throw new IllegalStateException(
                "out-of-date released versions" +
                    "\nFollowing versions are not really released, but the build thinks they are: " + notReallyReleased
            );
        }

        Set<Version> incorrectlyConsideredUnreleased = new HashSet<>(authoritativeReleasedVersions);
        incorrectlyConsideredUnreleased.retainAll(getUnreleased());
        if (incorrectlyConsideredUnreleased.isEmpty() == false) {
            throw new IllegalStateException(
                "out-of-date released versions" +
                    "\nBuild considers versions unreleased, " +
                    "but they are released according to an authoritative source: " + incorrectlyConsideredUnreleased +
                    "\nThe next versions probably needs to be added to Version.java (CURRENT doesn't count)."
            );
        }
    }

    private List<Version> getReleased() {
        List<Version> unreleased = getUnreleased();
        return groupByMajor.values().stream()
            .flatMap(Collection::stream)
            .filter(each -> unreleased.contains(each) == false)
            .sorted()
            .collect(Collectors.toList());
    }

    public List<Version> getIndexCompatible() {
        return unmodifiableList(
            groupByMajor.values().stream()
                .flatMap(Collection::stream)
                .filter(version -> version.equals(currentVersion) == false)
                .sorted()
                .collect(Collectors.toList())
        );
    }

    public List<Version> getWireCompatible() {
        Version lastPreviousMinor = getLatestVersionByKey(groupByMajor, currentVersion.getMajor() - 1);
        Version firstPreviousMinor = new Version(lastPreviousMinor.getMajor(), lastPreviousMinor.getMinor(), 0);
        return unmodifiableList(
            groupByMajor.values()
                .stream()
                .flatMap(Collection::stream)
                .filter(version -> version.compareTo(firstPreviousMinor) >= 0)
                .filter(version -> version.equals(currentVersion) == false)
                .sorted()
                .collect(Collectors.toList())
        );
    }

    public List<Version> getUnreleasedIndexCompatible() {
        List<Version> unreleasedIndexCompatible = new ArrayList<>(getIndexCompatible());
        unreleasedIndexCompatible.retainAll(getUnreleased());
        return unmodifiableList(unreleasedIndexCompatible);
    }

    public List<Version> getUnreleasedWireCompatible() {
        List<Version> unreleasedWireCompatible = new ArrayList<>(getWireCompatible());
        unreleasedWireCompatible.retainAll(getUnreleased());
        return unmodifiableList(unreleasedWireCompatible);
    }

}
