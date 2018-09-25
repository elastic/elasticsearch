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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VersionCollection {

    private final Pattern LINE_PATTERN = Pattern.compile(
        "\\W+public static final Version V_(\\d+)_(\\d+)_(\\d+)(_alpha\\d+|_beta\\d+|_rc\\d+)? .*"
    );
    private final SortedSet<Version> versions = new TreeSet<>();
    private Version currentVersion;

    public class UnreleasedVersionDescription {
        private final Version version;
        private final Integer index;
        private final String branch;

        public UnreleasedVersionDescription(Integer index, Version version, String branch) {
            this.version = version;
            this.index = index;
            this.branch = branch;
        }

        public Version getVersion() {
            return version;
        }

        public Integer getIndex() {
            return index;
        }

        public String getBranch() {
            return branch;
        }
    }

    protected VersionCollection(List<String> versionLines, Version currentVersionProperty) {
        versionLines.stream()
            .map(LINE_PATTERN::matcher)
            .filter(Matcher::matches)
            .map(match -> new Version(
                Integer.parseInt(match.group(1)),
                Integer.parseInt(match.group(2)),
                Integer.parseInt(match.group(3)),
                (match.group(4) == null ? "" : match.group(4)).replace('_', '-'),
                false
            ))
            .filter(((Predicate<? super Version>) Version::isSnapshot).negate())
            .filter(version -> version.getSuffix().isEmpty() || version.numbersOnly().equals(currentVersionProperty.numbersOnly()))
            .forEach(version -> {
                if (versions.add(version) == false) {
                    throw new IllegalStateException("Found duplicate while parsing all versions: " + version);
                }
            });
        if (versions.isEmpty()) {
            throw new IllegalArgumentException("Could not parse any versions");
        }

        currentVersion = versions.last();
        if (currentVersionProperty.equals(currentVersion) == false) {
            throw new IllegalStateException(
                "Parsed versions latest version does not match the one configured in build properties. " +
                    "Parsed latest version is " + currentVersion + " but the build has " +
                    currentVersionProperty
            );
        }

        SortedSet<Version> versionsMoreThanTowMajorsAgo = versions
            .headSet(new Version(currentVersion.getMajor() - 1, 0, 0, "", false));
        if (versionsMoreThanTowMajorsAgo.isEmpty() == false) {
            throw new IllegalStateException(
                "Found unexpected parsed versions, more than two majors ago: " + versionsMoreThanTowMajorsAgo
            );
        }

        List<Integer> distinctMajors = versions.stream()
            .map(Version::getMajor)
            .distinct()
            .collect(Collectors.toList());
        if (distinctMajors.size() != 2) {
            throw new IllegalStateException("Expected to have exactly 2 parsed major bout found: " + distinctMajors);
        }

        getUnreleased().forEach(version -> {
            versions.remove(version);
            versions.add(
                new Version(
                    version.getMajor(), version.getMinor(), version.getRevision(), version.getSuffix(), true
                )
            );
        });
    }

    public void forEachUnreleased(Consumer<UnreleasedVersionDescription> consumer) {
        List<Version> unreleasedVersionsList = new ArrayList<>(getUnreleased());
        IntStream.range(0, unreleasedVersionsList.size())
            .filter(index -> unreleasedVersionsList.get(index).equals(currentVersion) == false)
            .forEach(index ->
            consumer.accept(new UnreleasedVersionDescription(
                index,
                unreleasedVersionsList.get(index),
                getBranchFor(unreleasedVersionsList.get(index))
            ))
        );
    }

    protected String getBranchFor(Version unreleasedVersion) {
        SortedSet<Version> unreleased = getUnreleased();
        if (unreleased.contains(unreleasedVersion)) {
            if (unreleasedVersion.getMinor() == 0 && unreleasedVersion.getRevision() == 0) {
                return "master";
            }
            if (unreleasedVersion.getRevision() != 0) {
                return unreleasedVersion.getMajor() + "." + unreleasedVersion.getMinor();
            }
            Optional<Version> nextUnreleased = getVersionAfter(unreleased, unreleasedVersion);
            if (nextUnreleased.isPresent() && nextUnreleased.get().getMajor() == unreleasedVersion.getMajor()) {
                return unreleasedVersion.getMajor() + "." + unreleasedVersion.getMinor();
            }
            return unreleasedVersion.getMajor() + ".x";
        } else {
            throw new IllegalStateException(
                "Can't get branch of released version: " + unreleasedVersion + " unreleased versions are: " + unreleased
            );
        }
    }

    public SortedSet<Version> getUnreleased() {
        SortedSet<Version> unreleased = new TreeSet<>();
        // The current version is being worked, is always unreleased
        unreleased.add(currentVersion);
        // The tip of the previous major will also be unreleased
        unreleased.add(getLatestBugfixBeforeMajor(currentVersion));
        // Initial releases of minors and majors will have more unreleased versions before them
        if (currentVersion.getRevision() == 0) {
            Version greatestPreviousMinor = getLatestBugfixBeforeMinor(currentVersion);
            unreleased.add(greatestPreviousMinor);
            Version greatestMinorBehindByTwo = getLatestBugfixBeforeMinor(greatestPreviousMinor);
            if (greatestPreviousMinor.getRevision() == 0) {
                // So we have x.y.0 and x.(y-1).0 with nothing in-between. This happens when x.y.0 is feature freeze.
                // We'll add x.y.1 as soon as x.y.0 releases so this is correct.
                unreleased.add(greatestMinorBehindByTwo);
            }
        }
        if (unreleased.size() > 4) {
            throw new IllegalStateException("Expected no more than 4 unreleased version but found: " + unreleased);
        }
        return unreleased;
    }

    private Version getLatestBugfixBeforeMajor(Version version) {
        return getVersionBefore(new Version(version.getMajor(), 0, 0));
    }

    private Version getLatestBugfixBeforeMinor(Version version) {
        return getVersionBefore(new Version(version.getMajor(), version.getMinor(), 0));
    }

    public VersionCollection(List<String> versionLines) {
        this(versionLines, VersionProperties.getElasticsearch());
    }

    public SortedSet<Version> getIndexCompatible() {
        return Collections.unmodifiableSortedSet(
            versions
                .tailSet(new Version(currentVersion.getMajor() - 1, 0, 0))
                .headSet(currentVersion)
        );
    }

    public SortedSet<Version> getWireCompatible() {
        final Version lastPrevMajor = getLatestBugfixBeforeMajor(currentVersion);
        return Collections.unmodifiableSortedSet(
            versions
                .tailSet(new Version(lastPrevMajor.getMajor(), lastPrevMajor.getMinor(), 0))
                .headSet(currentVersion)
        );
    }

    public SortedSet<Version> getSnapshotsIndexCompatible() {
        SortedSet<Version> unreleasedIndexCompatible = new TreeSet<>(getIndexCompatible());
        unreleasedIndexCompatible.retainAll(getUnreleased());
        return Collections.unmodifiableSortedSet(unreleasedIndexCompatible);
    }

    public SortedSet<Version> getSnapshotsWireCompatible() {
        SortedSet<Version> unreleasedWireCompatible = new TreeSet<>(getWireCompatible());
        unreleasedWireCompatible.retainAll(getUnreleased());
        return Collections.unmodifiableSortedSet(unreleasedWireCompatible);
    }

    private Version getVersionBefore(Version e) {
        return versions
            .headSet(e).last();
    }

    private Optional<Version> getVersionAfter(SortedSet<Version> versions, Version version) {
        Iterator<Version> iterator = versions
            .tailSet(version).iterator();
        if (iterator.hasNext()) {
            iterator.next();
            if (iterator.hasNext()) {
                return Optional.of(iterator.next());
            }
        }
        return Optional.empty();
    }

}
