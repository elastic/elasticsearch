/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

/**
 * A container for elasticsearch supported version information used in BWC testing.
 * <p>
 * Parse the Java source file containing the versions declarations and use the known rules to figure out which are all
 * the version the current one is wire and index compatible with.
 * On top of this, figure out which of these are unreleased and provide the branch they can be built from.
 * <p>
 * Note that in this context, currentVersion is the unreleased version this build operates on.
 * At any point in time there will be at least three such versions and potentially four in the case of a staged release.
 * <p>
 * <ul>
 * <li>the current version on the `main` branch</li>
 * <li>the staged next <b>minor</b> on the `M.N` branch</li>
 * <li>the unreleased <b>bugfix</b>, `M.N-1` branch</li>
 * <li>the unreleased <b>maintenance</b>, M-1.d.e ( d &gt; 0, e &gt; 0) on the `(M-1).d` branch</li>
 * </ul>
 * <p>
 * Each build is only concerned with versions before it, as those are the ones that need to be tested
 * for backwards compatibility. We never look forward, and don't add forward facing version number to branches of previous
 * version.
 * <p>
 * Each branch has a current version, and expected compatible versions are parsed from the server code's Version` class.
 * We can reliably figure out which the unreleased versions are due to the convention of always adding the next unreleased
 * version number to server in all branches when a version is released.
 * E.x when M.N.c is released M.N.c+1 is added to the Version class mentioned above in all the following branches:
 *  `M.N`, and `main` so we can reliably assume that the leafs of the version tree are unreleased.
 * This convention is enforced by checking the versions we consider to be unreleased against an
 * authoritative source (maven central).
 * We are then able to map the unreleased version to branches in git and Gradle projects that are capable of checking
 * out and building them, so we can include these in the testing plan as well.
 */
public class BwcVersions {

    private static final Pattern LINE_PATTERN = Pattern.compile(
        "\\W+public static final Version V_(\\d+)_(\\d+)_(\\d+)(_alpha\\d+|_beta\\d+|_rc\\d+)? .*?LUCENE_(\\d+)_(\\d+)_(\\d+)\\);"
    );
    private static final Version MINIMUM_WIRE_COMPATIBLE_VERSION = Version.fromString("7.17.0");

    private final VersionPair currentVersion;
    private final List<VersionPair> versions;
    private final Map<Version, UnreleasedVersionInfo> unreleased;

    public BwcVersions(List<String> versionLines) {
        this(versionLines, Version.fromString(VersionProperties.getElasticsearch()));
    }

    public BwcVersions(Version currentVersionProperty, List<VersionPair> allVersions) {
        if (allVersions.isEmpty()) {
            throw new IllegalArgumentException("Could not parse any versions");
        }

        this.versions = allVersions;
        this.currentVersion = allVersions.get(allVersions.size() - 1);
        assertCurrentVersionMatchesParsed(currentVersionProperty);

        this.unreleased = computeUnreleased();
    }

    // Visible for testing
    BwcVersions(List<String> versionLines, Version currentVersionProperty) {
        this(currentVersionProperty, parseVersionLines(versionLines));
    }

    private static List<VersionPair> parseVersionLines(List<String> versionLines) {
        return versionLines.stream()
            .map(LINE_PATTERN::matcher)
            .filter(Matcher::matches)
            .map(
                match -> new VersionPair(
                    new Version(Integer.parseInt(match.group(1)), Integer.parseInt(match.group(2)), Integer.parseInt(match.group(3))),
                    new Version(Integer.parseInt(match.group(5)), Integer.parseInt(match.group(6)), Integer.parseInt(match.group(7)))
                )
            )
            .sorted()
            .toList();
    }

    private void assertCurrentVersionMatchesParsed(Version currentVersionProperty) {
        if (currentVersionProperty.equals(currentVersion.elasticsearch) == false) {
            throw new IllegalStateException(
                "Parsed versions latest version does not match the one configured in build properties. "
                    + "Parsed latest version is "
                    + currentVersion.elasticsearch
                    + " but the build has "
                    + currentVersionProperty
            );
        }
    }

    /**
     * Returns info about the unreleased version, or {@code null} if the version is released.
     */
    public UnreleasedVersionInfo unreleasedInfo(Version version) {
        return unreleased.get(version);
    }

    public void forPreviousUnreleased(Consumer<UnreleasedVersionInfo> consumer) {
        filterSupportedVersions(
            getUnreleased().stream().filter(version -> version.equals(currentVersion.elasticsearch) == false).collect(Collectors.toList())
        ).stream().map(unreleased::get).forEach(consumer);
    }

    private String getBranchFor(Version version) {
        if (version.equals(currentVersion.elasticsearch)) {
            // Just assume the current branch is 'main'. It's actually not important, we never check out the current branch.
            return "main";
        } else {
            return version.getMajor() + "." + version.getMinor();
        }
    }

    private Map<Version, UnreleasedVersionInfo> computeUnreleased() {
        Set<VersionPair> unreleased = new TreeSet<>();
        // The current version is being worked, is always unreleased
        unreleased.add(currentVersion);
        // Recurse for all unreleased versions starting from the current version
        addUnreleased(unreleased, currentVersion, 0);

        // Grab the latest version from the previous major if necessary as well, this is going to be a maintenance release
        VersionPair maintenance = versions.stream()
            .filter(v -> v.elasticsearch.getMajor() == currentVersion.elasticsearch.getMajor() - 1)
            .sorted(Comparator.reverseOrder())
            .findFirst()
            .orElseThrow();
        // This is considered the maintenance release only if we haven't yet encountered it
        boolean hasMaintenanceRelease = unreleased.add(maintenance);

        List<VersionPair> unreleasedList = unreleased.stream().sorted(Comparator.reverseOrder()).toList();
        Map<Version, UnreleasedVersionInfo> result = new TreeMap<>();
        for (int i = 0; i < unreleasedList.size(); i++) {
            Version esVersion = unreleasedList.get(i).elasticsearch;
            // This is either a new minor or staged release
            if (currentVersion.elasticsearch.equals(esVersion)) {
                result.put(esVersion, new UnreleasedVersionInfo(esVersion, getBranchFor(esVersion), ":distribution"));
            } else if (esVersion.getRevision() == 0) {
                // If there are two upcoming unreleased minors then this one is the new minor
                if (unreleasedList.get(i + 1).elasticsearch.getRevision() == 0) {
                    result.put(esVersion, new UnreleasedVersionInfo(esVersion, getBranchFor(esVersion), ":distribution:bwc:minor"));
                } else {
                    result.put(esVersion, new UnreleasedVersionInfo(esVersion, getBranchFor(esVersion), ":distribution:bwc:staged"));
                }
            } else {
                // If this is the oldest unreleased version and we have a maintenance release
                if (i == unreleasedList.size() - 1 && hasMaintenanceRelease) {
                    result.put(esVersion, new UnreleasedVersionInfo(esVersion, getBranchFor(esVersion), ":distribution:bwc:maintenance"));
                } else {
                    result.put(esVersion, new UnreleasedVersionInfo(esVersion, getBranchFor(esVersion), ":distribution:bwc:bugfix"));
                }
            }
        }

        return Collections.unmodifiableMap(result);
    }

    public List<Version> getUnreleased() {
        return unreleased.keySet().stream().sorted().toList();
    }

    private void addUnreleased(Set<VersionPair> unreleased, VersionPair current, int index) {
        if (current.elasticsearch.getRevision() == 0) {
            // If the current version is a new minor, the next version is also unreleased
            VersionPair next = versions.get(versions.size() - (index + 2));
            unreleased.add(next);

            // Keep looking through versions until we find the end of unreleased versions
            addUnreleased(unreleased, next, index + 1);
        } else {
            unreleased.add(current);
        }
    }

    public void compareToAuthoritative(List<Version> authoritativeReleasedVersions) {
        Set<Version> notReallyReleased = new HashSet<>(getReleased());
        notReallyReleased.removeAll(authoritativeReleasedVersions);
        if (notReallyReleased.isEmpty() == false) {
            throw new IllegalStateException(
                "out-of-date released versions"
                    + "\nFollowing versions are not really released, but the build thinks they are: "
                    + notReallyReleased
            );
        }

        Set<Version> incorrectlyConsideredUnreleased = new HashSet<>(authoritativeReleasedVersions);
        incorrectlyConsideredUnreleased.retainAll(getUnreleased());
        if (incorrectlyConsideredUnreleased.isEmpty() == false) {
            throw new IllegalStateException(
                "out-of-date released versions"
                    + "\nBuild considers versions unreleased, "
                    + "but they are released according to an authoritative source: "
                    + incorrectlyConsideredUnreleased
                    + "\nThe next versions probably needs to be added to Version.java (CURRENT doesn't count)."
            );
        }
    }

    private List<Version> getReleased() {
        return versions.stream().map(v -> v.elasticsearch).filter(v -> unreleased.containsKey(v) == false).toList();
    }

    public List<Version> getIndexCompatible() {
        return filterSupportedVersions(
            versions.stream().filter(v -> v.lucene.getMajor() >= (currentVersion.lucene.getMajor() - 1)).map(v -> v.elasticsearch).toList()
        );
    }

    public void withIndexCompatible(BiConsumer<Version, String> versionAction) {
        getIndexCompatible().forEach(v -> versionAction.accept(v, "v" + v.toString()));
    }

    public void withIndexCompatible(Predicate<Version> filter, BiConsumer<Version, String> versionAction) {
        getIndexCompatible().stream().filter(filter).forEach(v -> versionAction.accept(v, "v" + v.toString()));
    }

    public List<Version> getWireCompatible() {
        return filterSupportedVersions(
            versions.stream().map(v -> v.elasticsearch).filter(v -> v.compareTo(MINIMUM_WIRE_COMPATIBLE_VERSION) >= 0).toList()
        );
    }

    public void withWireCompatible(BiConsumer<Version, String> versionAction) {
        getWireCompatible().forEach(v -> versionAction.accept(v, "v" + v.toString()));
    }

    public void withWireCompatible(Predicate<Version> filter, BiConsumer<Version, String> versionAction) {
        getWireCompatible().stream().filter(filter).forEach(v -> versionAction.accept(v, "v" + v.toString()));
    }

    private List<Version> filterSupportedVersions(List<Version> wireCompat) {
        Predicate<Version> supported = v -> true;
        if (Architecture.current() == Architecture.AARCH64) {
            final String version;
            if (ElasticsearchDistribution.CURRENT_PLATFORM.equals(ElasticsearchDistribution.Platform.DARWIN)) {
                version = "7.16.0";
            } else {
                version = "7.12.0"; // linux shipped earlier for aarch64
            }
            supported = v -> v.onOrAfter(version);
        }
        return wireCompat.stream().filter(supported).collect(Collectors.toList());
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

    public Version getMinimumWireCompatibleVersion() {
        return MINIMUM_WIRE_COMPATIBLE_VERSION;
    }

    public record UnreleasedVersionInfo(Version version, String branch, String gradleProjectPath) {}

    public record VersionPair(Version elasticsearch, Version lucene) implements Comparable<VersionPair> {

        @Override
        public int compareTo(VersionPair o) {
            // For ordering purposes, sort by Elasticsearch version
            return this.elasticsearch.compareTo(o.elasticsearch);
        }
    }
}
