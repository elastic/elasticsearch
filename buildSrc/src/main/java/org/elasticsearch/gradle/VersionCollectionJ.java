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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class VersionCollectionJ {

    private final Pattern LINE_PATTERN = Pattern.compile(
        "\\W+public static final Version V_(\\d+)_(\\d+)_(\\d+)(_alpha\\d+|_beta\\d+|_rc\\d+)? .*"
    );
    private final SortedSet<Version> versions = new TreeSet<>();
    private Version currentVersion;

    protected VersionCollectionJ(List<String> versionLines, Version currentVersionProperty) {
        versionLines.stream()
            .map(LINE_PATTERN::matcher)
            .filter(Matcher::matches)
            .map(match ->  new Version(
                Integer.parseInt(match.group(1)),
                Integer.parseInt(match.group(2)),
                Integer.parseInt(match.group(3)),
                (match.group(4) == null ?  "" : match.group(4)).replace('_', '-'),
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
            .headSet(new Version(currentVersion.getMajor() - 1, 0,0, "", false));
        if (versionsMoreThanTowMajorsAgo.isEmpty() == false) {
            throw new IllegalStateException(
                "Found unexpected parsed versions, more than two majors ago: " + versionsMoreThanTowMajorsAgo
            );
        }

        List<Integer> distinctMajors = versions.stream()
            .map(Version::getMajor)
            .distinct()
            .collect(Collectors.toList());
        if (distinctMajors.size() !=  2) {
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
                unreleased.add(greatestMinorBehindByTwo);
                unreleased.add(greatestMinorBehindByTwo);
            }
        }
        return unreleased;
    }

    private Version getLatestBugfixBeforeMajor(Version version) {
        return getVersionBefore(new Version(version.getMajor(), 0, 0));
    }

    private Version getLatestBugfixBeforeMinor(Version version) {
        return getVersionBefore(new Version(version.getMajor(), version.getMinor(), 0));
    }

    public VersionCollectionJ(List<String> versionLines) {
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

    private Version getVersionBefore(Version e) {
        return versions
                .headSet(e).last();
    }

    private Version getVersionAfter(Version version) {
        Iterator<Version> iterator = versions
            .tailSet(version).iterator();
        if (iterator.hasNext()) {
            iterator.next();
            if (iterator.hasNext()) {
                return iterator.next();
            }
        }
        throw new NoSuchElementException(version.toString());
    }

}
