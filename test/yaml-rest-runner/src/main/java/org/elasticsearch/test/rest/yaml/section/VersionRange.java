/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

class VersionRange {

    private VersionRange() {}

    static final Predicate<Set<String>> NEVER = Predicates.never();

    static final Predicate<Set<String>> ALWAYS = Predicates.always();

    static final Predicate<Set<String>> CURRENT = versions -> versions.size() == 1 && versions.contains(Build.current().version());

    static final Predicate<Set<String>> NON_CURRENT = CURRENT.negate();

    static final Predicate<Set<String>> MIXED = versions -> versions.size() > 1;

    /**
     * Encapsulates the logic to test a set of node versions ({@code Predicate<Set<String>>}) against a version range.
     * The minimum node version must be included in the range between {@link MinimumContainedInVersionRange#lower} and
     * {@link MinimumContainedInVersionRange#upper}, both ends included.
     */
    static class MinimumContainedInVersionRange implements Predicate<Set<String>> {
        final Version lower;
        final Version upper;

        private MinimumContainedInVersionRange(Version lower, Version upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean test(Set<String> nodesVersions) {
            // Try to extract the minimum node version
            var minimumNodeVersion = nodesVersions.stream()
                .map(ESRestTestCase::parseLegacyVersion)
                .flatMap(Optional::stream)
                .min(Comparator.naturalOrder())
                .orElseThrow(() -> new IllegalArgumentException("Checks against a version range require semantic version format (x.y.z)"));
            return minimumNodeVersion.onOrAfter(lower) && minimumNodeVersion.onOrBefore(upper);
        }

        @Override
        public String toString() {
            return "MinimumContainedInVersionRange{lower=" + lower + ", upper=" + upper + '}';
        }
    }

    static List<Predicate<Set<String>>> parseVersionRanges(String rawRanges) {
        if (rawRanges == null) {
            return List.of(NEVER);
        }
        if (rawRanges.trim().equals("all")) {
            return List.of(ALWAYS);
        }
        if (rawRanges.trim().equals("current")) {
            return List.of(CURRENT);
        }
        if (rawRanges.trim().equals("non_current")) {
            return List.of(NON_CURRENT);
        }
        if (rawRanges.trim().equals("mixed")) {
            return List.of(MIXED);
        }

        String[] ranges = rawRanges.split(",");
        List<Predicate<Set<String>>> versionRanges = new ArrayList<>();
        for (String rawRange : ranges) {
            String[] skipVersions = rawRange.split("-", -1);
            if (skipVersions.length > 2) {
                throw new IllegalArgumentException("version range malformed: " + rawRanges);
            }

            String lower = skipVersions[0].trim();
            String upper = skipVersions[1].trim();
            var minimumContainedInVersionRange = new MinimumContainedInVersionRange(
                lower.isEmpty() ? VersionUtils.getFirstVersion() : Version.fromString(lower),
                upper.isEmpty() ? Version.CURRENT : Version.fromString(upper)
            );
            versionRanges.add(minimumContainedInVersionRange);
        }
        return versionRanges;
    }
}
