/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface VersionRange {

    boolean matches(Set<String> nodesVersions);

    VersionRange NEVER = v -> false;

    VersionRange ALWAYS = v -> true;

    VersionRange CURRENT = versions -> versions.size() == 1 && versions.contains(Build.current().version());

    VersionRange NON_CURRENT = versions -> CURRENT.matches(versions) == false;

    VersionRange MIXED = versions -> versions.size() > 1;

    class MinimumContainedInVersionRange implements VersionRange {

        final Version lower;
        final Version upper;

        public MinimumContainedInVersionRange(Version lower, Version upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public boolean matches(Set<String> nodesVersions) {
            // Try to extract the minimum node version
            var minimumNodeVersion = nodesVersions.stream()
                .map(ESRestTestCase::parseLegacyVersion)
                .flatMap(Optional::stream)
                .min(VersionId::compareTo)
                .orElseThrow(() -> new IllegalArgumentException("Checks against a version range require semantic version format (x.y.z)"));
            return minimumNodeVersion.onOrAfter(lower) && minimumNodeVersion.onOrBefore(upper);
        }

        @Override
        public String toString() {
            return "[" + lower + " - " + upper + "]";
        }
    }

    static List<VersionRange> parseVersionRanges(String rawRanges) {
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
        List<VersionRange> versionRanges = new ArrayList<>();
        for (String rawRange : ranges) {
            String[] skipVersions = rawRange.split("-", -1);
            if (skipVersions.length > 2) {
                throw new IllegalArgumentException("version range malformed: " + rawRanges);
            }

            String lower = skipVersions[0].trim();
            String upper = skipVersions[1].trim();
            VersionRange versionRange = new MinimumContainedInVersionRange(
                lower.isEmpty() ? VersionUtils.getFirstVersion() : Version.fromString(lower),
                upper.isEmpty() ? Version.CURRENT : Version.fromString(upper)
            );
            versionRanges.add(versionRange);
        }
        return versionRanges;
    }
}
