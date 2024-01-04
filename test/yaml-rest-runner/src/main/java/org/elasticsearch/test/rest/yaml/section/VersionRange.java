/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface VersionRange {

    boolean contains(Version currentVersion);

    VersionRange NEVER = v -> false;

    VersionRange ALWAYS = v -> true;

    class BoundedVersionRange implements VersionRange {

        final Version lower;
        final Version upper;

        public BoundedVersionRange(Version lower, Version upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public boolean contains(Version currentVersion) {
            return lower != null && upper != null && currentVersion.onOrAfter(lower) && currentVersion.onOrBefore(upper);
        }

        @Override
        public String toString() {
            return "[" + lower + " - " + upper + "]";
        }
    }

    static List<VersionRange> parseVersionRanges(String rawRanges) {
        if (rawRanges == null) {
            return Collections.singletonList(NEVER);
        }
        String[] ranges = rawRanges.split(",");
        List<VersionRange> versionRanges = new ArrayList<>();
        for (String rawRange : ranges) {
            if (rawRange.trim().equals("all")) {
                return Collections.singletonList(ALWAYS);
            }
            String[] skipVersions = rawRange.split("-", -1);
            if (skipVersions.length > 2) {
                throw new IllegalArgumentException("version range malformed: " + rawRanges);
            }

            String lower = skipVersions[0].trim();
            String upper = skipVersions[1].trim();
            VersionRange versionRange = new BoundedVersionRange(
                lower.isEmpty() ? VersionUtils.getFirstVersion() : Version.fromString(lower),
                upper.isEmpty() ? Version.CURRENT : Version.fromString(upper)
            );
            versionRanges.add(versionRange);
        }
        return versionRanges;
    }
}
