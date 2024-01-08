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

public record VersionRange(Version lower, Version upper) {

    public boolean contains(Version currentVersion) {
        return lower != null && upper != null && currentVersion.onOrAfter(lower) && currentVersion.onOrBefore(upper);
    }

    @Override
    public String toString() {
        return "[" + lower + " - " + upper + "]";
    }

    static List<VersionRange> parseVersionRanges(String rawRanges) {
        if (rawRanges == null) {
            return Collections.singletonList(new VersionRange(null, null));
        }
        String[] ranges = rawRanges.split(",");
        List<VersionRange> versionRanges = new ArrayList<>();
        for (String rawRange : ranges) {
            if (rawRange.trim().equals("all")) {
                return Collections.singletonList(new VersionRange(VersionUtils.getFirstVersion(), Version.CURRENT));
            }
            String[] skipVersions = rawRange.split("-", -1);
            if (skipVersions.length > 2) {
                throw new IllegalArgumentException("version range malformed: " + rawRanges);
            }

            String lower = skipVersions[0].trim();
            String upper = skipVersions[1].trim();
            VersionRange versionRange = new VersionRange(
                lower.isEmpty() ? VersionUtils.getFirstVersion() : Version.fromString(lower),
                upper.isEmpty() ? Version.CURRENT : Version.fromString(upper)
            );
            versionRanges.add(versionRange);
        }
        return versionRanges;
    }
}
