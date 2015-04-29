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
package org.elasticsearch.test.rest.section;

import com.google.common.collect.Lists;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.rest.support.Features;

import java.util.List;

/**
 * Represents a skip section that tells whether a specific test section or suite needs to be skipped
 * based on:
 * - the elasticsearch version the tests are running against
 * - a specific test feature required that might not be implemented yet by the runner
 */
public class SkipSection {

    public static final SkipSection EMPTY = new SkipSection();

    private final Version lowerVersion;
    private final Version upperVersion;
    private final List<String> features;
    private final String reason;
    
    private SkipSection() {
        this.lowerVersion = null;
        this.upperVersion = null;
        this.features = Lists.newArrayList();
        this.reason = null;
    }

    public SkipSection(String versionRange, List<String> features, String reason) {
        assert features != null;
        assert versionRange != null && features.isEmpty() || versionRange == null && features.isEmpty() == false;
        Version[] versions = parseVersionRange(versionRange);
        this.lowerVersion = versions[0];
        this.upperVersion = versions[1];
        this.features = features;
        this.reason = reason;
    }

    public Version getLowerVersion() {
        return lowerVersion;
    }
    
    public Version getUpperVersion() {
        return upperVersion;
    }

    public List<String> getFeatures() {
        return features;
    }

    public String getReason() {
        return reason;
    }

    public boolean skip(Version currentVersion) {
        if (isEmpty()) {
            return false;
        }
        if (isVersionCheck()) {
            return currentVersion.onOrAfter(lowerVersion) && currentVersion.onOrBefore(upperVersion);
        } else {
            return Features.areAllSupported(features) == false;
        }
    }

    public boolean isVersionCheck() {
        return features.isEmpty();
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }
    
    private Version[] parseVersionRange(String versionRange) {
        if (versionRange == null) {
            return new Version[] { null, null };
        }
        if (versionRange.trim().equals("all")) {
            return new Version[]{VersionUtils.getFirstVersion(), Version.CURRENT};
        }
        String[] skipVersions = versionRange.split("-");
        if (skipVersions.length > 2) {
            throw new IllegalArgumentException("version range malformed: " + versionRange);
        }

        String lower = skipVersions[0].trim();
        String upper = skipVersions[1].trim();
        return new Version[] {
            lower.isEmpty() ? VersionUtils.getFirstVersion() : Version.fromString(lower),
            upper.isEmpty() ? Version.CURRENT : Version.fromString(upper)
        };
    }
}
