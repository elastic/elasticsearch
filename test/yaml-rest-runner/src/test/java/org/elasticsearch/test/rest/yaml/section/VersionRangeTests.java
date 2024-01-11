/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.VersionUtils;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class VersionRangeTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testParseVersionNoLowerBound() {
        Version version = VersionUtils.randomVersion(random());
        String versionRangeString = Strings.format(" - %s", version);

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, hasSize(1));
        assertThat(versionRange.get(0).lower(), equalTo(VersionUtils.getFirstVersion()));
        assertThat(versionRange.get(0).upper(), equalTo(version));
    }

    public void testParseVersionNoUpperBound() {
        Version version = VersionUtils.randomVersion(random());
        String versionRangeString = Strings.format("%s - ", version);

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, hasSize(1));
        assertThat(versionRange.get(0).lower(), equalTo(version));
        assertThat(versionRange.get(0).upper(), equalTo(Version.CURRENT));
    }

    public void testParseAllVersions() {
        String versionRangeString = " all ";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, hasSize(1));
        assertThat(versionRange.get(0).lower(), equalTo(VersionUtils.getFirstVersion()));
        assertThat(versionRange.get(0).upper(), equalTo(Version.CURRENT));
    }

    public void testParseMultipleRanges() {
        String versionRangeString = "6.0.0 - 6.1.0, 7.1.0 - 7.5.0";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, hasSize(2));
        assertThat(versionRange.get(0).lower(), equalTo(Version.fromString("6.0.0")));
        assertThat(versionRange.get(0).upper(), equalTo(Version.fromString("6.1.0")));
        assertThat(versionRange.get(1).lower(), equalTo(Version.fromString("7.1.0")));
        assertThat(versionRange.get(1).upper(), equalTo(Version.fromString("7.5.0")));
    }

    public void testParseWithThreeDigitVersion() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> VersionRange.parseVersionRanges(" - 8.2.999"));
        assertThat(
            e.getMessage(),
            containsString("illegal revision version format - only one or two digit numbers are supported but found 999")
        );
    }
}
