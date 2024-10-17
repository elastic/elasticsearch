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
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.VersionUtils;

import java.util.Set;

import static org.elasticsearch.test.LambdaMatchers.falseWith;
import static org.elasticsearch.test.LambdaMatchers.trueWith;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class VersionRangeTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testParseVersionNoLowerBound() {
        Version version = VersionUtils.randomVersion(random());
        String versionRangeString = Strings.format(" - %s", version);

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, contains(instanceOf(VersionRange.MinimumContainedInVersionRange.class)));
        var boundedVersionRange = (VersionRange.MinimumContainedInVersionRange) versionRange.get(0);
        assertThat(boundedVersionRange.lower, equalTo(VersionUtils.getFirstVersion()));
        assertThat(boundedVersionRange.upper, equalTo(version));
    }

    public void testParseVersionNoUpperBound() {
        Version version = VersionUtils.randomVersion(random());
        String versionRangeString = Strings.format("%s - ", version);

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, contains(instanceOf(VersionRange.MinimumContainedInVersionRange.class)));
        var boundedVersionRange = (VersionRange.MinimumContainedInVersionRange) versionRange.get(0);
        assertThat(boundedVersionRange.lower, equalTo(version));
        assertThat(boundedVersionRange.upper, equalTo(Version.CURRENT));
    }

    public void testParseAllVersions() {
        String versionRangeString = " all ";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, hasSize(1));
        assertThat(versionRange.get(0), equalTo(VersionRange.ALWAYS));
    }

    public void testParseMultipleRanges() {
        String versionRangeString = "6.0.0 - 6.1.0, 7.1.0 - 7.5.0";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(
            versionRange,
            contains(
                instanceOf(VersionRange.MinimumContainedInVersionRange.class),
                instanceOf(VersionRange.MinimumContainedInVersionRange.class)
            )
        );
        var range1 = (VersionRange.MinimumContainedInVersionRange) versionRange.get(0);
        var range2 = (VersionRange.MinimumContainedInVersionRange) versionRange.get(1);
        assertThat(range1.lower, equalTo(Version.fromString("6.0.0")));
        assertThat(range1.upper, equalTo(Version.fromString("6.1.0")));
        assertThat(range2.lower, equalTo(Version.fromString("7.1.0")));
        assertThat(range2.upper, equalTo(Version.fromString("7.5.0")));
    }

    public void testParseWithThreeDigitVersion() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> VersionRange.parseVersionRanges(" - 8.2.999"));
        assertThat(
            e.getMessage(),
            containsString("illegal revision version format - only one or two digit numbers are supported but found 999")
        );
    }

    public void testParseCurrent() {
        String versionRangeString = "   current ";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, hasSize(1));
        assertThat(versionRange.get(0), is(VersionRange.CURRENT));
    }

    public void testParseNonCurrent() {
        String versionRangeString = "   non_current ";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, hasSize(1));
        assertThat(versionRange.get(0), is(VersionRange.NON_CURRENT));
    }

    public void testParseMixed() {
        String versionRangeString = "   mixed ";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);
        assertThat(versionRange, notNullValue());
        assertThat(versionRange, hasSize(1));
        assertThat(versionRange.get(0), is(VersionRange.MIXED));
    }

    public void testMatchRange() {
        String versionRangeString = "6.0.0 - 6.1.0, 7.1.0 - 7.5.0";

        var versionRanges = VersionRange.parseVersionRanges(versionRangeString);

        var match1 = versionRanges.stream().filter(range -> range.test(Set.of("6.1.0"))).findFirst();
        var match2 = versionRanges.stream().filter(range -> range.test(Set.of("7.1.0"))).findFirst();
        var nonMatch1 = versionRanges.stream().filter(range -> range.test(Set.of("5.1.0"))).findFirst();
        var nonMatch2 = versionRanges.stream().filter(range -> range.test(Set.of("8.1.0"))).findFirst();

        assertThat(match1, isPresentWith(versionRanges.get(0)));
        assertThat(match2, isPresentWith(versionRanges.get(1)));

        assertThat(nonMatch1, isEmpty());
        assertThat(nonMatch2, isEmpty());
    }

    public void testMatchRangeMultipleNodeVersions() {
        String versionRangeString = "6.0.0 - 6.1.0, 7.1.0 - 7.5.0";

        var versionRanges = VersionRange.parseVersionRanges(versionRangeString);

        var match1 = versionRanges.stream().filter(range -> range.test(Set.of("6.1.0", "6.0.0"))).findFirst();
        var match2 = versionRanges.stream().filter(range -> range.test(Set.of("7.1.0", "8.0.0"))).findFirst();
        var nonMatch1 = versionRanges.stream().filter(range -> range.test(Set.of("5.1.0", "6.1.0"))).findFirst();
        var nonMatch2 = versionRanges.stream().filter(range -> range.test(Set.of("8.0.0", "8.1.0"))).findFirst();

        assertThat(match1, isPresentWith(versionRanges.get(0)));
        assertThat(match2, isPresentWith(versionRanges.get(1)));

        assertThat(nonMatch1, isEmpty());
        assertThat(nonMatch2, isEmpty());
    }

    public void testMatchAll() {
        String versionRangeString = "all";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);

        assertThat(versionRange.get(0), trueWith(Set.of(randomAlphaOfLength(10))));
    }

    public void testMatchCurrent() {
        String versionRangeString = "current";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);

        assertThat(versionRange.get(0), falseWith(Set.of(randomAlphaOfLength(10))));
        assertThat(versionRange.get(0), trueWith(Set.of(Build.current().version())));
        assertThat(versionRange.get(0), falseWith(Set.of(Build.current().version(), "8.10.0")));
    }

    public void testMatchNonCurrent() {
        String versionRangeString = "non_current";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);

        assertThat(versionRange.get(0), trueWith(Set.of(randomAlphaOfLength(10))));
        assertThat(versionRange.get(0), falseWith(Set.of(Build.current().version())));
        assertThat(versionRange.get(0), trueWith(Set.of(Build.current().version(), "8.10.0")));
    }

    public void testMatchMixed() {
        String versionRangeString = "mixed";

        var versionRange = VersionRange.parseVersionRanges(versionRangeString);

        assertThat(versionRange.get(0), falseWith(Set.of(randomAlphaOfLength(10))));
        assertThat(versionRange.get(0), falseWith(Set.of(Build.current().version())));
        assertThat(versionRange.get(0), trueWith(Set.of(Build.current().version(), "8.10.0")));
        assertThat(versionRange.get(0), trueWith(Set.of("8.9.0", "8.10.0")));
    }
}
