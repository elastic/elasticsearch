/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

public class VersionTests extends ESTestCase {

    public void testVersionComparison() {
        Version V_7_2_0 = Version.fromString("7.2.0");
        Version V_8_0_0 = Version.fromString("8.0.0");
        assertThat(V_7_2_0.before(V_8_0_0), is(true));
        assertThat(V_7_2_0.before(V_7_2_0), is(false));
        assertThat(V_8_0_0.before(V_7_2_0), is(false));

        assertThat(V_7_2_0.onOrBefore(V_8_0_0), is(true));
        assertThat(V_7_2_0.onOrBefore(V_7_2_0), is(true));
        assertThat(V_8_0_0.onOrBefore(V_7_2_0), is(false));

        assertThat(V_7_2_0.after(V_8_0_0), is(false));
        assertThat(V_7_2_0.after(V_7_2_0), is(false));
        assertThat(V_8_0_0.after(V_7_2_0), is(true));

        assertThat(V_7_2_0.onOrAfter(V_8_0_0), is(false));
        assertThat(V_7_2_0.onOrAfter(V_7_2_0), is(true));
        assertThat(V_8_0_0.onOrAfter(V_7_2_0), is(true));

        assertThat(V_7_2_0, is(lessThan(V_8_0_0)));
        assertThat(V_7_2_0.compareTo(V_7_2_0), is(0));
        assertThat(V_8_0_0, is(greaterThan(V_7_2_0)));
    }

    public void testMin() {
        assertEquals(VersionUtils.getPreviousVersion(), Version.min(Version.CURRENT, VersionUtils.getPreviousVersion()));
        assertEquals(Version.fromString("1.0.1"), Version.min(Version.fromString("1.0.1"), Version.CURRENT));
        Version version = VersionUtils.randomVersion(random());
        Version version1 = VersionUtils.randomVersion(random());
        if (version.id <= version1.id) {
            assertEquals(version, Version.min(version1, version));
        } else {
            assertEquals(version1, Version.min(version1, version));
        }
    }

    public void testMax() {
        assertEquals(Version.CURRENT, Version.max(Version.CURRENT, VersionUtils.getPreviousVersion()));
        assertEquals(Version.CURRENT, Version.max(Version.fromString("1.0.1"), Version.CURRENT));
        Version version = VersionUtils.randomVersion(random());
        Version version1 = VersionUtils.randomVersion(random());
        if (version.id >= version1.id) {
            assertEquals(version, Version.max(version1, version));
        } else {
            assertEquals(version1, Version.max(version1, version));
        }
    }

    public void testVersionConstantPresent() {
        assertThat(Version.CURRENT, sameInstance(Version.fromId(Version.CURRENT.id)));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion(random());
            assertThat(version, sameInstance(Version.fromId(version.id)));
        }
    }

    public void testCURRENTIsLatest() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion(random());
            if (version != Version.CURRENT) {
                assertThat(
                    "Version: " + version + " should be before: " + Version.CURRENT + " but wasn't",
                    version.before(Version.CURRENT),
                    is(true)
                );
            }
        }
    }

    public void testVersionFromString() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion(random());
            assertThat(Version.fromString(version.toString()), sameInstance(version));
        }
    }

    public void testTooLongVersionFromString() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> Version.fromString("1.0.0.1.3"));
        assertThat(e.getMessage(), containsString("needs to contain major, minor, and revision"));
    }

    public void testTooShortVersionFromString() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> Version.fromString("1.0"));
        assertThat(e.getMessage(), containsString("needs to contain major, minor, and revision"));
    }

    public void testWrongVersionFromString() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> Version.fromString("WRONG.VERSION"));
        assertThat(e.getMessage(), containsString("needs to contain major, minor, and revision"));
    }

    public void testMinCompatVersion() {
        Version major = Version.fromString("2.0.0");
        assertThat(Version.fromString("2.0.0").minimumCompatibilityVersion(), equalTo(major));
        assertThat(Version.fromString("2.2.0").minimumCompatibilityVersion(), equalTo(major));
        assertThat(Version.fromString("2.3.0").minimumCompatibilityVersion(), equalTo(major));

        Version major5x = Version.fromString("5.0.0");
        assertThat(Version.fromString("5.0.0").minimumCompatibilityVersion(), equalTo(major5x));
        assertThat(Version.fromString("5.2.0").minimumCompatibilityVersion(), equalTo(major5x));
        assertThat(Version.fromString("5.3.0").minimumCompatibilityVersion(), equalTo(major5x));

        Version major56x = Version.fromString("5.6.0");
        assertThat(Version.fromString("6.4.0").minimumCompatibilityVersion(), equalTo(major56x));
        assertThat(Version.fromString("6.3.1").minimumCompatibilityVersion(), equalTo(major56x));

        // from 7.0 on we are supporting the latest minor of the previous major... this might fail once we add a new version ie. 5.x is
        // released since we need to bump the supported minor in Version#minimumCompatibilityVersion()
        Version lastVersion = Version.fromString("6.8.0"); // TODO: remove this once min compat version is a constant instead of method
        assertEquals(lastVersion.major, Version.V_7_0_0.minimumCompatibilityVersion().major);
        assertEquals(
            "did you miss to bump the minor in Version#minimumCompatibilityVersion()",
            lastVersion.minor,
            Version.V_7_0_0.minimumCompatibilityVersion().minor
        );
        assertEquals(0, Version.V_7_0_0.minimumCompatibilityVersion().revision);
    }

    public void testToString() {
        assertEquals("5.0.0", Version.fromId(5000099).toString());
        assertEquals("2.3.0", Version.fromString("2.3.0").toString());
        assertEquals("0.90.0", Version.fromString("0.90.0").toString());
        assertEquals("1.0.0", Version.fromString("1.0.0").toString());
        assertEquals("2.0.0", Version.fromString("2.0.0").toString());
        assertEquals("5.0.0", Version.fromString("5.0.0").toString());
    }

    public void testParseVersion() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Version version = randomVersion(random());
            if (random().nextBoolean()) {
                version = new Version(version.id);
            }
            Version parsedVersion = Version.fromString(version.toString());
            assertEquals(version, parsedVersion);
        }

        expectThrows(IllegalArgumentException.class, () -> { Version.fromString("5.0.0-alph2"); });
        assertSame(Version.CURRENT, Version.fromString(Version.CURRENT.toString()));

        assertEquals(Version.fromString("2.0.0-SNAPSHOT"), Version.fromId(2000099));

        expectThrows(IllegalArgumentException.class, () -> { Version.fromString("5.0.0-SNAPSHOT"); });
    }

    public void testAllVersionsMatchId() throws Exception {
        final Set<Version> versions = new HashSet<>(VersionUtils.allVersions());
        Map<String, Version> maxBranchVersions = new HashMap<>();
        for (java.lang.reflect.Field field : Version.class.getFields()) {
            if (field.getName().matches("_ID")) {
                assertTrue(field.getName() + " should be static", Modifier.isStatic(field.getModifiers()));
                assertTrue(field.getName() + " should be final", Modifier.isFinal(field.getModifiers()));
                int versionId = (Integer) field.get(Version.class);

                String constantName = field.getName().substring(0, field.getName().indexOf("_ID"));
                java.lang.reflect.Field versionConstant = Version.class.getField(constantName);
                assertTrue(constantName + " should be static", Modifier.isStatic(versionConstant.getModifiers()));
                assertTrue(constantName + " should be final", Modifier.isFinal(versionConstant.getModifiers()));

                Version v = (Version) versionConstant.get(null);
                logger.debug("Checking {}", v);
                assertTrue(versions.contains(v));
                assertEquals("Version id " + field.getName() + " does not point to " + constantName, v, Version.fromId(versionId));
                assertEquals("Version " + constantName + " does not have correct id", versionId, v.id);
                String number = v.toString();
                assertEquals("V_" + number.replace('.', '_'), constantName);
            }
        }
    }

    public void testIsCompatible() {
        assertTrue(isCompatible(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()));
        assertFalse(isCompatible(Version.V_7_0_0, Version.V_8_0_0));
        assertTrue(isCompatible(Version.fromString("6.8.0"), Version.fromString("7.0.0")));
        assertFalse(isCompatible(Version.fromId(2000099), Version.V_7_0_0));
        assertFalse(isCompatible(Version.fromId(2000099), Version.fromString("6.5.0")));

        final Version currentMajorVersion = Version.fromId(Version.CURRENT.major * 1000000 + 99);
        final Version currentOrNextMajorVersion;
        if (Version.CURRENT.minor > 0) {
            currentOrNextMajorVersion = Version.fromId((Version.CURRENT.major + 1) * 1000000 + 99);
        } else {
            currentOrNextMajorVersion = currentMajorVersion;
        }
        final Version lastMinorFromPreviousMajor = VersionUtils.allVersions()
            .stream()
            .filter(v -> v.major == currentOrNextMajorVersion.major - 1)
            .max(Version::compareTo)
            .orElseThrow(() -> new IllegalStateException("expected previous minor version for [" + currentOrNextMajorVersion + "]"));
        final Version previousMinorVersion = VersionUtils.getPreviousMinorVersion();

        boolean isCompatible = previousMinorVersion.major == currentOrNextMajorVersion.major
            || previousMinorVersion.minor == lastMinorFromPreviousMajor.minor;

        final String message = String.format(
            Locale.ROOT,
            "[%s] should %s be compatible with [%s]",
            previousMinorVersion,
            isCompatible ? "" : " not",
            currentOrNextMajorVersion
        );
        assertThat(message, isCompatible(VersionUtils.getPreviousMinorVersion(), currentOrNextMajorVersion), equalTo(isCompatible));

        assertFalse(isCompatible(Version.fromId(5000099), Version.fromString("6.0.0")));
        assertFalse(isCompatible(Version.fromId(5000099), Version.fromString("7.0.0")));

        Version a = randomVersion(random());
        Version b = randomVersion(random());
        assertThat(a.isCompatible(b), equalTo(b.isCompatible(a)));
    }

    public boolean isCompatible(Version left, Version right) {
        boolean result = left.isCompatible(right);
        assert result == right.isCompatible(left);
        return result;
    }

    public void testIllegalMinorAndPatchNumbers() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Version.fromString("8.2.999"));
        assertThat(
            e.getMessage(),
            containsString("illegal revision version format - only one or two digit numbers are supported but found 999")
        );

        e = expectThrows(IllegalArgumentException.class, () -> Version.fromString("8.888.99"));
        assertThat(
            e.getMessage(),
            containsString("illegal minor version format - only one or two digit numbers are supported but found 888")
        );
    }

}
