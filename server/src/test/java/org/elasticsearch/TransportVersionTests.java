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
import org.elasticsearch.test.TransportVersionUtils;

import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

public class TransportVersionTests extends ESTestCase {

    public void testVersionComparison() {
        TransportVersion V_8_2_0 = TransportVersions.V_8_2_0;
        TransportVersion V_8_16_0 = TransportVersions.V_8_16_0;
        assertThat(V_8_2_0.before(V_8_16_0), is(true));
        assertThat(V_8_2_0.before(V_8_2_0), is(false));
        assertThat(V_8_16_0.before(V_8_2_0), is(false));

        assertThat(V_8_2_0.onOrBefore(V_8_16_0), is(true));
        assertThat(V_8_2_0.onOrBefore(V_8_2_0), is(true));
        assertThat(V_8_16_0.onOrBefore(V_8_2_0), is(false));

        assertThat(V_8_2_0.after(V_8_16_0), is(false));
        assertThat(V_8_2_0.after(V_8_2_0), is(false));
        assertThat(V_8_16_0.after(V_8_2_0), is(true));

        assertThat(V_8_2_0.onOrAfter(V_8_16_0), is(false));
        assertThat(V_8_2_0.onOrAfter(V_8_2_0), is(true));
        assertThat(V_8_16_0.onOrAfter(V_8_2_0), is(true));

        assertThat(V_8_2_0, is(lessThan(V_8_16_0)));
        assertThat(V_8_2_0.compareTo(V_8_2_0), is(0));
        assertThat(V_8_16_0, is(greaterThan(V_8_2_0)));
    }

    public static class CorrectFakeVersion {
        public static final TransportVersion V_0_00_01 = new TransportVersion(199);
        public static final TransportVersion V_0_000_002 = new TransportVersion(2);
        public static final TransportVersion V_0_000_003 = new TransportVersion(3);
        public static final TransportVersion V_0_000_004 = new TransportVersion(4);
    }

    public static class DuplicatedIdFakeVersion {
        public static final TransportVersion V_0_000_001 = new TransportVersion(1);
        public static final TransportVersion V_0_000_002 = new TransportVersion(2);
        public static final TransportVersion V_0_000_003 = new TransportVersion(2);
    }

    public void testStaticTransportVersionChecks() {
        assertThat(
            TransportVersions.collectAllVersionIdsDefinedInClass(CorrectFakeVersion.class),
            contains(
                CorrectFakeVersion.V_0_000_002,
                CorrectFakeVersion.V_0_000_003,
                CorrectFakeVersion.V_0_000_004,
                CorrectFakeVersion.V_0_00_01
            )
        );
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> TransportVersions.collectAllVersionIdsDefinedInClass(DuplicatedIdFakeVersion.class)
        );
        assertThat(e.getMessage(), containsString("have the same version number"));
    }

    private static String padNumber(String number) {
        return number.length() == 1 ? "0" + number : number;
    }

    public void testDefinedConstants() throws IllegalAccessException {
        Pattern historicalVersion = Pattern.compile("^V_(\\d{1,2})_(\\d{1,2})_(\\d{1,2})$");
        Pattern transportVersion = Pattern.compile("^V_(\\d+)_(\\d{3})_(\\d{3})$");
        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE", "MINIMUM_CCS_VERSION");

        for (java.lang.reflect.Field field : TransportVersion.class.getFields()) {
            if (field.getType() == TransportVersion.class && ignore.contains(field.getName()) == false) {

                // check the field modifiers
                assertEquals(
                    "Field " + field.getName() + " should be public static final",
                    Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL,
                    field.getModifiers()
                );

                Matcher matcher = historicalVersion.matcher(field.getName());
                if (matcher.matches()) {
                    // old-style version constant
                    String idString = matcher.group(1) + padNumber(matcher.group(2)) + padNumber(matcher.group(3)) + "99";
                    assertEquals(
                        "Field " + field.getName() + " does not have expected id " + idString,
                        idString,
                        field.get(null).toString()
                    );
                } else if ((matcher = transportVersion.matcher(field.getName())).matches()) {
                    String idString = matcher.group(1) + matcher.group(2) + matcher.group(3);
                    assertEquals(
                        "Field " + field.getName() + " does not have expected id " + idString,
                        idString,
                        field.get(null).toString()
                    );
                } else {
                    fail("Field " + field.getName() + " does not have expected format");
                }
            }
        }
    }

    public void testMin() {
        assertEquals(
            TransportVersionUtils.getPreviousVersion(),
            TransportVersion.min(TransportVersion.current(), TransportVersionUtils.getPreviousVersion())
        );
        assertEquals(
            TransportVersion.fromId(1_01_01_99),
            TransportVersion.min(TransportVersion.fromId(1_01_01_99), TransportVersion.current())
        );
        TransportVersion version = TransportVersionUtils.randomVersion();
        TransportVersion version1 = TransportVersionUtils.randomVersion();
        if (version.id() <= version1.id()) {
            assertEquals(version, TransportVersion.min(version1, version));
        } else {
            assertEquals(version1, TransportVersion.min(version1, version));
        }
    }

    public void testMax() {
        assertEquals(
            TransportVersion.current(),
            TransportVersion.max(TransportVersion.current(), TransportVersionUtils.getPreviousVersion())
        );
        assertEquals(TransportVersion.current(), TransportVersion.max(TransportVersion.fromId(1_01_01_99), TransportVersion.current()));
        TransportVersion version = TransportVersionUtils.randomVersion();
        TransportVersion version1 = TransportVersionUtils.randomVersion();
        if (version.id() >= version1.id()) {
            assertEquals(version, TransportVersion.max(version1, version));
        } else {
            assertEquals(version1, TransportVersion.max(version1, version));
        }
    }

    public void testIsPatchFrom() {
        TransportVersion patchVersion = TransportVersion.fromId(8_800_0_04);
        assertThat(TransportVersion.fromId(8_799_0_00).isPatchFrom(patchVersion), is(false));
        assertThat(TransportVersion.fromId(8_799_0_09).isPatchFrom(patchVersion), is(false));
        assertThat(TransportVersion.fromId(8_800_0_00).isPatchFrom(patchVersion), is(false));
        assertThat(TransportVersion.fromId(8_800_0_03).isPatchFrom(patchVersion), is(false));
        assertThat(TransportVersion.fromId(8_800_0_04).isPatchFrom(patchVersion), is(true));
        assertThat(TransportVersion.fromId(8_800_0_49).isPatchFrom(patchVersion), is(true));
        assertThat(TransportVersion.fromId(8_800_1_00).isPatchFrom(patchVersion), is(false));
        assertThat(TransportVersion.fromId(8_801_0_00).isPatchFrom(patchVersion), is(false));
    }

    public void testVersionConstantPresent() {
        Set<TransportVersion> ignore = Set.of(TransportVersions.ZERO, TransportVersion.current(), TransportVersions.MINIMUM_COMPATIBLE);
        assertThat(TransportVersion.current(), sameInstance(TransportVersion.fromId(TransportVersion.current().id())));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            TransportVersion version = TransportVersionUtils.randomVersion(ignore);

            assertThat(version, sameInstance(TransportVersion.fromId(version.id())));
        }
    }

    public void testCURRENTIsLatest() {
        assertThat(TransportVersion.getAllVersions().getLast(), is(TransportVersion.current()));
    }

    public void testPatchVersionsStillAvailable() {
        for (TransportVersion tv : TransportVersion.getAllVersions()) {
            if (tv.onOrAfter(TransportVersions.V_8_9_X) && (tv.id() % 100) > 90) {
                fail(
                    "Transport version "
                        + tv
                        + " is nearing the limit of available patch numbers."
                        + " Please inform the Core/Infra team that isPatchFrom may need to be modified"
                );
            }
        }
    }

    public void testToReleaseVersion() {
        assertThat(TransportVersion.current().toReleaseVersion(), endsWith(Version.CURRENT.toString()));
    }

    public void testToString() {
        assertEquals("5000099", TransportVersion.fromId(5_00_00_99).toString());
        assertEquals("2030099", TransportVersion.fromId(2_03_00_99).toString());
        assertEquals("1000099", TransportVersion.fromId(1_00_00_99).toString());
        assertEquals("2000099", TransportVersion.fromId(2_00_00_99).toString());
        assertEquals("5000099", TransportVersion.fromId(5_00_00_99).toString());
    }
}
