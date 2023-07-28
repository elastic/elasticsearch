/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

public class TransportVersionTests extends ESTestCase {

    public void testVersionComparison() {
        TransportVersion V_7_2_0 = TransportVersion.V_7_2_0;
        TransportVersion V_8_0_0 = TransportVersion.V_8_0_0;
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
            TransportVersion.getAllVersionIds(CorrectFakeVersion.class),
            equalTo(
                Map.of(
                    199,
                    CorrectFakeVersion.V_0_00_01,
                    2,
                    CorrectFakeVersion.V_0_000_002,
                    3,
                    CorrectFakeVersion.V_0_000_003,
                    4,
                    CorrectFakeVersion.V_0_000_004
                )
            )
        );
        AssertionError e = expectThrows(AssertionError.class, () -> TransportVersion.getAllVersionIds(DuplicatedIdFakeVersion.class));
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

    public void testVersionConstantPresent() {
        Set<TransportVersion> ignore = Set.of(TransportVersion.ZERO, TransportVersion.current(), TransportVersion.MINIMUM_COMPATIBLE);
        assertThat(TransportVersion.current(), sameInstance(TransportVersion.fromId(TransportVersion.current().id())));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            TransportVersion version = TransportVersionUtils.randomVersion(ignore);

            assertThat(version, sameInstance(TransportVersion.fromId(version.id())));
        }
    }

    public void testCURRENTIsLatest() {
        assertThat(Collections.max(TransportVersion.getAllVersions()), is(TransportVersion.current()));
    }

    public void testToString() {
        assertEquals("5000099", TransportVersion.fromId(5_00_00_99).toString());
        assertEquals("2030099", TransportVersion.fromId(2_03_00_99).toString());
        assertEquals("1000099", TransportVersion.fromId(1_00_00_99).toString());
        assertEquals("2000099", TransportVersion.fromId(2_00_00_99).toString());
        assertEquals("5000099", TransportVersion.fromId(5_00_00_99).toString());
    }
}
