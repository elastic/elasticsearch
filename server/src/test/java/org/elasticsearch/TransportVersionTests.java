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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

public class TransportVersionTests extends ESTestCase {

    public void testVersionComparison() {
        TransportVersion older = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.minimumCompatible(),
            TransportVersionUtils.getPreviousVersion(TransportVersion.current())
        );
        TransportVersion newer = TransportVersionUtils.randomVersionBetween(random(), older, TransportVersion.current());
        assertThat(older.before(newer), is(true));
        assertThat(older.before(older), is(false));
        assertThat(newer.before(older), is(false));

        assertThat(older.onOrBefore(newer), is(true));
        assertThat(older.onOrBefore(older), is(true));
        assertThat(newer.onOrBefore(older), is(false));

        assertThat(older.after(newer), is(false));
        assertThat(older.after(older), is(false));
        assertThat(newer.after(older), is(true));

        assertThat(older.onOrAfter(newer), is(false));
        assertThat(older.onOrAfter(older), is(true));
        assertThat(newer.onOrAfter(older), is(true));

        assertThat(older, is(lessThan(newer)));
        assertThat(older.compareTo(older), is(0));
        assertThat(newer, is(greaterThan(older)));
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
        Set<TransportVersion> ignore = Set.of(TransportVersion.zero(), TransportVersion.current(), TransportVersion.minimumCompatible());
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

    public void testDuplicateConstants() {
        List<TransportVersion> tvs = TransportVersion.getAllVersions();
        TransportVersion previous = tvs.get(0);
        for (int i = 1; i < tvs.size(); i++) {
            TransportVersion next = tvs.get(i);
            if (next.id() == previous.id()) {
                throw new AssertionError("Duplicate transport version id: " + next.id());
            }
            previous = next;
        }
    }

    public void testLatest() {
        TransportVersion latest = TransportVersion.parseFromBufferedReader(
            "<test>",
            "/transport/definitions/" + Version.CURRENT.major + "." + Version.CURRENT.minor + ".csv",
            TransportVersion.class::getResourceAsStream,
            (c, p, br) -> TransportVersion.fromBufferedReader(c, p, true, false, br, Integer.MAX_VALUE)
        );
        // TODO: once placeholder is removed, test the latest known version can be found fromName
        // assertThat(latest, is(TransportVersion.fromName(latest.name())));
    }

    public void testSupports() {
        byte[] data0 = "100001000,3001000".getBytes(StandardCharsets.UTF_8);
        TransportVersion test0 = TransportVersion.fromBufferedReader(
            "<test>",
            "testSupports0",
            false,
            true,
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data0), StandardCharsets.UTF_8)),
            5000000
        );
        assertThat(new TransportVersion(null, 2003000, null).supports(test0), is(false));
        assertThat(new TransportVersion(null, 3001000, null).supports(test0), is(true));
        assertThat(new TransportVersion(null, 100001001, null).supports(test0), is(true));

        byte[] data1 = "3002000".getBytes(StandardCharsets.UTF_8);
        TransportVersion test1 = TransportVersion.fromBufferedReader(
            "<test>",
            "testSupports1",
            false,
            true,
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data1), StandardCharsets.UTF_8)),
            5000000
        );
        assertThat(new TransportVersion(null, 2003000, null).supports(test1), is(false));
        assertThat(new TransportVersion(null, 3001000, null).supports(test1), is(false));
        assertThat(new TransportVersion(null, 3001001, null).supports(test1), is(false));
        assertThat(new TransportVersion(null, 3002000, null).supports(test1), is(true));
        assertThat(new TransportVersion(null, 100001000, null).supports(test1), is(true));
        assertThat(new TransportVersion(null, 100001001, null).supports(test1), is(true));

        byte[] data2 = "3003000,2001001,1001001".getBytes(StandardCharsets.UTF_8);
        TransportVersion test2 = TransportVersion.fromBufferedReader(
            "<test>",
            "testSupports2",
            false,
            true,
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data2), StandardCharsets.UTF_8)),
            5000000
        );
        assertThat(new TransportVersion(null, 1001000, null).supports(test2), is(false));
        assertThat(new TransportVersion(null, 1001001, null).supports(test2), is(true));
        assertThat(new TransportVersion(null, 1001002, null).supports(test2), is(true));
        assertThat(new TransportVersion(null, 1002000, null).supports(test2), is(false));
        assertThat(new TransportVersion(null, 1002001, null).supports(test2), is(false));
        assertThat(new TransportVersion(null, 2001000, null).supports(test2), is(false));
        assertThat(new TransportVersion(null, 2001001, null).supports(test2), is(true));
        assertThat(new TransportVersion(null, 2001002, null).supports(test2), is(true));
        assertThat(new TransportVersion(null, 2003000, null).supports(test2), is(false));
        assertThat(new TransportVersion(null, 2003001, null).supports(test2), is(false));
        assertThat(new TransportVersion(null, 3001000, null).supports(test2), is(false));
        assertThat(new TransportVersion(null, 3001001, null).supports(test2), is(false));
        assertThat(new TransportVersion(null, 3003000, null).supports(test2), is(true));
        assertThat(new TransportVersion(null, 3003001, null).supports(test2), is(true));
        assertThat(new TransportVersion(null, 3003002, null).supports(test2), is(true));
        assertThat(new TransportVersion(null, 3003003, null).supports(test2), is(true));
        assertThat(new TransportVersion(null, 100001000, null).supports(test2), is(true));
        assertThat(new TransportVersion(null, 100001001, null).supports(test2), is(true));

        byte[] data3 = "100002000,3003001,2001002".getBytes(StandardCharsets.UTF_8);
        TransportVersion test3 = TransportVersion.fromBufferedReader(
            "<test>",
            "testSupports3",
            false,
            true,
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data3), StandardCharsets.UTF_8)),
            5000000
        );
        assertThat(new TransportVersion(null, 1001001, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 1001002, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 1001003, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 1002001, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 1002002, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 2001001, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 2001002, null).supports(test3), is(true));
        assertThat(new TransportVersion(null, 2001003, null).supports(test3), is(true));
        assertThat(new TransportVersion(null, 2003000, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 2003001, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 3001000, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 3001001, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 3003000, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 3003001, null).supports(test3), is(true));
        assertThat(new TransportVersion(null, 3003002, null).supports(test3), is(true));
        assertThat(new TransportVersion(null, 3003003, null).supports(test3), is(true));
        assertThat(new TransportVersion(null, 3004000, null).supports(test3), is(true));
        assertThat(new TransportVersion(null, 100001000, null).supports(test3), is(true));
        assertThat(new TransportVersion(null, 100001001, null).supports(test3), is(true));

        byte[] data4 = "100002000,3003002,2001003,1001002".getBytes(StandardCharsets.UTF_8);
        TransportVersion test4 = TransportVersion.fromBufferedReader(
            "<test>",
            "testSupports3",
            false,
            true,
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data4), StandardCharsets.UTF_8)),
            5000000
        );
        assertThat(new TransportVersion(null, 1001001, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 1001002, null).supports(test4), is(true));
        assertThat(new TransportVersion(null, 1001003, null).supports(test4), is(true));
        assertThat(new TransportVersion(null, 1002001, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 1002002, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 1002003, null).supports(test3), is(false));
        assertThat(new TransportVersion(null, 2001002, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 2001003, null).supports(test4), is(true));
        assertThat(new TransportVersion(null, 2001004, null).supports(test4), is(true));
        assertThat(new TransportVersion(null, 2003000, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 2003001, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 3001000, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 3001001, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 3003000, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 3003001, null).supports(test4), is(false));
        assertThat(new TransportVersion(null, 3003002, null).supports(test4), is(true));
        assertThat(new TransportVersion(null, 3003003, null).supports(test4), is(true));
        assertThat(new TransportVersion(null, 3003004, null).supports(test4), is(true));
        assertThat(new TransportVersion(null, 3004000, null).supports(test4), is(true));
        assertThat(new TransportVersion(null, 100001000, null).supports(test4), is(true));
        assertThat(new TransportVersion(null, 100001001, null).supports(test4), is(true));
    }

    public void testComment() {
        byte[] data1 = ("#comment" + System.lineSeparator() + "1000000").getBytes(StandardCharsets.UTF_8);
        TransportVersion test1 = TransportVersion.fromBufferedReader(
            "<test>",
            "testSupports3",
            false,
            true,
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data1), StandardCharsets.UTF_8)),
            5000000
        );
        assertThat(new TransportVersion(null, 1000000, null).supports(test1), is(true));

        byte[] data2 = (" # comment" + System.lineSeparator() + "1000000").getBytes(StandardCharsets.UTF_8);
        TransportVersion test2 = TransportVersion.fromBufferedReader(
            "<test>",
            "testSupports3",
            false,
            true,
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data2), StandardCharsets.UTF_8)),
            5000000
        );
        assertThat(new TransportVersion(null, 1000000, null).supports(test2), is(true));

        byte[] data3 = ("#comment" + System.lineSeparator() + "# comment3" + System.lineSeparator() + "1000000").getBytes(
            StandardCharsets.UTF_8
        );
        TransportVersion test3 = TransportVersion.fromBufferedReader(
            "<test>",
            "testSupports3",
            false,
            true,
            new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data3), StandardCharsets.UTF_8)),
            5000000
        );
        assertThat(new TransportVersion(null, 1000000, null).supports(test3), is(true));
    }

    public void testMoreLikeThis() {
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> TransportVersion.fromName("to_child_lock_join_query"));
        assertThat(
            ise.getMessage(),
            is(
                "Unknown transport version [to_child_lock_join_query]. "
                    + "Did you mean [to_child_block_join_query]? "
                    + "If this is a new transport version, run './gradlew generateTransportVersion'."
            )
        );

        ise = expectThrows(IllegalStateException.class, () -> TransportVersion.fromName("brand_new_version_unrelated_to_others"));
        assertThat(
            ise.getMessage(),
            is(
                "Unknown transport version [brand_new_version_unrelated_to_others]. "
                    + "If this is a new transport version, run './gradlew generateTransportVersion'."
            )
        );
    }

    public void testTransportVersionsLocked() {
        assertThat(
            "TransportVersions.java is locked. Generate transport versions with TransportVersion.fromName "
                + "and generateTransportVersion gradle task",
            TransportVersions.DEFINED_VERSIONS.getLast().id(),
            equalTo(8_797_0_05)
        );
    }
}
