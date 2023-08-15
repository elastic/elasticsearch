/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.lucene.util.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

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

public class IndexVersionTests extends ESTestCase {

    public void testVersionComparison() {
        IndexVersion V_7_2_0 = IndexVersion.V_7_2_0;
        IndexVersion V_8_0_0 = IndexVersion.V_8_0_0;
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
        public static final IndexVersion V_0_00_01 = new IndexVersion(199, Version.LATEST);
        public static final IndexVersion V_0_000_002 = new IndexVersion(2, Version.LATEST);
        public static final IndexVersion V_0_000_003 = new IndexVersion(3, Version.LATEST);
        public static final IndexVersion V_0_000_004 = new IndexVersion(4, Version.LATEST);
    }

    public static class DuplicatedIdFakeVersion {
        public static final IndexVersion V_0_000_001 = new IndexVersion(1, Version.LATEST);
        public static final IndexVersion V_0_000_002 = new IndexVersion(2, Version.LATEST);
        public static final IndexVersion V_0_000_003 = new IndexVersion(2, Version.LATEST);
    }

    public void testStaticIndexVersionChecks() {
        assertThat(
            IndexVersion.getAllVersionIds(IndexVersionTests.CorrectFakeVersion.class),
            equalTo(
                Map.of(
                    199,
                    IndexVersionTests.CorrectFakeVersion.V_0_00_01,
                    2,
                    IndexVersionTests.CorrectFakeVersion.V_0_000_002,
                    3,
                    IndexVersionTests.CorrectFakeVersion.V_0_000_003,
                    4,
                    IndexVersionTests.CorrectFakeVersion.V_0_000_004
                )
            )
        );
        AssertionError e = expectThrows(AssertionError.class, () -> IndexVersion.getAllVersionIds(DuplicatedIdFakeVersion.class));
        assertThat(e.getMessage(), containsString("have the same version number"));
    }

    private static String padNumber(String number) {
        return number.length() == 1 ? "0" + number : number;
    }

    public void testDefinedConstants() throws IllegalAccessException {
        Pattern historicalVersion = Pattern.compile("^V_(\\d{1,2})_(\\d{1,2})_(\\d{1,2})$");
        Pattern IndexVersion = Pattern.compile("^V_(\\d+)_(\\d{3})_(\\d{3})$");
        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE");

        for (java.lang.reflect.Field field : IndexVersion.class.getFields()) {
            if (field.getType() == IndexVersion.class && ignore.contains(field.getName()) == false) {

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
                } else if ((matcher = IndexVersion.matcher(field.getName())).matches()) {
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
            IndexVersionUtils.getPreviousVersion(),
            IndexVersion.min(IndexVersion.current(), IndexVersionUtils.getPreviousVersion())
        );
        assertEquals(IndexVersion.fromId(1_01_01_99), IndexVersion.min(IndexVersion.fromId(1_01_01_99), IndexVersion.current()));
        IndexVersion version = IndexVersionUtils.randomVersion();
        IndexVersion version1 = IndexVersionUtils.randomVersion();
        if (version.id() <= version1.id()) {
            assertEquals(version, IndexVersion.min(version1, version));
        } else {
            assertEquals(version1, IndexVersion.min(version1, version));
        }
    }

    public void testMax() {
        assertEquals(IndexVersion.current(), IndexVersion.max(IndexVersion.current(), IndexVersionUtils.getPreviousVersion()));
        assertEquals(IndexVersion.current(), IndexVersion.max(IndexVersion.fromId(1_01_01_99), IndexVersion.current()));
        IndexVersion version = IndexVersionUtils.randomVersion();
        IndexVersion version1 = IndexVersionUtils.randomVersion();
        if (version.id() >= version1.id()) {
            assertEquals(version, IndexVersion.max(version1, version));
        } else {
            assertEquals(version1, IndexVersion.max(version1, version));
        }
    }

    public void testVersionConstantPresent() {
        Set<IndexVersion> ignore = Set.of(IndexVersion.ZERO, IndexVersion.current(), IndexVersion.MINIMUM_COMPATIBLE);
        assertThat(IndexVersion.current(), sameInstance(IndexVersion.fromId(IndexVersion.current().id())));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            IndexVersion version = IndexVersionUtils.randomVersion(ignore);

            assertThat(version, sameInstance(IndexVersion.fromId(version.id())));
        }
    }

    public void testCURRENTIsLatest() {
        assertThat(Collections.max(IndexVersion.getAllVersions()), is(IndexVersion.current()));
    }

    public void testToString() {
        assertEquals("5000099", IndexVersion.fromId(5_00_00_99).toString());
        assertEquals("2030099", IndexVersion.fromId(2_03_00_99).toString());
        assertEquals("1000099", IndexVersion.fromId(1_00_00_99).toString());
        assertEquals("2000099", IndexVersion.fromId(2_00_00_99).toString());
        assertEquals("5000099", IndexVersion.fromId(5_00_00_99).toString());
    }
}
