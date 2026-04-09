/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.hamcrest.Matchers;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Locale;
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
        IndexVersion V_8_2_0 = IndexVersions.V_8_2_0;
        IndexVersion current = IndexVersion.current();
        assertThat(V_8_2_0.before(current), is(true));
        assertThat(V_8_2_0.before(V_8_2_0), is(false));
        assertThat(current.before(V_8_2_0), is(false));

        assertThat(V_8_2_0.onOrBefore(current), is(true));
        assertThat(V_8_2_0.onOrBefore(V_8_2_0), is(true));
        assertThat(current.onOrBefore(V_8_2_0), is(false));

        assertThat(V_8_2_0.after(current), is(false));
        assertThat(V_8_2_0.after(V_8_2_0), is(false));
        assertThat(current.after(V_8_2_0), is(true));

        assertThat(V_8_2_0.onOrAfter(current), is(false));
        assertThat(V_8_2_0.onOrAfter(V_8_2_0), is(true));
        assertThat(current.onOrAfter(V_8_2_0), is(true));

        assertThat(V_8_2_0, is(lessThan(current)));
        assertThat(V_8_2_0.compareTo(V_8_2_0), is(0));
        assertThat(current, is(greaterThan(V_8_2_0)));
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
            IndexVersions.getAllVersionIds(IndexVersionTests.CorrectFakeVersion.class),
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
        AssertionError e = expectThrows(AssertionError.class, () -> IndexVersions.getAllVersionIds(DuplicatedIdFakeVersion.class));
        assertThat(e.getMessage(), containsString("have the same version number"));
    }

    private static String padNumber(String number) {
        return number.length() == 1 ? "0" + number : number;
    }

    public void testDefinedConstants() throws IllegalAccessException {
        Pattern historicalVersion = Pattern.compile("^V_(\\d{1,2})_(\\d{1,2})_(\\d{1,2})$");
        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE");

        for (java.lang.reflect.Field field : IndexVersion.class.getFields()) {
            if (field.getType() == IndexVersion.class && ignore.contains(field.getName()) == false) {

                // check the field modifiers
                assertEquals(
                    "Field " + field.getName() + " should be public static final",
                    Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL,
                    field.getModifiers()
                );

                Matcher matcher;
                if ("UPGRADE_TO_LUCENE_9_9".equals(field.getName())) {
                    // OK
                } else if ((matcher = historicalVersion.matcher(field.getName())).matches()) {
                    // old-style version constant
                    String idString = matcher.group(1) + padNumber(matcher.group(2)) + padNumber(matcher.group(3)) + "99";
                    assertEquals(
                        "Field " + field.getName() + " does not have expected id " + idString,
                        idString,
                        field.get(null).toString()
                    );
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

    public void testGetMinimumCompatibleIndexVersion() {
        assertThat(IndexVersion.getMinimumCompatibleIndexVersion(7170099), equalTo(IndexVersion.fromId(6000099)));
        assertThat(IndexVersion.getMinimumCompatibleIndexVersion(8000099), equalTo(IndexVersion.fromId(7000099)));
        assertThat(IndexVersion.getMinimumCompatibleIndexVersion(9000099), equalTo(IndexVersion.fromId(8000099)));
        assertThat(IndexVersion.getMinimumCompatibleIndexVersion(10000000), equalTo(IndexVersion.fromId(9000000)));
    }

    public void testVersionConstantPresent() {
        Set<IndexVersion> ignore = Set.of(IndexVersions.ZERO, IndexVersion.current(), IndexVersions.MINIMUM_COMPATIBLE);
        assertThat(IndexVersion.current(), sameInstance(IndexVersion.fromId(IndexVersion.current().id())));
        assertThat(IndexVersion.current().luceneVersion(), equalTo(org.apache.lucene.util.Version.LATEST));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            IndexVersion version = IndexVersionUtils.randomVersion(ignore);

            assertThat(version, sameInstance(IndexVersion.fromId(version.id())));
            assertThat(version.luceneVersion(), sameInstance(IndexVersion.fromId(version.id()).luceneVersion()));
        }
    }

    public void testCURRENTIsLatest() {
        assertThat(Collections.max(IndexVersions.getAllVersions()), is(IndexVersion.current()));
    }

    public void testToString() {
        assertEquals("5000099", IndexVersion.fromId(5_00_00_99).toString());
        assertEquals("2030099", IndexVersion.fromId(2_03_00_99).toString());
        assertEquals("1000099", IndexVersion.fromId(1_00_00_99).toString());
        assertEquals("2000099", IndexVersion.fromId(2_00_00_99).toString());
        assertEquals("5000099", IndexVersion.fromId(5_00_00_99).toString());
    }

    public void testParseLenient() {
        // note this is just a silly sanity check, we test it in lucene
        for (IndexVersion version : IndexVersionUtils.allReleasedVersions()) {
            org.apache.lucene.util.Version luceneVersion = version.luceneVersion();
            String string = luceneVersion.toString().toUpperCase(Locale.ROOT).replaceFirst("^LUCENE_(\\d+)_(\\d+)$", "$1.$2");
            assertThat(luceneVersion, Matchers.equalTo(Lucene.parseVersionLenient(string, null)));
        }
    }

    public void testLuceneVersionOnUnknownVersions() {
        // between two known versions, should use the lucene version of the previous version
        IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion();
        IndexVersion currentVersion = IndexVersion.current();
        int intermediateVersionId = previousVersion.id() + randomInt(currentVersion.id() - previousVersion.id() - 1);
        IndexVersion intermediateVersion = IndexVersion.fromId(intermediateVersionId);
        // the version is either the previous version or between the previous version and the current version excluded, so it is assumed to
        // use the same Lucene version as the previous version
        assertThat(intermediateVersion.luceneVersion(), equalTo(previousVersion.luceneVersion()));

        // too old version, major should be the oldest supported lucene version minus 1
        IndexVersion oldVersion = IndexVersion.fromId(5020199);
        assertThat(oldVersion.luceneVersion().major, equalTo(IndexVersionUtils.getLowestReadCompatibleVersion().luceneVersion().major - 1));

        // future version, should be the same version as today
        IndexVersion futureVersion = IndexVersion.fromId(currentVersion.id() + 100);
        assertThat(futureVersion.luceneVersion(), equalTo(currentVersion.luceneVersion()));
    }
}
