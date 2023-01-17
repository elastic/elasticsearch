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

import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

public class TransportVersionTests extends ESTestCase {

    public void testVersionComparison() throws Exception {
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

    public void testMin() {
        assertEquals(
            TransportVersionUtils.getPreviousVersion(),
            TransportVersion.min(TransportVersion.CURRENT, TransportVersionUtils.getPreviousVersion())
        );
        assertEquals(
            TransportVersion.fromId(1_01_01_99),
            TransportVersion.min(TransportVersion.fromId(1_01_01_99), TransportVersion.CURRENT)
        );
        TransportVersion version = TransportVersionUtils.randomVersion();
        TransportVersion version1 = TransportVersionUtils.randomVersion();
        if (version.id <= version1.id) {
            assertEquals(version, TransportVersion.min(version1, version));
        } else {
            assertEquals(version1, TransportVersion.min(version1, version));
        }
    }

    public void testMax() {
        assertEquals(TransportVersion.CURRENT, TransportVersion.max(TransportVersion.CURRENT, TransportVersionUtils.getPreviousVersion()));
        assertEquals(TransportVersion.CURRENT, TransportVersion.max(TransportVersion.fromId(1_01_01_99), TransportVersion.CURRENT));
        TransportVersion version = TransportVersionUtils.randomVersion();
        TransportVersion version1 = TransportVersionUtils.randomVersion();
        if (version.id >= version1.id) {
            assertEquals(version, TransportVersion.max(version1, version));
        } else {
            assertEquals(version1, TransportVersion.max(version1, version));
        }
    }

    public void testVersionConstantPresent() {
        // TODO those versions are not cached at the moment, perhaps we should add them to idToVersion set too?
        Set<TransportVersion> ignore = Set.of(TransportVersion.ZERO, TransportVersion.CURRENT, TransportVersion.MINIMUM_COMPATIBLE);
        assertThat(TransportVersion.CURRENT, sameInstance(TransportVersion.fromId(TransportVersion.CURRENT.id)));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            TransportVersion version = TransportVersionUtils.randomVersion(ignore);

            assertThat(version, sameInstance(TransportVersion.fromId(version.id)));
        }
    }

    public void testCURRENTIsLatest() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            TransportVersion version = TransportVersionUtils.randomVersion();
            if (version != TransportVersion.CURRENT) {
                assertThat(
                    "Version: " + version + " should be before: " + Version.CURRENT + " but wasn't",
                    version.before(TransportVersion.CURRENT),
                    is(true)
                );
            }
        }
    }

    public void testToString() {
        assertEquals("5000099", TransportVersion.fromId(5_00_00_99).toString());
        assertEquals("2030099", TransportVersion.fromId(2_03_00_99).toString());
        assertEquals("1000099", TransportVersion.fromId(1_00_00_99).toString());
        assertEquals("2000099", TransportVersion.fromId(2_00_00_99).toString());
        assertEquals("5000099", TransportVersion.fromId(5_00_00_99).toString());
    }

    public void testMinCompatVersion() {
        Version minCompat = Version.fromId(TransportVersion.V_8_0_0.calculateMinimumCompatVersion().id);
        assertEquals(7, minCompat.major);
        assertEquals("This needs to be updated when 7.18 is released", 17, minCompat.minor);
        assertEquals(0, minCompat.revision);
    }
}
