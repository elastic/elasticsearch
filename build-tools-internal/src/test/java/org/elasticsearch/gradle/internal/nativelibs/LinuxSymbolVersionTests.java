/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.junit.Test;

import static org.elasticsearch.gradle.internal.nativelibs.LinuxSymbolVersion.Kind.GLIBC;
import static org.elasticsearch.gradle.internal.nativelibs.LinuxSymbolVersion.Kind.GLIBCXX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LinuxSymbolVersionTests {

    @Test
    public void parsesGlibcAndGlibcxxTokens() {
        assertEquals(new LinuxSymbolVersion(GLIBC, 2, 28, 0), LinuxSymbolVersion.parse("GLIBC_2.28").orElseThrow());
        assertEquals(new LinuxSymbolVersion(GLIBC, 2, 28, 0), LinuxSymbolVersion.parse("GLIBC_2.28.0").orElseThrow());
        assertEquals(new LinuxSymbolVersion(GLIBCXX, 3, 4, 25), LinuxSymbolVersion.parse("GLIBCXX_3.4.25").orElseThrow());
        assertEquals(new LinuxSymbolVersion(GLIBCXX, 3, 4, 0), LinuxSymbolVersion.parse("GLIBCXX_3.4").orElseThrow());
        assertEquals(new LinuxSymbolVersion(GLIBCXX, 3, 4, 0), LinuxSymbolVersion.parse("GLIBCXX_3.4.0").orElseThrow());
    }

    @Test
    public void compareToOrdersByMajorThenMinorThenPatch() {
        LinuxSymbolVersion baseline = new LinuxSymbolVersion(GLIBCXX, 3, 4, 25);

        assertTrue(new LinuxSymbolVersion(GLIBCXX, 2, 99, 99).compareTo(baseline) < 0);
        assertTrue(new LinuxSymbolVersion(GLIBCXX, 4, 0, 0).compareTo(baseline) > 0);

        assertTrue(new LinuxSymbolVersion(GLIBCXX, 3, 3, 99).compareTo(baseline) < 0);
        assertTrue(new LinuxSymbolVersion(GLIBCXX, 3, 5, 0).compareTo(baseline) > 0);

        assertTrue(new LinuxSymbolVersion(GLIBCXX, 3, 4, 24).compareTo(baseline) < 0);
        assertTrue(new LinuxSymbolVersion(GLIBCXX, 3, 4, 26).compareTo(baseline) > 0);

        assertEquals(0, new LinuxSymbolVersion(GLIBCXX, 3, 4, 25).compareTo(baseline));
    }

    @Test
    public void compareToTreatsMissingPatchAsZero() {
        LinuxSymbolVersion withPatch = new LinuxSymbolVersion(GLIBC, 2, 17, 0);
        LinuxSymbolVersion withoutPatch = LinuxSymbolVersion.parse("GLIBC_2.17").orElseThrow();

        assertEquals(0, withPatch.compareTo(withoutPatch));
        assertEquals(0, withoutPatch.compareTo(withPatch));
    }

    @Test
    public void exceedsIsFalseForEqualVersions() {
        LinuxSymbolVersion maxGlibc = new LinuxSymbolVersion(GLIBC, 2, 28, 0);
        LinuxSymbolVersion maxGlibcxx = new LinuxSymbolVersion(GLIBCXX, 3, 4, 25);

        assertFalse(maxGlibc.exceeds(maxGlibc));
        assertFalse(maxGlibcxx.exceeds(maxGlibcxx));
        assertFalse(new LinuxSymbolVersion(GLIBC, 2, 28, 0).exceeds(maxGlibc));
        assertFalse(new LinuxSymbolVersion(GLIBCXX, 3, 4, 25).exceeds(maxGlibcxx));
    }

    @Test
    public void exceedsDetectsHigherMajorMinorOrPatch() {
        LinuxSymbolVersion maxGlibc = new LinuxSymbolVersion(GLIBC, 2, 28, 0);
        LinuxSymbolVersion maxGlibcxx = new LinuxSymbolVersion(GLIBCXX, 3, 4, 25);

        assertFalse(new LinuxSymbolVersion(GLIBC, 2, 27, 99).exceeds(maxGlibc));
        assertFalse(new LinuxSymbolVersion(GLIBC, 1, 99, 99).exceeds(maxGlibc));
        assertTrue(new LinuxSymbolVersion(GLIBC, 3, 0, 0).exceeds(maxGlibc));
        assertTrue(new LinuxSymbolVersion(GLIBC, 2, 29, 0).exceeds(maxGlibc));
        assertTrue(new LinuxSymbolVersion(GLIBC, 2, 28, 1).exceeds(maxGlibc));

        assertFalse(new LinuxSymbolVersion(GLIBCXX, 3, 4, 24).exceeds(maxGlibcxx));
        assertFalse(new LinuxSymbolVersion(GLIBCXX, 3, 3, 99).exceeds(maxGlibcxx));
        assertFalse(new LinuxSymbolVersion(GLIBCXX, 2, 99, 99).exceeds(maxGlibcxx));
        assertTrue(new LinuxSymbolVersion(GLIBCXX, 4, 0, 0).exceeds(maxGlibcxx));
        assertTrue(new LinuxSymbolVersion(GLIBCXX, 3, 5, 0).exceeds(maxGlibcxx));
        assertTrue(new LinuxSymbolVersion(GLIBCXX, 3, 4, 26).exceeds(maxGlibcxx));
    }

    @Test
    public void normalizeTrimsAndUppercases() {
        assertEquals("GLIBC_2.28", LinuxSymbolVersion.normalize("  glibc_2.28  "));
        assertEquals("GLIBCXX_3.4.25", LinuxSymbolVersion.normalize("glibcxx_3.4.25"));
        assertEquals("2.28", LinuxSymbolVersion.normalize(" 2.28 "));
    }

    @Test
    public void toStringBasic() {
        assertEquals("GLIBC_2.28", new LinuxSymbolVersion(GLIBC, 2, 28, 0).toString());
        assertEquals("GLIBCXX_3.4.25", new LinuxSymbolVersion(GLIBCXX, 3, 4, 25).toString());
    }
}
