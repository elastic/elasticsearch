/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.versionfield.VersionEncoder.EncodedVersion;

import java.util.Arrays;

import static org.elasticsearch.xpack.versionfield.VersionEncoder.decodeVersion;

public class VersionEncoderTests extends ESTestCase {

    public void testEncodingOrderingSemver() {
        assertTrue(encodeVersion("1").compareTo(encodeVersion("1.0")) < 0);
        assertTrue(encodeVersion("1.0").compareTo(encodeVersion("1.0.0.0.0.0.0.0.0.1")) < 0);
        assertTrue(encodeVersion("1.0.0").compareTo(encodeVersion("1.0.0.0.0.0.0.0.0.1")) < 0);
        assertTrue(encodeVersion("1.0.0").compareTo(encodeVersion("2.0.0")) < 0);
        assertTrue(encodeVersion("2.0.0").compareTo(encodeVersion("11.0.0")) < 0);
        assertTrue(encodeVersion("2.0.0").compareTo(encodeVersion("2.1.0")) < 0);
        assertTrue(encodeVersion("2.1.0").compareTo(encodeVersion("2.1.1")) < 0);
        assertTrue(encodeVersion("2.1.1").compareTo(encodeVersion("2.1.1.0")) < 0);
        assertTrue(encodeVersion("2.0.0").compareTo(encodeVersion("11.0.0")) < 0);
        assertTrue(encodeVersion("1.0.0").compareTo(encodeVersion("2.0")) < 0);
        assertTrue(encodeVersion("1.0.0-a").compareTo(encodeVersion("1.0.0-b")) < 0);
        assertTrue(encodeVersion("1.0.0-1.0.0").compareTo(encodeVersion("1.0.0-2.0")) < 0);
        assertTrue(encodeVersion("1.0.0-alpha").compareTo(encodeVersion("1.0.0-alpha.1")) < 0);
        assertTrue(encodeVersion("1.0.0-alpha.1").compareTo(encodeVersion("1.0.0-alpha.beta")) < 0);
        assertTrue(encodeVersion("1.0.0-alpha.beta").compareTo(encodeVersion("1.0.0-beta")) < 0);
        assertTrue(encodeVersion("1.0.0-beta").compareTo(encodeVersion("1.0.0-beta.2")) < 0);
        assertTrue(encodeVersion("1.0.0-beta.2").compareTo(encodeVersion("1.0.0-beta.11")) < 0);
        assertTrue(encodeVersion("1.0.0-beta11").compareTo(encodeVersion("1.0.0-beta2")) < 0); // correct according to Semver specs
        assertTrue(encodeVersion("1.0.0-beta.11").compareTo(encodeVersion("1.0.0-rc.1")) < 0);
        assertTrue(encodeVersion("1.0.0-rc.1").compareTo(encodeVersion("1.0.0")) < 0);
        assertTrue(encodeVersion("1.0.0").compareTo(encodeVersion("2.0.0-pre127")) < 0);
        assertTrue(encodeVersion("2.0.0-pre127").compareTo(encodeVersion("2.0.0-pre128")) < 0);
        assertTrue(encodeVersion("2.0.0-pre128").compareTo(encodeVersion("2.0.0-pre128-somethingelse")) < 0);
        assertTrue(encodeVersion("2.0.0-pre20201231z110026").compareTo(encodeVersion("2.0.0-pre227")) < 0);
        // invalid versions sort after valid ones
        assertTrue(encodeVersion("99999.99999.99999").compareTo(encodeVersion("1.invalid")) < 0);
        assertTrue(encodeVersion("").compareTo(encodeVersion("a")) < 0);
    }

    private static BytesRef encodeVersion(String version) {
        return VersionEncoder.encodeVersion(version).bytesRef;
    }

    public void testPreReleaseFlag() {
        assertTrue(VersionEncoder.encodeVersion("1.2-alpha.beta").isPreRelease);
        assertTrue(VersionEncoder.encodeVersion("1.2.3-someOtherPreRelease").isPreRelease);
        assertTrue(VersionEncoder.encodeVersion("1.2.3-some-Other-Pre.123").isPreRelease);
        assertTrue(VersionEncoder.encodeVersion("1.2.3-some-Other-Pre.123+withBuild").isPreRelease);

        assertFalse(VersionEncoder.encodeVersion("1").isPreRelease);
        assertFalse(VersionEncoder.encodeVersion("1.2").isPreRelease);
        assertFalse(VersionEncoder.encodeVersion("1.2.3").isPreRelease);
        assertFalse(VersionEncoder.encodeVersion("1.2.3+buildSufix").isPreRelease);
        assertFalse(VersionEncoder.encodeVersion("1.2.3+buildSufix-withDash").isPreRelease);
    }

    public void testVersionPartExtraction() {
        int numParts = randomIntBetween(1, 6);
        String[] parts = new String[numParts];
        for (int i = 0; i < numParts; i++) {
            parts[i] = String.valueOf(randomIntBetween(1, 1000));
        }
        EncodedVersion encodedVersion = VersionEncoder.encodeVersion(String.join(".", parts));
        assertEquals(parts[0], encodedVersion.major.toString());
        if (numParts > 1) {
            assertEquals(parts[1], encodedVersion.minor.toString());
        } else {
            assertNull(encodedVersion.minor);
        }
        if (numParts > 2) {
            assertEquals(parts[2], encodedVersion.patch.toString());
        } else {
            assertNull(encodedVersion.patch);
        }
    }

    public void testMaxDigitGroupLength() {
        String versionString = "1.0." + "1".repeat(128);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> decodeVersion(encodeVersion(versionString)));
        assertEquals("Groups of digits cannot be longer than 127, but found: 128", ex.getMessage());
    }

    /**
     * test that encoding and decoding leads back to the same version string
     */
    public void testRandomRoundtrip() {
        String versionString = randomVersionString();
        assertEquals(versionString, decodeVersion(encodeVersion(versionString)).utf8ToString());
    }

    private String randomVersionString() {
        StringBuilder sb = new StringBuilder();
        sb.append(randomIntBetween(0, 1000));
        int releaseNumerals = randomIntBetween(0, 4);
        for (int i = 0; i < releaseNumerals; i++) {
            sb.append(".");
            sb.append(randomIntBetween(0, 10000));
        }
        // optional pre-release part
        if (randomBoolean()) {
            sb.append("-");
            int preReleaseParts = randomIntBetween(1, 5);
            for (int i = 0; i < preReleaseParts; i++) {
                if (randomBoolean()) {
                    sb.append(randomIntBetween(0, 1000));
                } else {
                    int alphanumParts = 3;
                    for (int j = 0; j < alphanumParts; j++) {
                        if (randomBoolean()) {
                            sb.append(randomAlphaOfLengthBetween(1, 2));
                        } else {
                            sb.append(randomIntBetween(1, 99));
                        }
                        if (rarely()) {
                            sb.append(randomFrom(Arrays.asList("-")));
                        }
                    }
                }
                sb.append(".");
            }
            sb.deleteCharAt(sb.length() - 1);  // remove trailing dot
        }
        // optional build part
        if (randomBoolean()) {
            sb.append("+").append(randomAlphaOfLengthBetween(1, 15));
        }
        return sb.toString();
    }

    /**
     * taken from https://regex101.com/r/vkijKf/1/ via https://semver.org/
     */
    public void testSemVerValidation() {
        String[] validSemverVersions = new String[] {
            "0.0.4",
            "1.2.3",
            "10.20.30",
            "1.1.2-prerelease+meta",
            "1.1.2+meta",
            "1.1.2+meta-valid",
            "1.0.0-alpha",
            "1.0.0-beta",
            "1.0.0-alpha.beta",
            "1.0.0-alpha.beta.1",
            "1.0.0-alpha.1",
            "1.0.0-alpha0.valid",
            "1.0.0-alpha.0valid",
            "1.0.0-alpha-a.b-c-somethinglong+build.1-aef.1-its-okay",
            "1.0.0-rc.1+build.1",
            "2.0.0-rc.1+build.123",
            "1.2.3-beta",
            "10.2.3-DEV-SNAPSHOT",
            "1.2.3-SNAPSHOT-123",
            "1.0.0",
            "2.0.0",
            "1.1.7",
            "2.0.0+build.1848",
            "2.0.1-alpha.1227",
            "1.0.0-alpha+beta",
            "1.2.3----RC-SNAPSHOT.12.9.1--.12+788",
            "1.2.3----R-S.12.9.1--.12+meta",
            "1.2.3----RC-SNAPSHOT.12.9.1--.12",
            "1.0.0+0.build.1-rc.10000aaa-kk-0.1",

            "999999999.999999999.999999999",
            "1.0.0-0A.is.legal",
            // the following are not strict semver but we allow them
            "1.2-SNAPSHOT",
            "1.2-RC-SNAPSHOT",
            "1",
            "1.2.3.4" };
        for (String version : validSemverVersions) {
            assertTrue("should be valid: " + version, VersionEncoder.encodeVersion(version).isLegal);
            // since we're here, also check encoding / decoding rountrip
            assertEquals(version, decodeVersion(encodeVersion(version)).utf8ToString());
        }

        String[] invalidSemverVersions = new String[] {
            "",
            "1.2.3-0123",
            "1.2.3-0123.0123",
            "1.1.2+.123",
            "+invalid",
            "-invalid",
            "-invalid+invalid",
            "-invalid.01",
            "alpha",
            "alpha.beta",
            "alpha.beta.1",
            "alpha.1",
            "alpha+beta",
            "alpha_beta",
            "alpha.",
            "alpha..",
            "beta",
            "1.0.0-alpha_beta",
            "-alpha.",
            "1.0.0-alpha..",
            "1.0.0-alpha..1",
            "1.0.0-alpha...1",
            "1.0.0-alpha....1",
            "1.0.0-alpha.....1",
            "1.0.0-alpha......1",
            "1.0.0-alpha.......1",
            "01.1.1",
            "1.01.1",
            "1.1.01",
            "1.2.3.DEV",
            "1.2.31.2.3----RC-SNAPSHOT.12.09.1--..12+788",
            "-1.0.3-gamma+b7718",
            "+justmeta",
            "9.8.7+meta+meta",
            "9.8.7-whatever+meta+meta",
            "999999999.999999999.999999999.----RC-SNAPSHOT.12.09.1--------------------------------..12",
            "12.el2",
            "12.el2-1.0-rc5",
            "6.nüll.7" // make sure extended ascii-range (128-255) in invalid versions is decoded correctly
        };
        for (String version : invalidSemverVersions) {
            assertFalse("should be invalid: " + version, VersionEncoder.encodeVersion(version).isLegal);
            // since we're here, also check encoding / decoding rountrip
            assertEquals(version, decodeVersion(encodeVersion(version)).utf8ToString());
        }
    }
}
