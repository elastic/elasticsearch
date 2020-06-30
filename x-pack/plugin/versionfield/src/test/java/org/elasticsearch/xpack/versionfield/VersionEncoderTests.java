/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.versionfield.VersionEncoder.SortMode;
import org.elasticsearch.xpack.versionfield.VersionEncoder.VersionParts;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.versionfield.VersionEncoder.decodeVersion;

public class VersionEncoderTests extends ESTestCase {

    public void testEncodingOrderingSemver() {
        VersionEncoder.strictSemverCheck = false;
        assertTrue(encSemver("1").compareTo(encSemver("1.0")) < 0);
        assertTrue(encSemver("1.0").compareTo(encSemver("1.0.0.0.0.0.0.0.0.1")) < 0);
        assertTrue(encSemver("1.0.0").compareTo(encSemver("1.0.0.0.0.0.0.0.0.1")) < 0);
        assertTrue(encSemver("1.0.0").compareTo(encSemver("2.0.0")) < 0);
        assertTrue(encSemver("2.0.0").compareTo(encSemver("11.0.0")) < 0);
        assertTrue(encSemver("2.0.0").compareTo(encSemver("2.1.0")) < 0);
        assertTrue(encSemver("2.1.0").compareTo(encSemver("2.1.1")) < 0);
        assertTrue(encSemver("2.1.1").compareTo(encSemver("2.1.1.0")) < 0);
        assertTrue(encSemver("1.0.0").compareTo(encSemver("2.0")) < 0);
        assertTrue(encSemver("1.0.0-a").compareTo(encSemver("1.0.0-b")) < 0);
        assertTrue(encSemver("1.0.0-1.0.0").compareTo(encSemver("1.0.0-2.0")) < 0);
        assertTrue(encSemver("1.0.0-alpha").compareTo(encSemver("1.0.0-alpha.1")) < 0);
        assertTrue(encSemver("1.0.0-alpha.1").compareTo(encSemver("1.0.0-alpha.beta")) < 0);
        assertTrue(encSemver("1.0.0-alpha.beta").compareTo(encSemver("1.0.0-beta")) < 0);
        assertTrue(encSemver("1.0.0-beta").compareTo(encSemver("1.0.0-beta.2")) < 0);
        assertTrue(encSemver("1.0.0-beta.2").compareTo(encSemver("1.0.0-beta.11")) < 0);
        assertTrue(encSemver("1.0.0-beta11").compareTo(encSemver("1.0.0-beta2")) < 0); // correct according to Semver specs
        assertTrue(encSemver("1.0.0-beta.11").compareTo(encSemver("1.0.0-rc.1")) < 0);
        assertTrue(encSemver("1.0.0-rc.1").compareTo(encSemver("1.0.0")) < 0);
        assertTrue(encSemver("1.0.0").compareTo(encSemver("2.0.0-pre127")) < 0);
        assertTrue(encSemver("2.0.0-pre127").compareTo(encSemver("2.0.0-pre128")) < 0);
        assertTrue(encSemver("2.0.0-pre20201231z110026").compareTo(encSemver("2.0.0-pre227")) < 0);
        // some rare ones that fail strict validation
        // assertTrue(encSemver("12.el2").compareTo(encSemver("12.el11")) < 0);
        // assertTrue(encSemver("12.el2-1.0-rc5").compareTo(encSemver("12.el2")) < 0);
    }

    private BytesRef encSemver(String s) {
        return VersionEncoder.encodeVersion(s, SortMode.SEMVER);
    };

    public void testDecodingSemver() {
        for (String version : List.of(
            "1.0.0",
            "1.2.34",
            "1.0.0-alpha",
            "1.0.0-alpha.11",
            "1.0.0-a1234.12.13278.beta",
            "1.0.0-beta+someBuildNumber-123456-open",
            "1.3.0+build1234567"
        )) {
            String decoded = decodeVersion(encSemver(version));
            assertEquals(version, decoded);
        }
    }

    public void testEncodingOrderingNumerical() {
        VersionEncoder.strictSemverCheck = false;
        assertTrue(encNumeric("1.0.0").compareTo(encNumeric("2.0.0")) < 0);
        assertTrue(encNumeric("2.0.0").compareTo(encNumeric("11.0.0")) < 0);
        assertTrue(encNumeric("2.0.0").compareTo(encNumeric("2.1.0")) < 0);
        assertTrue(encNumeric("2.1.0").compareTo(encNumeric("2.1.1")) < 0);
        assertTrue(encNumeric("2.1.1").compareTo(encNumeric("2.1.1.0")) < 0);
        assertTrue(encNumeric("1.0.0").compareTo(encNumeric("2.0")) < 0);
        assertTrue(encNumeric("1.0.0-a").compareTo(encNumeric("1.0.0-b")) < 0);
        assertTrue(encNumeric("1.0.0-1.0.0").compareTo(encNumeric("1.0.0-2.0")) < 0);
        assertTrue(encNumeric("1.0.0-alpha").compareTo(encNumeric("1.0.0-alpha.1")) < 0);
        assertTrue(encNumeric("1.0.0-123u11").compareTo(encNumeric("1.0.0-234u11")) < 0);
        assertTrue(encNumeric("1.0.0-alpha.1").compareTo(encNumeric("1.0.0-alpha.beta")) < 0);
        assertTrue(encNumeric("1.0.0-alpha.beta").compareTo(encNumeric("1.0.0-beta")) < 0);
        assertTrue(encNumeric("1.0.0-beta").compareTo(encNumeric("1.0.0-beta.2")) < 0);
        assertTrue(encNumeric("1.0.0-beta.2").compareTo(encNumeric("1.0.0-beta.11")) < 0);
        assertTrue(encNumeric("1.0.0-beta2").compareTo(encNumeric("1.0.0-beta11")) < 0);
        assertTrue(encNumeric("1.0.0-beta.11").compareTo(encNumeric("1.0.0-rc.1")) < 0);
        assertTrue(encNumeric("1.0.0-rc.1").compareTo(encNumeric("1.0.0")) < 0);
        assertTrue(encNumeric("1.0.0").compareTo(encNumeric("2.0.0-pre127")) < 0);
        assertTrue(encNumeric("2.0.0-pre127").compareTo(encNumeric("2.0.0-pre128")) < 0);
        assertTrue(encNumeric("2.0.0-pre227").compareTo(encNumeric("2.0.0-pre20201231z110026")) < 0);
    }

    private BytesRef encNumeric(String s) {
        return VersionEncoder.encodeVersion(s, SortMode.NATURAL);
    };

    public void testDecodingHonourNumeral() {
        for (String version : List.of(
            "1.0.0",
            "1.2.345",
            "1.0.0-alpha",
            "1.0.0-alpha.11",
            "1.0.0-a1234.12.13278.beta",
            "1.0.0-beta+someBuildNumber-123456-open",
            "1.3.0+build1234567"
        )) {
            String decoded = decodeVersion(encSemver(version));
            assertEquals(version, decoded);
        }
    }

    public void testMaxDigitGroupLength() {
        String versionString = "1.0." + "1".repeat(128);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> decodeVersion(encSemver(versionString))
        );
        assertEquals("Groups of digits cannot be longer than 127, but found: 128", ex.getMessage());
    }

    /**
     * test that encoding and decoding leads back to the same version string
     */
    public void testRandomRoundtrip() {
        VersionEncoder.strictSemverCheck = false;
        String versionString = randomVersionString();
        assertEquals(versionString, decodeVersion(encSemver(versionString)));
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
        VersionEncoder.strictSemverCheck = true;
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
            "99999999999999999999999.999999999999999999.99999999999999999",
            "1.0.0-0A.is.legal" };
        for (String version : validSemverVersions) {
            assertTrue("should be valid: " + version, VersionEncoder.legalVersionString(VersionParts.ofVersion(version)));
        }

        String[] invalidSemverVersions = new String[] {
            "1",
            "1.2",
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
            "1.2",
            "1.2.3.DEV",
            "1.2-SNAPSHOT",
            "1.2.31.2.3----RC-SNAPSHOT.12.09.1--..12+788",
            "1.2-RC-SNAPSHOT",
            "-1.0.3-gamma+b7718",
            "+justmeta",
            "9.8.7+meta+meta",
            "9.8.7-whatever+meta+meta",
            "99999999999999999999999.999999999999999999.99999999999999999----RC-SNAPSHOT.12.09.1--------------------------------..12" };
        for (String version : invalidSemverVersions) {
            assertFalse("should be invalid: " + version, VersionEncoder.legalVersionString(VersionParts.ofVersion(version)));
        }
    }
}
