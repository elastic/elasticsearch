/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.VersionEncoder.SortMode;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.mapper.VersionEncoder.decodeVersion;

public class VersionEncoderTests extends ESTestCase {

    public void testEncodingOrderingSemver() {
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
    }

    private BytesRef encSemver(String s) {
        return VersionEncoder.encodeVersion(s, SortMode.SEMVER);
    };

    public void testDecodingSemver() {
        for (String version : List.of(
            "1",
            "1.1",
            "1.0.0",
            "1.2.3.4",
            "1.0.0-alpha",
            "1-alpha.11",
            "1-a1234.12.13278.beta",
            "1.0.0-beta+someBuildNumber-123456-open",
            "1.3.0+build1234567"
        )) {
            String decoded = decodeVersion(encSemver(version), SortMode.SEMVER);
            assertEquals(version, decoded);
        }
    }

    public void testEncodingOrderingNumerical() {
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
        return VersionEncoder.encodeVersion(s, SortMode.HONOUR_NUMERALS);
    };

    public void testDecodingHonourNumeral() {
        for (String version : List.of(
            "1",
            "1.1",
            "1.0.0",
            "1.2.3.4",
            "1.0.0-alpha",
            "1-alpha.11",
            "1-a1234.12.13278.beta",
            "1.0.0-beta+someBuildNumber-123456-open",
            "1.3.0+build1234567"
        )) {
            String decoded = decodeVersion(encSemver(version), SortMode.HONOUR_NUMERALS);
            assertEquals(version, decoded);
        }
    }

    /**
     * test that encoding and decoding leads back to the same version string
     */
    public void testRandomRoundtrip() {
        String versionString = randomVersionString();
        assertEquals(versionString, decodeVersion(encSemver(versionString), SortMode.SEMVER));
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
            sb.append("+").append(randomAlphaOfLengthBetween(1,15));
        }
        return sb.toString();
    }
}
