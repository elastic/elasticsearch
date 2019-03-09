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

package org.elasticsearch.common.network;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class CidrsTests extends ESTestCase {
    public void testNullCidr() {
        try {
            Cidrs.cidrMaskToMinMax(null);
            fail("expected NullPointerException");
        } catch (NullPointerException e) {
            assertThat(e, hasToString(containsString("cidr")));
        }
    }

    public void testSplittingSlash() {
        List<String> cases = new ArrayList<>();
        cases.add("1.2.3.4");
        cases.add("1.2.3.4/32/32");
        cases.add("1.2.3.4/");
        cases.add("/");
        for (String test : cases) {
            try {
                Cidrs.cidrMaskToMinMax(test);
                fail("expected IllegalArgumentException after splitting");
            } catch (IllegalArgumentException e) {
                assertThat(e, hasToString(containsString("expected [a.b.c.d, e]")));
                assertThat(e, hasToString(containsString("splitting on \"/\"")));
            }
        }
    }

    public void testSplittingDot() {
        List<String> cases = new ArrayList<>();
        cases.add("1.2.3/32");
        cases.add("1/32");
        cases.add("1./32");
        cases.add("1../32");
        cases.add("1.../32");
        cases.add("1.2.3.4.5/32");
        cases.add("/32");
        for (String test : cases) {
            try {
                Cidrs.cidrMaskToMinMax(test);
                fail("expected IllegalArgumentException after splitting");
            } catch (IllegalArgumentException e) {
                assertThat(e, hasToString(containsString("unable to parse")));
                assertThat(e, hasToString(containsString("as an IP address literal")));
            }
        }
    }

    public void testValidSpecificCases() {
        List<Tuple<String, long[]>> cases = new ArrayList<>();
        cases.add(new Tuple<>("192.168.0.0/24", new long[]{(192L << 24) + (168 << 16), (192L << 24) + (168 << 16) + (1 << 8)}));
        cases.add(new Tuple<>("192.168.128.0/17",
            new long[]{(192L << 24) + (168 << 16) + (128 << 8), (192L << 24) + (168 << 16) + (128 << 8) + (1 << 15)}));
        cases.add(new Tuple<>("128.0.0.0/1", new long[]{128L << 24, (128L << 24) + (1L << 31)})); // edge case
        cases.add(new Tuple<>("0.0.0.0/0", new long[]{0, 1L << 32})); // edge case
        cases.add(new Tuple<>("0.0.0.0/1", new long[]{0, 1L << 31})); // edge case
        cases.add(new Tuple<>(
                "192.168.1.1/32",
                new long[]{(192L << 24) + (168L << 16) + (1L << 8) + 1L, (192L << 24) + (168L << 16) + (1L << 8) + 1L + 1})
        ); // edge case
        for (Tuple<String, long[]> test : cases) {
            long[] actual = Cidrs.cidrMaskToMinMax(test.v1());
            assertArrayEquals(test.v1(), test.v2(), actual);
        }
    }

    public void testInvalidSpecificOctetCases() {
        List<String> cases = new ArrayList<>();
        cases.add("256.0.0.0/8"); // first octet out of range
        cases.add("255.256.0.0/16"); // second octet out of range
        cases.add("255.255.256.0/24"); // third octet out of range
        cases.add("255.255.255.256/32"); // fourth octet out of range
        cases.add("abc.0.0.0/8"); // octet that can not be parsed
        cases.add("-1.0.0.0/8"); // first octet out of range
        cases.add("128.-1.0.0/16"); // second octet out of range
        cases.add("128.128.-1.0/24"); // third octet out of range
        cases.add("128.128.128.-1/32"); // fourth octet out of range

        for (String test : cases) {
            try {
                Cidrs.cidrMaskToMinMax(test);
                fail("expected invalid address");
            } catch (IllegalArgumentException e) {
                assertThat(e, hasToString(containsString("unable to parse")));
                assertThat(e, hasToString(containsString("as an IP address literal")));
            }
        }
    }

    public void testInvalidSpecificNetworkMaskCases() {
        List<String> cases = new ArrayList<>();
        cases.add("128.128.128.128/-1"); // network mask out of range
        cases.add("128.128.128.128/33"); // network mask out of range
        cases.add("128.128.128.128/abc"); // network mask that can not be parsed

        for (String test : cases) {
            try {
                Cidrs.cidrMaskToMinMax(test);
                fail("expected invalid network mask");
            } catch (IllegalArgumentException e) {
                assertThat(e, hasToString(containsString("network mask")));
            }
        }
    }

    public void testValidCombinations() {
        for (long i = 0; i < (1 << 16); i++) {
            String octetsString = Cidrs.octetsToString(Cidrs.longToOctets(i << 16));
            for (int mask = 16; mask <= 32; mask++) {
                String test = octetsString + "/" + mask;
                long[] actual = Cidrs.cidrMaskToMinMax(test);
                assertNotNull(test, actual);
                assertEquals(test, 2, actual.length);
                assertEquals(test, i << 16, actual[0]);
                assertEquals(test, (i << 16) + (1L << (32 - mask)), actual[1]);
            }
        }
    }

    public void testInvalidCombinations() {
        List<String> cases = new ArrayList<>();
        cases.add("192.168.0.1/24"); // invalid because fourth octet is not zero
        cases.add("192.168.1.0/16"); // invalid because third octet is not zero
        cases.add("192.1.0.0/8"); // invalid because second octet is not zero
        cases.add("128.0.0.0/0"); // invalid because first octet is not zero
        // create cases that have a bit set outside of the network mask
        int value = 1;
        for (int i = 0; i < 31; i++) {
            cases.add(Cidrs.octetsToCIDR(Cidrs.longToOctets(value), 32 - i - 1));
            value <<= 1;
        }

        for (String test : cases) {
            try {
                Cidrs.cidrMaskToMinMax(test);
                fail("expected invalid combination");
            } catch (IllegalArgumentException e) {
                assertThat(test, e, hasToString(containsString("invalid address/network mask combination")));
            }
        }
    }

    public void testRandomValidCombinations() {
        List<Tuple<String, Integer>> cases = new ArrayList<>();
        // random number of strings with valid octets and valid network masks
        for (int i = 0; i < randomIntBetween(1, 1024); i++) {
            int networkMask = randomIntBetween(0, 32);
            long mask = (1L << (32 - networkMask)) - 1;
            long address = randomLongInIPv4Range() & ~mask;
            cases.add(new Tuple<>(Cidrs.octetsToCIDR(Cidrs.longToOctets(address), networkMask), networkMask));
        }

        for (Tuple<String, Integer> test : cases) {
            long[] actual = Cidrs.cidrMaskToMinMax(test.v1());
            assertNotNull(test.v1(), actual);
            assertEquals(test.v1(), 2, actual.length);
            // assert the resulting block has the right size
            assertEquals(test.v1(), 1L << (32 - test.v2()), actual[1] - actual[0]);
        }
    }

    private long randomLongInIPv4Range() {
        return randomLong() & 0x00000000FFFFFFFFL;
    }
}
