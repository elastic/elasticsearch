/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import static org.elasticsearch.index.mapper.IpPrefixAutomatonUtil.buildIpPrefixAutomaton;
import static org.elasticsearch.index.mapper.IpPrefixAutomatonUtil.parseIp6Prefix;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class IpPrefixAutomatonUtilTests extends ESTestCase {

    public void testCreateIp4PrefixAutomaton() throws UnknownHostException {
        InetAddress randomIp = randomIp(true);
        String ipString = NetworkAddress.format(randomIp);

        // get a random prefix, some emphasis on shorter ones, and compile a prefix automaton for it
        String randomPrefix = ipString.substring(0, randomBoolean() ? randomIntBetween(1, 6) : randomIntBetween(1, ipString.length()));
        CompiledAutomaton ip4Automaton = compileAutomaton(IpPrefixAutomatonUtil.createIp4Automaton(randomPrefix));

        // check that the original ip is accepted
        assertTrue(ip4Automaton.runAutomaton.run(randomIp.getAddress(), 0, randomIp.getAddress().length));

        // check that another random ip that doesn't have the same prefix isn't accepted
        byte[] nonMatchingIp = randomValueOtherThanMany(ipv4 -> {
            try {
                return NetworkAddress.format(InetAddress.getByAddress(ipv4)).startsWith(randomPrefix);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }, () -> randomByteArrayOfLength(4));
        assertFalse(ip4Automaton.runAutomaton.run(nonMatchingIp, 0, nonMatchingIp.length));

        // no bytes sequence longer than four bytes should be accepted
        byte[] fiveBytes = Arrays.copyOf(randomIp.getAddress(), 5);
        fiveBytes[4] = randomByte();
        assertFalse(ip4Automaton.runAutomaton.run(fiveBytes, 0, 5));

        // the empty prefix should create an automaton that accepts every four bytes address
        CompiledAutomaton acceptAll = compileAutomaton(IpPrefixAutomatonUtil.createIp4Automaton(""));
        assertTrue(acceptAll.runAutomaton.run(randomByteArrayOfLength(4), 0, 4));
    }

    public void testIncompleteDecimalGroupAutomaton() throws UnknownHostException {
        for (int p = 0; p <= 255; p++) {
            String prefix = String.valueOf(p);
            Automaton automaton = IpPrefixAutomatonUtil.INCOMPLETE_IP4_GROUP_AUTOMATON_LOOKUP.get(Integer.parseInt(prefix));
            CompiledAutomaton compiledAutomaton = compileAutomaton(automaton);
            for (int i = 0; i < 256; i++) {
                if (String.valueOf(i).startsWith(prefix)) {
                    assertTrue(compiledAutomaton.runAutomaton.run(new byte[] { (byte) i }, 0, 1));
                } else {
                    assertFalse(compiledAutomaton.runAutomaton.run(new byte[] { (byte) i }, 0, 1));
                }
            }
        }
    }

    public void testBuildPrefixAutomaton() throws UnknownHostException {
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("10");
            assertFalse(accepts(a, "1.2.3.4"));
            assertTrue(accepts(a, "10.2.3.4"));
            assertFalse(accepts(a, "2.2.3.4"));
            assertFalse(accepts(a, "1::1"));
            assertTrue(accepts(a, "10::1"));
            assertTrue(accepts(a, "100::1"));
            assertTrue(accepts(a, "1000::1"));
            assertTrue(accepts(a, "1000::1.2.3.4"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("1");
            assertTrue(accepts(a, "1.2.3.4"));
            assertTrue(accepts(a, "10.2.3.4"));
            assertFalse(accepts(a, "2.2.3.4"));
            assertTrue(accepts(a, "1af::1:2"));
            assertTrue(accepts(a, "1f::1:2"));
            assertFalse(accepts(a, "::1:2"));
            assertTrue(accepts(a, "1cce:e003:0:0:9279:d8d3:ffff:ffff"));

        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("0");
            assertTrue(accepts(a, "0.2.3.4"));
            assertFalse(accepts(a, "::1.1.2.3"));
            assertFalse(accepts(a, "1c7:21d6:ffff:ffff:6852:f279::"));
            assertFalse(accepts(a, "0000::127.0.0.1"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("00");
            assertFalse(accepts(a, "0.2.3.4"));
            assertFalse(accepts(a, "::1.1.2.3"));
            assertFalse(accepts(a, "c7:21d6:ffff:ffff:6852:f279::"));
            assertFalse(accepts(a, "0000::127.0.0.1"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("0:");
            assertTrue(accepts(a, "0.2.3.4"));
            assertTrue(accepts(a, "::1.1.2.3"));
            assertTrue(accepts(a, "0:21d6:ffff:ffff:6852:f279::"));
            assertTrue(accepts(a, "0000::127.0.0.1"));
            assertFalse(accepts(a, "0001:21d6:ffff:ffff:6852:f279::"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton(":");
            assertTrue(accepts(a, "0.2.3.4"));
            assertTrue(accepts(a, "::1.1.2.3"));
            assertTrue(accepts(a, "0:21d6:ffff:ffff:6852:f279::"));
            assertTrue(accepts(a, "::"));
            assertFalse(accepts(a, "1::1"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("1.");
            assertTrue(accepts(a, "1.2.3.4"));
            assertFalse(accepts(a, "10.2.3.4"));
            assertFalse(accepts(a, "2.2.3.4"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("1:2");
            assertTrue(accepts(a, "1:2::1"));
            assertTrue(accepts(a, "1:2a::1"));
            assertTrue(accepts(a, "1:2ab::1"));
            assertTrue(accepts(a, "1:2ab5::1"));
            assertFalse(accepts(a, "::1:2:3:4"));
            assertFalse(accepts(a, "10:2::3:4"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("::1:2");
            assertTrue(accepts(a, "::1:2"));
            assertTrue(accepts(a, "0:0:1:2::1"));
            assertFalse(accepts(a, "1:2ab::1"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("1::1.2");
            assertTrue(accepts(a, "1::1.2.3.4"));
            assertFalse(accepts(a, "1::1.3.2.4"));
            assertTrue(accepts(a, "1::1.22.3.4"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("201.");
            assertFalse(accepts(a, "c935:1902::643f:9e65:0:0"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("935");
            assertTrue(accepts(a, "0935:1902::643f:9e65:0:0"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("239");
            assertTrue(accepts(a, "239.27.240.24"));
            assertTrue(accepts(a, "239f:a360::25bb:828f:ffff:ffff"));
            assertFalse(accepts(a, "2309:a360::25bb:828f:ffff:ffff"));
        }
        {
            CompiledAutomaton a = buildIpPrefixAutomaton("255");
            assertTrue(accepts(a, "255.27.240.24"));
            assertTrue(accepts(a, "255:a360::25bb:828f:ffff:ffff"));
        }
    }

    private static boolean accepts(CompiledAutomaton compiledAutomaton, String address) throws UnknownHostException {
        byte[] encoded = InetAddressPoint.encode(InetAddress.getByName(address));
        return compiledAutomaton.runAutomaton.run(encoded, 0, encoded.length);
    }

    public void testParseIp6Prefix() {
        assertThat(parseIp6Prefix("123"), contains("123"));
        assertThat(parseIp6Prefix("123:12"), contains("123:", "12"));
        assertThat(parseIp6Prefix("123::12"), contains("123:", ":", "12"));
        assertThat(parseIp6Prefix("123::12:00ab"), contains("123:", ":", "12:", "00ab"));
        assertThat(parseIp6Prefix("123::12:00ah"), is(empty()));
        assertThat(parseIp6Prefix("12345:"), is(empty()));
        assertThat(
            parseIp6Prefix("2001:0db8:85a3:08d3:1319:8a2e:0370:7344"),
            contains("2001:", "0db8:", "85a3:", "08d3:", "1319:", "8a2e:", "0370:", "7344")
        );
        assertThat(parseIp6Prefix("2001:db8:0:8d3:0:8a2e:70:7344"), contains("2001:", "db8:", "0:", "8d3:", "0:", "8a2e:", "70:", "7344"));
        assertThat(parseIp6Prefix("2001:db8::1428:57ab"), contains("2001:", "db8:", ":", "1428:", "57ab"));
        assertThat(parseIp6Prefix("::ffff:7f00:1"), contains(":", ":", "ffff:", "7f00:", "1"));
        assertThat(parseIp6Prefix("::ffff:127.0.0.1"), contains(":", ":", "ffff:", "127.0.0.1"));
        assertThat(parseIp6Prefix("::127."), contains(":", ":", "127."));
        assertThat(parseIp6Prefix("::127.1.2"), contains(":", ":", "127.1.2"));
        assertThat(parseIp6Prefix("::127.1.1f"), is(empty()));
        assertThat(parseIp6Prefix("::127.1234.1.3"), is(empty()));
        assertThat(parseIp6Prefix("::127.1234.1:3"), is(empty()));
    }

    public void testAutomatonFromIPv6Group() throws UnknownHostException {
        expectThrows(AssertionError.class, () -> IpPrefixAutomatonUtil.automatonFromIPv6Group(""));
        expectThrows(AssertionError.class, () -> IpPrefixAutomatonUtil.automatonFromIPv6Group("12345"));

        // start with a 4-char hex string, build automaton for random prefix of it, then assure its accepted
        byte[] bytes = randomByteArrayOfLength(2);
        String randomHex = new String(Hex.encodeHex(bytes)).replaceAll("^0+", "");
        String prefix = randomHex.substring(0, randomIntBetween(1, randomHex.length()));
        Automaton automaton = IpPrefixAutomatonUtil.automatonFromIPv6Group(prefix);
        CompiledAutomaton compiledAutomaton = compileAutomaton(automaton);
        assertTrue(compiledAutomaton.runAutomaton.run(bytes, 0, bytes.length));

        // create random 4-char hex that isn't prefixed by the current prefix and check it isn't accepted
        byte[] badGroup = randomValueOtherThanMany(
            b -> new String(Hex.encodeHex(b)).replaceAll("^0+", "").startsWith(prefix),
            () -> randomByteArrayOfLength(2)
        );
        assertFalse(compiledAutomaton.runAutomaton.run(badGroup, 0, badGroup.length));
    }

    private static CompiledAutomaton compileAutomaton(Automaton automaton) {
        automaton = MinimizationOperations.minimize(automaton, Integer.MAX_VALUE);
        CompiledAutomaton compiledAutomaton = new CompiledAutomaton(
            automaton,
            null,
            false,
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            true
        );
        return compiledAutomaton;
    }
}
