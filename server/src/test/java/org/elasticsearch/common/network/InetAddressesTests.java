/*
 * @notice
 * Copyright (C) 2008 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.network;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;
import org.hamcrest.Matchers;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class InetAddressesTests extends ESTestCase {
    public void testForStringBogusInput() {
        String[] bogusInputs = {
            "",
            "016.016.016.016",
            "016.016.016",
            "016.016",
            "016",
            "000.000.000.000",
            "000",
            "0x0a.0x0a.0x0a.0x0a",
            "0x0a.0x0a.0x0a",
            "0x0a.0x0a",
            "0x0a",
            "42.42.42.42.42",
            "42.42.42",
            "42.42",
            "42",
            "42..42.42",
            "42..42.42.42",
            "42.42.42.42.",
            "42.42.42.42...",
            ".42.42.42.42",
            "...42.42.42.42",
            "42.42.42.-0",
            "42.42.42.+0",
            ".",
            "...",
            "bogus",
            "bogus.com",
            "192.168.0.1.com",
            "12345.67899.-54321.-98765",
            "257.0.0.0",
            "42.42.42.-42",
            "3ffe::1.net",
            "3ffe::1::1",
            "1::2::3::4:5",
            "::7:6:5:4:3:2:",  // should end with ":0"
            ":6:5:4:3:2:1::",  // should begin with "0:"
            "2001::db:::1",
            "FEDC:9878",
            "+1.+2.+3.4",
            "1.2.3.4e0",
            "::7:6:5:4:3:2:1:0",  // too many parts
            "7:6:5:4:3:2:1:0::",  // too many parts
            "9:8:7:6:5:4:3::2:1",  // too many parts
            "0:1:2:3::4:5:6:7",  // :: must remove at least one 0.
            "3ffe:0:0:0:0:0:0:0:1",  // too many parts (9 instead of 8)
            "3ffe::10000",  // hextet exceeds 16 bits
            "3ffe::goog",
            "3ffe::-0",
            "3ffe::+0",
            "3ffe::-1",
            ":",
            ":::",
            "::1.2.3",
            "::1.2.3.4.5",
            "::1.2.3.4:",
            "1.2.3.4::",
            "2001:db8::1:",
            ":2001:db8::1",
            ":1:2:3:4:5:6:7",
            "1:2:3:4:5:6:7:",
            ":1:2:3:4:5:6:" };

        for (int i = 0; i < bogusInputs.length; i++) {
            try {
                InetAddresses.forString(bogusInputs[i]);
                fail("IllegalArgumentException expected for '" + bogusInputs[i] + "'");
            } catch (IllegalArgumentException expected) {
                // expected behavior
            }
            assertFalse(InetAddresses.isInetAddress(bogusInputs[i]));
        }
    }

    public void test3ff31() {
        try {
            InetAddresses.forString("3ffe:::1");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) {
            // expected behavior
        }
        assertFalse(InetAddresses.isInetAddress("016.016.016.016"));
    }

    public void testIpv6WithZeroScopeIdIsEqualToIpv6AddressWithOptionalScopeId() {
        assertThat(InetAddresses.forString("fdbd:dc00:111:222:0:0:0:333"), equalTo(InetAddresses.forString("fdbd:dc00:111:222::333")));
    }

    public void testForStringIPv4Input() throws UnknownHostException {
        String ipStr = "192.168.0.1";
        InetAddress ipv4Addr = null;
        // Shouldn't hit DNS, because it's an IP string literal.
        ipv4Addr = InetAddress.getByName(ipStr);
        assertEquals(ipv4Addr, InetAddresses.forString(ipStr));
        assertTrue(InetAddresses.isInetAddress(ipStr));
    }

    public void testForStringIPv6Input() throws UnknownHostException {
        String ipStr = "3ffe::1";
        InetAddress ipv6Addr = null;
        // Shouldn't hit DNS, because it's an IP string literal.
        ipv6Addr = InetAddress.getByName(ipStr);
        assertEquals(ipv6Addr, InetAddresses.forString(ipStr));
        assertTrue(InetAddresses.isInetAddress(ipStr));
    }

    public void testForStringIPv6WithScopeIdInput() throws java.io.IOException {
        final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        String scopeId = null;
        while (interfaces.hasMoreElements()) {
            final NetworkInterface nint = interfaces.nextElement();
            if (nint.isLoopback()) {
                scopeId = nint.getName();
                break;
            }
        }
        assertNotNull(scopeId);
        String ipStr = "0:0:0:0:0:0:0:1%" + scopeId;
        InetAddress ipv6Addr = InetAddress.getByName(ipStr);
        assertEquals(ipv6Addr, InetAddresses.forString(ipStr));
        assertTrue(InetAddresses.isInetAddress(ipStr));
    }

    public void testForStringIPv6WithInvalidScopeIdInput() {
        String ipStr = "0:0:0:0:0:0:0:1%";
        assertFalse(InetAddresses.isInetAddress(ipStr));
    }

    public void testForStringIPv6EightColons() throws UnknownHostException {
        String[] eightColons = { "::7:6:5:4:3:2:1", "::7:6:5:4:3:2:0", "7:6:5:4:3:2:1::", "0:6:5:4:3:2:1::", };

        for (int i = 0; i < eightColons.length; i++) {
            InetAddress ipv6Addr = null;
            // Shouldn't hit DNS, because it's an IP string literal.
            ipv6Addr = InetAddress.getByName(eightColons[i]);
            assertEquals(ipv6Addr, InetAddresses.forString(eightColons[i]));
            assertTrue(InetAddresses.isInetAddress(eightColons[i]));
        }
    }

    public void testConvertDottedQuadToHex() throws UnknownHostException {
        String[] ipStrings = { "7::0.128.0.127", "7::0.128.0.128", "7::128.128.0.127", "7::0.128.128.127", "::ffff:10.10.1.1" };

        for (String ipString : ipStrings) {
            // Shouldn't hit DNS, because it's an IP string literal.
            InetAddress ipv6Addr = InetAddress.getByName(ipString);
            assertEquals(ipv6Addr, InetAddresses.forString(ipString));
            int extraLength = randomInt(8);
            int offset = randomInt(extraLength);
            byte[] asBytes = ipString.getBytes(StandardCharsets.UTF_8);
            byte[] bytes = new byte[asBytes.length + extraLength];
            System.arraycopy(asBytes, 0, bytes, offset, asBytes.length);
            assertEquals(ipv6Addr, InetAddresses.forString(bytes, offset, asBytes.length));
            assertTrue(InetAddresses.isInetAddress(ipString));
        }
    }

    public void testToAddrStringIPv4() {
        // Don't need to test IPv4 much; it just calls getHostAddress().
        assertEquals("1.2.3.4", InetAddresses.toAddrString(InetAddresses.forString("1.2.3.4")));
    }

    public void testToAddrStringIPv6() {
        assertEquals("1:2:3:4:5:6:7:8", InetAddresses.toAddrString(InetAddresses.forString("1:2:3:4:5:6:7:8")));
        assertEquals("2001:0:0:4::8", InetAddresses.toAddrString(InetAddresses.forString("2001:0:0:4:0:0:0:8")));
        assertEquals("2001::4:5:6:7:8", InetAddresses.toAddrString(InetAddresses.forString("2001:0:0:4:5:6:7:8")));
        assertEquals("2001:0:3:4:5:6:7:8", InetAddresses.toAddrString(InetAddresses.forString("2001:0:3:4:5:6:7:8")));
        assertEquals("0:0:3::ffff", InetAddresses.toAddrString(InetAddresses.forString("0:0:3:0:0:0:0:ffff")));
        assertEquals("::4:0:0:0:ffff", InetAddresses.toAddrString(InetAddresses.forString("0:0:0:4:0:0:0:ffff")));
        assertEquals("::5:0:0:ffff", InetAddresses.toAddrString(InetAddresses.forString("0:0:0:0:5:0:0:ffff")));
        assertEquals("1::4:0:0:7:8", InetAddresses.toAddrString(InetAddresses.forString("1:0:0:4:0:0:7:8")));
        assertEquals("::", InetAddresses.toAddrString(InetAddresses.forString("0:0:0:0:0:0:0:0")));
        assertEquals("::1", InetAddresses.toAddrString(InetAddresses.forString("0:0:0:0:0:0:0:1")));
        assertEquals("2001:658:22a:cafe::", InetAddresses.toAddrString(InetAddresses.forString("2001:0658:022a:cafe::")));
        assertEquals("::102:304", InetAddresses.toAddrString(InetAddresses.forString("::1.2.3.4")));
        assertEquals("2001:db8::1:0:0:1", InetAddresses.toAddrString(InetAddresses.forString("2001:db8::1:0:0:1")));
    }

    public void testWithOffsets() {
        List<String> ipStrings = List.of(
            "1:2:3:4:5:6:7:8",
            "2001:0:0:4::8",
            "2001::4:5:6:7:8",
            "2001:0:3:4:5:6:7:8",
            "0:0:3::ffff",
            "::4:0:0:0:ffff",
            "::5:0:0:ffff",
            "1::4:0:0:7:8",
            "::",
            "::1",
            "2001:658:22a:cafe::",
            "::102:304",
            "2001:db8::1:0:0:1",
            "2001:db8::1:0:1:0"
        );
        for (String ipString : ipStrings) {
            byte[] bytes = ipString.getBytes(StandardCharsets.UTF_8);
            int extraLength = randomInt(8);
            int offset = randomInt(extraLength);
            byte[] bytesWithPadding = new byte[bytes.length + extraLength];
            System.arraycopy(bytes, 0, bytesWithPadding, offset, bytes.length);
            InetAddress addr = InetAddresses.forString(bytesWithPadding, offset, bytes.length);
            assertEquals(ipString, InetAddresses.toAddrString(addr));
        }
    }

    public void testToUriStringIPv4() {
        String ipStr = "1.2.3.4";
        InetAddress ip = InetAddresses.forString(ipStr);
        assertEquals("1.2.3.4", InetAddresses.toUriString(ip));
    }

    public void testToUriStringIPv6() {
        // Unfortunately the InetAddress.toString() method for IPv6 addresses
        // does not collapse contiguous shorts of zeroes with the :: abbreviation.
        String ipStr = "3ffe::1";
        InetAddress ip = InetAddresses.forString(ipStr);
        assertEquals("[3ffe::1]", InetAddresses.toUriString(ip));
    }

    public void testParseCidr() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> InetAddresses.parseCidr(""));
        assertThat(e.getMessage(), Matchers.containsString("Expected [ip/prefix] but was []"));

        e = expectThrows(IllegalArgumentException.class, () -> InetAddresses.parseCidr("192.168.1.42/33"));
        assertThat(e.getMessage(), Matchers.containsString("Illegal prefix length"));

        e = expectThrows(IllegalArgumentException.class, () -> InetAddresses.parseCidr("::1/129"));
        assertThat(e.getMessage(), Matchers.containsString("Illegal prefix length"));

        e = expectThrows(IllegalArgumentException.class, () -> InetAddresses.parseCidr("::ffff:0:0/96"));
        assertThat(e.getMessage(), Matchers.containsString("CIDR notation is not allowed with IPv6-mapped IPv4 address"));

        Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr("192.168.0.0/24");
        assertEquals(InetAddresses.forString("192.168.0.0"), cidr.v1());
        assertEquals(Integer.valueOf(24), cidr.v2());

        cidr = InetAddresses.parseCidr("::fffe:0:0/95");
        assertEquals(InetAddresses.forString("::fffe:0:0"), cidr.v1());
        assertEquals(Integer.valueOf(95), cidr.v2());

        cidr = InetAddresses.parseCidr("192.168.0.0/32");
        assertEquals(InetAddresses.forString("192.168.0.0"), cidr.v1());
        assertEquals(Integer.valueOf(32), cidr.v2());

        cidr = InetAddresses.parseCidr("::fffe:0:0/128");
        assertEquals(InetAddresses.forString("::fffe:0:0"), cidr.v1());
        assertEquals(Integer.valueOf(128), cidr.v2());
    }

    public void testEncodeAsIpv6() throws Exception {
        assertEquals(16, InetAddresses.encodeAsIpv6(new Text("::1")).length);
        assertEquals(16, InetAddresses.encodeAsIpv6(new Text("192.168.0.0")).length);
        assertEquals(
            "192.168.0.0",
            InetAddresses.toAddrString(InetAddress.getByAddress(InetAddresses.encodeAsIpv6(new Text("192.168.0.0"))))
        );
    }

    public void testAppendHexBytes() {
        for (int i = 0; i < 256; i++) {
            byte b1 = randomByte();
            byte b2 = randomByte();
            // The expected string is the hex representation of the two bytes, padded to 4 characters
            String expected = String.format(Locale.ROOT, "%1$04x", (b1 & 0xFF) << 8 | b2 & 0xFF);
            byte[] hex = new byte[4];
            InetAddresses.appendHexBytes(hex, 0, b1, b2);
            String actual = new String(hex, StandardCharsets.US_ASCII);
            assertEquals(expected, actual);
        }
    }
}
