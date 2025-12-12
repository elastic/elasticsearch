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

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentString;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;

public class InetAddresses {
    private static final int IPV4_PART_COUNT = 4;
    private static final int IPV6_PART_COUNT = 8;
    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    public static boolean isInetAddress(String ipString) {
        byte[] utf8Bytes = ipString.getBytes(StandardCharsets.UTF_8);
        return ipStringToBytes(utf8Bytes, 0, utf8Bytes.length, false) != null;
    }

    public static String getIpOrHost(String ipString) {
        byte[] utf8Bytes = ipString.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = ipStringToBytes(utf8Bytes, 0, utf8Bytes.length, false);
        if (bytes == null) { // is not InetAddress
            return ipString;
        }
        return NetworkAddress.format(bytesToInetAddress(bytes));
    }

    /**
     * Encodes the given {@link XContentString} in binary encoding, always using 16 bytes for both IPv4 and IPv6 addresses.
     * This is how Lucene encodes IP addresses in {@link org.apache.lucene.document.InetAddressPoint}.
     *
     * @param ipString the IP address as a string
     * @return a byte array containing the binary representation of the IP address
     * @throws IllegalArgumentException if the argument is not a valid IP string literal
     */
    public static byte[] encodeAsIpv6(XContentString ipString) {
        XContentString.UTF8Bytes uft8Bytes = ipString.bytes();
        byte[] address = ipStringToBytes(uft8Bytes.bytes(), uft8Bytes.offset(), uft8Bytes.length(), true);
        // The argument was malformed, i.e. not an IP string literal.
        if (address == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "'%s' is not an IP string literal.", ipString.string()));
        }
        return address;
    }

    /**
     * Converts an IP address string to a byte array.
     * <p>
     * This method supports both IPv4 and IPv6 addresses, including dotted quad notation for IPv6.
     *
     * @param ipUtf8  the IP address as a byte array in UTF-8 encoding
     * @param offset  the starting index in the byte array
     * @param length  the length of the IP address string
     * @param asIpv6  if true, always returns a 16-byte array (IPv6 format), otherwise returns a 4-byte array for IPv4
     * @return a byte array representing the IP address, or null if the input is invalid
     */
    private static byte[] ipStringToBytes(byte[] ipUtf8, int offset, int length, boolean asIpv6) {
        // Make a first pass to categorize the characters in this string.
        int indexOfLastColon = -1;
        boolean hasDot = false;
        for (int i = offset; i < offset + length; i++) {
            byte c = ipUtf8[i];
            if ((c & 0x80) != 0) {
                return null;  // Only allow ASCII characters.
            } else if (c == '.') {
                hasDot = true;
            } else if (c == ':') {
                if (hasDot) {
                    return null;  // Colons must not appear after dots.
                }
                indexOfLastColon = i;
            } else if (c == '%') {
                if (i == offset + length - 1) {
                    return null;  // Filter out strings that end in % and have an empty scope ID.
                }
                length = i - offset;
                break; // Everything after a '%' is ignored (it's a Scope ID)
            }
        }

        // Now decide which address family to parse.
        if (indexOfLastColon >= 0) {
            if (hasDot) {
                ipUtf8 = convertDottedQuadToHex(ipUtf8, offset, length, indexOfLastColon);
                if (ipUtf8 == null) {
                    return null;
                }
                offset = 0;
                length = ipUtf8.length;
            }
            return textToNumericFormatV6(ipUtf8, offset, length);
        } else if (hasDot) {
            return textToNumericFormatV4(ipUtf8, offset, length, asIpv6);
        }
        return null;
    }

    private static byte[] convertDottedQuadToHex(byte[] ipUtf8, int offset, int length, int indexOfLastColon) {
        int quadOffset = indexOfLastColon - offset + 1; // +1 to include the colon
        assert quadOffset >= 0 : "Expected at least one colon in dotted quad IPv6 address";
        byte[] quad = textToNumericFormatV4(ipUtf8, offset + quadOffset, length - quadOffset, false);
        if (quad == null) {
            return null;
        }
        // initialPart(quadOffset) + penultimate(4) + ":"(1) + ultimate(4)
        byte[] result = new byte[quadOffset + 9];
        System.arraycopy(ipUtf8, offset, result, 0, quadOffset);
        appendHexBytes(result, quadOffset, quad[0], quad[1]); // penultimate part
        result[quadOffset + 4] = ':';
        appendHexBytes(result, quadOffset + 5, quad[2], quad[3]); // ultimate part
        return result;
    }

    static void appendHexBytes(byte[] result, int offset, byte b1, byte b2) {
        result[offset] = (byte) HEX_DIGITS[((b1 & 0xf0) >> 4)];
        result[offset + 1] = (byte) HEX_DIGITS[(b1 & 0x0f)];
        result[offset + 2] = (byte) HEX_DIGITS[((b2 & 0xf0) >> 4)];
        result[offset + 3] = (byte) HEX_DIGITS[(b2 & 0x0f)];
    }

    private static byte[] textToNumericFormatV4(byte[] ipUtf8, int offset, int length, boolean asIpv6) {
        byte[] bytes;
        byte octet;
        if (asIpv6) {
            bytes = new byte[IPV6_PART_COUNT * 2];
            System.arraycopy(CIDRUtils.IPV4_PREFIX, 0, bytes, 0, CIDRUtils.IPV4_PREFIX.length);
            octet = (byte) CIDRUtils.IPV4_PREFIX.length;
        } else {
            bytes = new byte[IPV4_PART_COUNT];
            octet = 0;
        }
        byte digits = 0;
        int current = 0;
        for (int i = offset; i < offset + length; i++) {
            byte c = ipUtf8[i];
            if (c == '.') {
                if (octet >= bytes.length /* too many octets */
                    || digits == 0 /* empty octet */
                    || current > 255 /* octet is outside a byte range */) {
                    return null;
                }
                bytes[octet++] = (byte) current;
                current = 0;
                digits = 0;
            } else if (c >= '0' && c <= '9') {
                if (digits != 0 && current == 0 /* octet contains leading 0 */) {
                    return null;
                }
                current = current * 10 + (c - '0');
                digits++;
            } else {
                return null;
            }
        }
        if (octet != bytes.length - 1 /* too many or too few octets */
            || digits == 0 /* empty octet */
            || current > 255 /* octet is outside a byte range */) {
            return null;
        }
        bytes[octet] = (byte) current;
        return bytes;
    }

    private static byte[] textToNumericFormatV6(byte[] ipUtf8, int offset, int length) {
        if (length < 2) {
            // IPv6 addresses must be at least 2 characters long (e.g., "::")
            return null;
        }
        if (ipUtf8[offset] == ':' && ipUtf8[offset + 1] != ':') {
            // Addresses can't start with a single colon
            return null;
        }
        if (ipUtf8[offset + length - 1] == ':' && ipUtf8[offset + length - 2] != ':') {
            // Addresses can't end with a single colon
            return null;
        }

        // An IPv6 address has 8 hextets (16-bit pieces), each represented by 1-4 hex digits
        // Total size: 16 bytes (128 bits)
        ByteBuffer bytes = ByteBuffer.allocate(IPV6_PART_COUNT * 2);

        // Find position of :: abbreviation if present
        int compressedHextetIndex = -1;
        int hextetIndex = 0;
        int currentHextetStart = offset;
        int currentHextet = 0;
        for (int i = offset; i < offset + length; i++) {
            byte c = ipUtf8[i];
            if (c == ':') {
                if (currentHextetStart == i) {
                    // Two colons in a row, indicating a compressed section
                    if (compressedHextetIndex >= 0 && i != offset + 1) {
                        // We've already seen a ::, can't have another
                        return null;
                    }
                    compressedHextetIndex = hextetIndex; // Mark the position of the compressed section
                } else {
                    if (putHextet(bytes, currentHextet) == false) {
                        return null;
                    }
                    currentHextet = 0;
                    hextetIndex++;
                }
                currentHextetStart = i + 1;
            } else if (c >= '0' && c <= '9') {
                // Valid hex digit
                currentHextet = currentHextet * 16 + (c - '0');
            } else if (c >= 'a' && c <= 'f') {
                // Valid hex digit in lowercase
                currentHextet = currentHextet * 16 + (c - 'a' + 10);
            } else if (c >= 'A' && c <= 'F') {
                // Valid hex digit in uppercase
                currentHextet = currentHextet * 16 + (c - 'A' + 10);
            } else {
                return null; // Invalid character
            }
        }
        // Handle the last hextet unless the IP address ends with ::
        if (currentHextetStart < offset + length) {
            if (putHextet(bytes, currentHextet) == false) {
                return null;
            }
            hextetIndex++;
        }

        if (compressedHextetIndex >= 0) {
            if (hextetIndex >= IPV6_PART_COUNT) {
                return null; // Invalid, too many hextets
            }
            shiftHextetsRight(bytes, compressedHextetIndex, hextetIndex);
        } else if (hextetIndex != IPV6_PART_COUNT) {
            return null; // Invalid, not enough hextets
        }

        return bytes.array();
    }

    private static void shiftHextetsRight(ByteBuffer bytes, int start, int end) {
        int shift = IPV6_PART_COUNT - end;
        for (int hextetIndexToShift = end - 1; hextetIndexToShift >= start; hextetIndexToShift--) {
            int bytesIndexBeforeShift = hextetIndexToShift * Short.BYTES;
            short hextetToShift = bytes.getShort(bytesIndexBeforeShift);
            bytes.putShort(bytesIndexBeforeShift, (short) 0);
            bytes.putShort(bytesIndexBeforeShift + shift * Short.BYTES, hextetToShift);
        }
    }

    private static boolean putHextet(ByteBuffer buf, int hextet) {
        if (buf.remaining() < 2) {
            return false;
        }
        if (hextet > 0xffff) {
            return false;
        }
        buf.putShort((short) hextet);
        return true;
    }

    /**
     * Returns the string representation of an {@link InetAddress} suitable
     * for inclusion in a URI.
     *
     * <p>For IPv4 addresses, this is identical to
     * {@link InetAddress#getHostAddress()}, but for IPv6 addresses it
     * compresses zeroes and surrounds the text with square brackets; for example
     * {@code "[2001:db8::1]"}.
     *
     * <p>Per section 3.2.2 of
     * <a target="_parent"
     *    href="http://tools.ietf.org/html/rfc3986#section-3.2.2"
     *  >http://tools.ietf.org/html/rfc3986</a>,
     * a URI containing an IPv6 string literal is of the form
     * {@code "http://[2001:db8::1]:8888/index.html"}.
     *
     * <p>Use of either {@link InetAddresses#toAddrString},
     * {@link InetAddress#getHostAddress()}, or this method is recommended over
     * {@link InetAddress#toString()} when an IP address string literal is
     * desired.  This is because {@link InetAddress#toString()} prints the
     * hostname and the IP address string joined by a "/".
     *
     * @param ip {@link InetAddress} to be converted to URI string literal
     * @return {@code String} containing URI-safe string literal
     */
    public static String toUriString(InetAddress ip) {
        if (ip instanceof Inet6Address) {
            return "[" + toAddrString(ip) + "]";
        }
        return toAddrString(ip);
    }

    /**
     * Returns the string representation of an {@link InetAddress}.
     *
     * <p>For IPv4 addresses, this is identical to
     * {@link InetAddress#getHostAddress()}, but for IPv6 addresses, the output
     * follows <a href="http://tools.ietf.org/html/rfc5952">RFC 5952</a>
     * section 4.  The main difference is that this method uses "::" for zero
     * compression, while Java's version uses the uncompressed form.
     *
     * <p>This method uses hexadecimal for all IPv6 addresses, including
     * IPv4-mapped IPv6 addresses such as "::c000:201".  The output does not
     * include a Scope ID.
     *
     * @param ip {@link InetAddress} to be converted to an address string
     * @return {@code String} containing the text-formatted IP address
     * @since 10.0
     */
    @SuppressForbidden(reason = "java.net.Inet4Address#getHostAddress() is fine no need to duplicate its code")
    public static String toAddrString(InetAddress ip) {
        if (ip == null) {
            throw new NullPointerException("ip");
        }
        if (ip instanceof Inet4Address inet4Address) {
            // For IPv4, Java's formatting is good enough.
            return inet4Address.getHostAddress();
        }
        if ((ip instanceof Inet6Address) == false) {
            throw new IllegalArgumentException("ip");
        }
        byte[] bytes = ip.getAddress();
        int[] hextets = new int[IPV6_PART_COUNT];
        for (int i = 0; i < hextets.length; i++) {
            hextets[i] = (bytes[2 * i] & 255) << 8 | bytes[2 * i + 1] & 255;
        }
        compressLongestRunOfZeroes(hextets);
        return hextetsToIPv6String(hextets);
    }

    /**
     * Identify and mark the longest run of zeroes in an IPv6 address.
     *
     * <p>Only runs of two or more hextets are considered.  In case of a tie, the
     * leftmost run wins.  If a qualifying run is found, its hextets are replaced
     * by the sentinel value -1.
     *
     * @param hextets {@code int[]} mutable array of eight 16-bit hextets
     */
    private static void compressLongestRunOfZeroes(int[] hextets) {
        int bestRunStart = -1;
        int bestRunLength = -1;
        int runStart = -1;
        for (int i = 0; i < hextets.length + 1; i++) {
            if (i < hextets.length && hextets[i] == 0) {
                if (runStart < 0) {
                    runStart = i;
                }
            } else if (runStart >= 0) {
                int runLength = i - runStart;
                if (runLength > bestRunLength) {
                    bestRunStart = runStart;
                    bestRunLength = runLength;
                }
                runStart = -1;
            }
        }
        if (bestRunLength >= 2) {
            Arrays.fill(hextets, bestRunStart, bestRunStart + bestRunLength, -1);
        }
    }

    /**
     * Convert a list of hextets into a human-readable IPv6 address.
     *
     * <p>In order for "::" compression to work, the input should contain negative
     * sentinel values in place of the elided zeroes.
     *
     * @param hextets {@code int[]} array of eight 16-bit hextets, or -1s
     */
    private static String hextetsToIPv6String(int[] hextets) {
        /*
         * While scanning the array, handle these state transitions:
         *   start->num => "num"     start->gap => "::"
         *   num->num   => ":num"    num->gap   => "::"
         *   gap->num   => "num"     gap->gap   => ""
         */
        StringBuilder buf = new StringBuilder(39);
        boolean lastWasNumber = false;
        for (int i = 0; i < hextets.length; i++) {
            boolean thisIsNumber = hextets[i] >= 0;
            if (thisIsNumber) {
                if (lastWasNumber) {
                    buf.append(':');
                }
                buf.append(Integer.toHexString(hextets[i]));
            } else {
                if (i == 0 || lastWasNumber) {
                    buf.append("::");
                }
            }
            lastWasNumber = thisIsNumber;
        }
        return buf.toString();
    }

    /**
     * Returns the {@link InetAddress} having the given string representation.
     *
     * <p>This deliberately avoids all nameservice lookups (e.g. no DNS).
     *
     * @param ipString {@code String} containing an IPv4 or IPv6 string literal, e.g.
     *     {@code "192.168.0.1"} or {@code "2001:db8::1"}
     * @return {@link InetAddress} representing the argument
     * @throws IllegalArgumentException if the argument is not a valid IP string literal
     */
    public static InetAddress forString(String ipString) {
        byte[] utf8Bytes = ipString.getBytes(StandardCharsets.UTF_8);
        return forString(utf8Bytes, 0, utf8Bytes.length);
    }

    /**
     * A variant of {@link #forString(String)} that accepts an {@link XContentString.UTF8Bytes} object,
     * which utilizes a more efficient implementation for parsing the IP address.
     */
    public static InetAddress forString(XContentString.UTF8Bytes bytes) {
        return forString(bytes.bytes(), bytes.offset(), bytes.length());
    }

    /**
     * A variant of {@link #forString(String)} that accepts a byte array,
     * which utilizes a more efficient implementation for parsing the IP address.
     */
    public static InetAddress forString(byte[] ipUtf8, int offset, int length) {
        byte[] addr = ipStringToBytes(ipUtf8, offset, length, false);

        // The argument was malformed, i.e. not an IP string literal.
        if (addr == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "'%s' is not an IP string literal.", new String(ipUtf8, offset, length, StandardCharsets.UTF_8))
            );
        }

        return bytesToInetAddress(addr);
    }

    /**
     * Convert a byte array into an InetAddress.
     *
     * {@link InetAddress#getByAddress} is documented as throwing a checked
     * exception "if IP address is of illegal length."  We replace it with
     * an unchecked exception, for use by callers who already know that addr
     * is an array of length 4 or 16.
     *
     * @param addr the raw 4-byte or 16-byte IP address in big-endian order
     * @return an InetAddress object created from the raw IP address
     */
    private static InetAddress bytesToInetAddress(byte[] addr) {
        try {
            return InetAddress.getByAddress(addr);
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Parse an IP address and its prefix length using the CIDR notation.
     * @throws IllegalArgumentException if the string is not formatted as {@code ip_address/prefix_length}
     * @throws IllegalArgumentException if the IP address is an IPv6-mapped ipv4 address
     * @throws IllegalArgumentException if the prefix length is not in 0-32 for IPv4 addresses and 0-128 for IPv6 addresses
     * @throws NumberFormatException if the prefix length is not an integer
     */
    public static Tuple<InetAddress, Integer> parseCidr(String maskedAddress) {
        String[] fields = maskedAddress.split("/");
        if (fields.length == 2) {
            final String addressString = fields[0];
            final InetAddress address = forString(addressString);
            if (addressString.contains(":") && address.getAddress().length == 4) {
                throw new IllegalArgumentException(
                    "CIDR notation is not allowed with IPv6-mapped IPv4 address ["
                        + addressString
                        + " as it introduces ambiguity as to whether the prefix length should be interpreted as a v4 prefix length or a"
                        + " v6 prefix length"
                );
            }
            final int prefixLength = Integer.parseInt(fields[1]);
            if (prefixLength < 0 || prefixLength > 8 * address.getAddress().length) {
                throw new IllegalArgumentException(
                    "Illegal prefix length ["
                        + prefixLength
                        + "] in ["
                        + maskedAddress
                        + "]. Must be 0-32 for IPv4 ranges, 0-128 for IPv6 ranges"
                );
            }
            return new Tuple<>(address, prefixLength);
        } else {
            throw new IllegalArgumentException("Expected [ip/prefix] but was [" + maskedAddress + "]");
        }
    }

    /**
     * Given an address and prefix length, returns the string representation of the range in CIDR notation.
     *
     * See {@link #toAddrString} for details on how the address is represented.
     */
    public static String toCidrString(InetAddress address, int prefixLength) {
        return new StringBuilder().append(toAddrString(address)).append("/").append(prefixLength).toString();
    }

    /**
     * Represents a range of IP addresses
     * @param lowerBound start of the ip range (inclusive)
     * @param upperBound end of the ip range (inclusive)
     */
    public record IpRange(InetAddress lowerBound, InetAddress upperBound) {}

    /**
     * Parse an IP address and its prefix length using the CIDR notation
     * into a range of ip addresses corresponding to it.
     * @param maskedAddress ip address range in a CIDR notation
     * @throws IllegalArgumentException if the string is not formatted as {@code ip_address/prefix_length}
     * @throws IllegalArgumentException if the IP address is an IPv6-mapped ipv4 address
     * @throws IllegalArgumentException if the prefix length is not in 0-32 for IPv4 addresses and 0-128 for IPv6 addresses
     * @throws NumberFormatException if the prefix length is not an integer
     */
    public static IpRange parseIpRangeFromCidr(String maskedAddress) {
        final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(maskedAddress);
        // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
        byte[] lower = cidr.v1().getAddress();
        byte[] upper = lower.clone();
        for (int i = cidr.v2(); i < 8 * lower.length; i++) {
            int m = 1 << 7 - (i & 7);
            lower[i >> 3] &= (byte) ~m;
            upper[i >> 3] |= (byte) m;
        }
        try {
            return new IpRange(InetAddress.getByAddress(lower), InetAddress.getByAddress(upper));
        } catch (UnknownHostException bogus) {
            throw new AssertionError(bogus);
        }
    }
}
