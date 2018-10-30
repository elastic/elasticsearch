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

package org.elasticsearch.common.hash;

import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class MessageDigestsTests extends ESTestCase {
    private void assertHash(String expected, String test, MessageDigest messageDigest) {
        String actual = MessageDigests.toHexString(messageDigest.digest(test.getBytes(StandardCharsets.UTF_8)));
        assertEquals(expected, actual);
    }

    public void testMd5() throws Exception {
        assertHash("d41d8cd98f00b204e9800998ecf8427e", "", MessageDigests.md5());
        assertHash("900150983cd24fb0d6963f7d28e17f72", "abc", MessageDigests.md5());
        assertHash("8215ef0796a20bcaaae116d3876c664a",
            "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", MessageDigests.md5());
        assertHash("7707d6ae4e027c70eea2a935c2296f21",
            new String(new char[1000000]).replace("\0", "a"), MessageDigests.md5());
        assertHash("9e107d9d372bb6826bd81d3542a419d6", "The quick brown fox jumps over the lazy dog", MessageDigests.md5());
        assertHash("1055d3e698d289f2af8663725127bd4b", "The quick brown fox jumps over the lazy cog", MessageDigests.md5());
    }

    public void testSha1() throws Exception {
        assertHash("da39a3ee5e6b4b0d3255bfef95601890afd80709", "", MessageDigests.sha1());
        assertHash("a9993e364706816aba3e25717850c26c9cd0d89d", "abc", MessageDigests.sha1());
        assertHash("84983e441c3bd26ebaae4aa1f95129e5e54670f1",
            "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", MessageDigests.sha1());
        assertHash("34aa973cd4c4daa4f61eeb2bdbad27316534016f",
            new String(new char[1000000]).replace("\0", "a"), MessageDigests.sha1());
        assertHash("2fd4e1c67a2d28fced849ee1bb76e7391b93eb12", "The quick brown fox jumps over the lazy dog", MessageDigests.sha1());
        assertHash("de9f2c7fd25e1b3afad3e85a0bd17d9b100db4b3", "The quick brown fox jumps over the lazy cog", MessageDigests.sha1());
    }

    public void testSha256() throws Exception {
        assertHash("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "", MessageDigests.sha256());
        assertHash("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", "abc", MessageDigests.sha256());
        assertHash("248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
            "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", MessageDigests.sha256());
        assertHash("cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
            new String(new char[1000000]).replace("\0", "a"), MessageDigests.sha256());
        assertHash("d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
            "The quick brown fox jumps over the lazy dog", MessageDigests.sha256());
        assertHash("e4c4d8f3bf76b692de791a173e05321150f7a345b46484fe427f6acc7ecc81be",
            "The quick brown fox jumps over the lazy cog", MessageDigests.sha256());
    }

    public void testToHexString() throws Exception {
        BigInteger expected = BigInteger.probablePrime(256, random());
        byte[] bytes = expected.toByteArray();
        String hex = MessageDigests.toHexString(bytes);
        String zeros = new String(new char[2 * bytes.length]).replace("\0", "0");
        String expectedAsString = expected.toString(16);
        String expectedHex = zeros.substring(expectedAsString.length()) + expectedAsString;
        assertEquals(expectedHex, hex);
        BigInteger actual = new BigInteger(hex, 16);
        assertEquals(expected, actual);
    }
}
