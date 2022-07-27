/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.hash;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasLength;

public class MessageDigestsTests extends ESTestCase {

    private void assertHexString(String expected, byte[] bytes) {
        final String actualDirect = MessageDigests.toHexString(bytes);
        assertThat(actualDirect, equalTo(expected));
    }

    private void assertHash(String expected, String test, MessageDigest messageDigest) throws IOException {
        final byte[] testBytes = test.getBytes(StandardCharsets.UTF_8);

        assertHexString(expected, messageDigest.digest(testBytes));
        assertHexString(expected, MessageDigests.digest(new BytesArray(testBytes), messageDigest));
        try (var in = new ByteArrayInputStream(testBytes)) {
            assertHexString(expected, MessageDigests.digest(in, messageDigest));
        }
    }

    public void testMd5() throws Exception {
        assertHash("d41d8cd98f00b204e9800998ecf8427e", "", MessageDigests.md5());
        assertHash("900150983cd24fb0d6963f7d28e17f72", "abc", MessageDigests.md5());
        assertHash("8215ef0796a20bcaaae116d3876c664a", "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", MessageDigests.md5());
        assertHash("7707d6ae4e027c70eea2a935c2296f21", new String(new char[1000000]).replace("\0", "a"), MessageDigests.md5());
        assertHash("9e107d9d372bb6826bd81d3542a419d6", "The quick brown fox jumps over the lazy dog", MessageDigests.md5());
        assertHash("1055d3e698d289f2af8663725127bd4b", "The quick brown fox jumps over the lazy cog", MessageDigests.md5());
    }

    public void testSha1() throws Exception {
        assertHash("da39a3ee5e6b4b0d3255bfef95601890afd80709", "", MessageDigests.sha1());
        assertHash("a9993e364706816aba3e25717850c26c9cd0d89d", "abc", MessageDigests.sha1());
        assertHash(
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
            "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
            MessageDigests.sha1()
        );
        assertHash("34aa973cd4c4daa4f61eeb2bdbad27316534016f", new String(new char[1000000]).replace("\0", "a"), MessageDigests.sha1());
        assertHash("2fd4e1c67a2d28fced849ee1bb76e7391b93eb12", "The quick brown fox jumps over the lazy dog", MessageDigests.sha1());
        assertHash("de9f2c7fd25e1b3afad3e85a0bd17d9b100db4b3", "The quick brown fox jumps over the lazy cog", MessageDigests.sha1());
    }

    public void testSha256() throws Exception {
        assertHash("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "", MessageDigests.sha256());
        assertHash("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", "abc", MessageDigests.sha256());
        assertHash(
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
            "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
            MessageDigests.sha256()
        );
        assertHash(
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
            new String(new char[1000000]).replace("\0", "a"),
            MessageDigests.sha256()
        );
        assertHash(
            "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
            "The quick brown fox jumps over the lazy dog",
            MessageDigests.sha256()
        );
        assertHash(
            "e4c4d8f3bf76b692de791a173e05321150f7a345b46484fe427f6acc7ecc81be",
            "The quick brown fox jumps over the lazy cog",
            MessageDigests.sha256()
        );
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

    public void testDigestFromStreamWithMultipleBlocks() throws Exception {
        final String longString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".repeat(1000);
        assertThat(longString, hasLength(26_000));

        try (InputStream in = getInputStream(longString)) {
            final byte[] md5 = MessageDigests.digest(in, MessageDigests.md5());
            assertThat(MessageDigests.toHexString(md5), equalTo("5c48e92239a655cfe1762851c6708ddb"));
        }
        try (InputStream in = getInputStream(longString)) {
            final byte[] md5 = MessageDigests.digest(in, MessageDigests.sha1());
            assertThat(MessageDigests.toHexString(md5), equalTo("e363dfc35f4d170906aafcbb6b1f6fd1ae854808"));
        }
        try (InputStream in = getInputStream(longString)) {
            final byte[] md5 = MessageDigests.digest(in, MessageDigests.sha256());
            assertThat(MessageDigests.toHexString(md5), equalTo("e59a4d700410ce60f912bd6e5b24f77230cbc68b27838c5a9c06daef94737a8a"));
        }
    }

    public void testDigestFromStreamWithExactlyOneBlock() throws Exception {
        final String blockString = "ABCDEFGHIJKLMNOP".repeat(64);
        assertThat(blockString, hasLength(MessageDigests.STREAM_DIGEST_BLOCK_SIZE));

        try (InputStream in = getInputStream(blockString)) {
            final byte[] md5 = MessageDigests.digest(in, MessageDigests.md5());
            assertThat(MessageDigests.toHexString(md5), equalTo("2eda00073add15c6ee5c848797f8c0f4"));
        }
        try (InputStream in = getInputStream(blockString)) {
            final byte[] md5 = MessageDigests.digest(in, MessageDigests.sha1());
            assertThat(MessageDigests.toHexString(md5), equalTo("bb8275d97cb190cb02fd2c03e9bba2279955ace3"));
        }
        try (InputStream in = getInputStream(blockString)) {
            final byte[] md5 = MessageDigests.digest(in, MessageDigests.sha256());
            assertThat(MessageDigests.toHexString(md5), equalTo("36350546f9cc3cbd56d3b655ecae0e4281909d510687635b900ea7650976eb3b"));
        }
    }

    private InputStream getInputStream(String str) {
        InputStream in = randomBoolean()
            ? new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8))
            : new BytesArray(str).streamInput();
        if (randomBoolean()) {
            in = new BufferedInputStream(in);
        }
        return in;
    }
}
