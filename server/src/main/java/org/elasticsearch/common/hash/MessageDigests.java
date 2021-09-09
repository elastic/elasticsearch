/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.hash;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * This MessageDigests class provides convenience methods for obtaining
 * thread local {@link MessageDigest} instances for MD5, SHA-1, SHA-256 and
 * SHA-512 message digests.
 */
public final class MessageDigests {

    private static ThreadLocal<MessageDigest> createThreadLocalMessageDigest(String digest) {
        return ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance(digest);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("unexpected exception creating MessageDigest instance for [" + digest + "]", e);
            }
        });
    }

    private static final ThreadLocal<MessageDigest> MD5_DIGEST = createThreadLocalMessageDigest("MD5");
    private static final ThreadLocal<MessageDigest> SHA_1_DIGEST = createThreadLocalMessageDigest("SHA-1");
    private static final ThreadLocal<MessageDigest> SHA_256_DIGEST = createThreadLocalMessageDigest("SHA-256");
    private static final ThreadLocal<MessageDigest> SHA_512_DIGEST = createThreadLocalMessageDigest("SHA-512");

    /**
     * Returns a {@link MessageDigest} instance for MD5 digests; note
     * that the instance returned is thread local and must not be
     * shared amongst threads.
     *
     * @return a thread local {@link MessageDigest} instance that
     * provides MD5 message digest functionality.
     */
    public static MessageDigest md5() {
        return get(MD5_DIGEST);
    }

    /**
     * Returns a {@link MessageDigest} instance for SHA-1 digests; note
     * that the instance returned is thread local and must not be
     * shared amongst threads.
     *
     * @return a thread local {@link MessageDigest} instance that
     * provides SHA-1 message digest functionality.
     */
    public static MessageDigest sha1() {
        return get(SHA_1_DIGEST);
    }

    /**
     * Returns a {@link MessageDigest} instance for SHA-256 digests;
     * note that the instance returned is thread local and must not be
     * shared amongst threads.
     *
     * @return a thread local {@link MessageDigest} instance that
     * provides SHA-256 message digest functionality.
     */
    public static MessageDigest sha256() {
        return get(SHA_256_DIGEST);
    }

    /**
     * Returns a {@link MessageDigest} instance for SHA-512 digests;
     * note that the instance returned is thread local and must not be
     * shared amongst threads.
     *
     * @return a thread local {@link MessageDigest} instance that
     * provides SHA-512 message digest functionality.
     */
    public static MessageDigest sha512() {
        return get(SHA_512_DIGEST);
    }

    private static MessageDigest get(ThreadLocal<MessageDigest> messageDigest) {
        MessageDigest instance = messageDigest.get();
        instance.reset();
        return instance;
    }

    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    /**
     * Format a byte array as a hex string.
     *
     * @param bytes the input to be represented as hex.
     * @return a hex representation of the input as a String.
     */
    public static String toHexString(byte[] bytes) {
        return new String(toHexCharArray(bytes));
    }

    /**
     * Encodes the byte array into a newly created hex char array, without allocating any other temporary variables.
     *
     * @param bytes the input to be encoded as hex.
     * @return the hex encoding of the input as a char array.
     */
    public static char[] toHexCharArray(byte[] bytes) {
        Objects.requireNonNull(bytes);
        final char[] result = new char[2 * bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            result[2 * i] = HEX_DIGITS[b >> 4 & 0xf];
            result[2 * i + 1] = HEX_DIGITS[b & 0xf];
        }
        return result;
    }

    /**
     * Updates the given digest with the given bytes reference and the returns the result of the digest.
     *
     * @param bytesReference bytes to add to digest
     * @param digest         digest to update and return the result for
     * @return digest result
     */
    public static byte[] digest(BytesReference bytesReference, MessageDigest digest) {
        final BytesRefIterator iterator = bytesReference.iterator();
        BytesRef ref;
        try {
            while ((ref = iterator.next()) != null) {
                digest.update(ref.bytes, ref.offset, ref.length);
            }
        } catch (IOException e) {
            throw new AssertionError("no actual IO happens here", e);
        }
        return digest.digest();
    }

}
