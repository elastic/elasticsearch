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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * This MessageDigests class provides convenience methods for obtaining
 * thread local {@link MessageDigest} instances for MD5, SHA-1, and
 * SHA-256 message digests.
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

}
