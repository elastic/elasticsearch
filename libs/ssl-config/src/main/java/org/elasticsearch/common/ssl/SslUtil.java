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

package org.elasticsearch.common.ssl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Objects;

public final class SslUtil {

    private SslUtil() {
        // utility class
    }

    public static String calculateFingerprint(X509Certificate certificate) throws CertificateEncodingException {
        final MessageDigest sha1 = messageDigest("SHA-1");
        sha1.update(certificate.getEncoded());
        return toHexString(sha1.digest());
    }

    static MessageDigest messageDigest(String digestAlgorithm) {
        try {
            return MessageDigest.getInstance(digestAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new SslConfigException("unexpected exception creating MessageDigest instance for [" + digestAlgorithm + "]", e);
        }
    }

    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    /**
     * Format a byte array as a hex string.
     *
     * @param bytes the input to be represented as hex.
     * @return a hex representation of the input as a String.
     */
    static String toHexString(byte[] bytes) {
        return new String(toHexCharArray(bytes));
    }

    /**
     * Encodes the byte array into a newly created hex char array, without allocating any other temporary variables.
     *
     * @param bytes the input to be encoded as hex.
     * @return the hex encoding of the input as a char array.
     */
    static char[] toHexCharArray(byte[] bytes) {
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
