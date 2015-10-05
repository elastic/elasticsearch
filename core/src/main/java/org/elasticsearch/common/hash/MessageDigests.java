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

import org.elasticsearch.ElasticsearchException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MessageDigests {

    private static final MessageDigest MD5_DIGEST;
    private static final MessageDigest SHA_1_DIGEST;
    private static final MessageDigest SHA_256_DIGEST;

    static {
        try {
            MD5_DIGEST = MessageDigest.getInstance("MD5");
            SHA_1_DIGEST = MessageDigest.getInstance("SHA-1");
            SHA_256_DIGEST = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new ElasticsearchException("Unexpected exception creating MessageDigest instance", e);
        }
    }

    public static MessageDigest md5() {
        return clone(MD5_DIGEST);
    }

    public static MessageDigest sha1() {
        return clone(SHA_1_DIGEST);
    }

    public static MessageDigest sha256() {
        return clone(SHA_256_DIGEST);
    }

    private static MessageDigest clone(MessageDigest messageDigest) {
        try {
            return (MessageDigest) messageDigest.clone();
        } catch (CloneNotSupportedException e) {
            throw new ElasticsearchException("Unexpected exception cloning MessageDigest instance", e);
        }
    }

    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();
    public static String toHexString(byte[] bytes) {
        if (bytes == null) {
            throw new NullPointerException("bytes");
        }
        StringBuilder sb = new StringBuilder(2 * bytes.length);

        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            sb.append(HEX_DIGITS[b >> 4 & 0xf]).append(HEX_DIGITS[b & 0xf]);
        }

        return sb.toString();
    }
}
