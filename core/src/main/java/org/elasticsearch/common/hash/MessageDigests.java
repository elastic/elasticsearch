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
import java.util.Objects;

public class MessageDigests {

    private static ThreadLocal<MessageDigest> createThreadLocalMessageDigest(final String digest) {
        return new ThreadLocal<MessageDigest>() {
            @Override
            protected MessageDigest initialValue() {
                try {
                    return MessageDigest.getInstance(digest);
                } catch (NoSuchAlgorithmException e) {
                    throw new ElasticsearchException("unexpected exception creating MessageDigest instance for [" + digest + "]", e);
                }
            }
        };
    }

    private static final ThreadLocal<MessageDigest> MD5_DIGEST = createThreadLocalMessageDigest("MD5");
    private static final ThreadLocal<MessageDigest> SHA_1_DIGEST = createThreadLocalMessageDigest("SHA-1");
    private static final ThreadLocal<MessageDigest> SHA_256_DIGEST = createThreadLocalMessageDigest("SHA-256");

    public static MessageDigest md5() {
        return get(MD5_DIGEST);
    }

    public static MessageDigest sha1() {
        return get(SHA_1_DIGEST);
    }

    public static MessageDigest sha256() {
        return get(SHA_256_DIGEST);
    }

    private static MessageDigest get(ThreadLocal<MessageDigest> messageDigest) {
        MessageDigest instance = messageDigest.get();
        instance.reset();
        return instance;
    }

    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    public static String toHexString(byte[] bytes) {
        Objects.requireNonNull(bytes);
        StringBuilder sb = new StringBuilder(2 * bytes.length);

        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            sb.append(HEX_DIGITS[b >> 4 & 0xf]).append(HEX_DIGITS[b & 0xf]);
        }

        return sb.toString();
    }

}
