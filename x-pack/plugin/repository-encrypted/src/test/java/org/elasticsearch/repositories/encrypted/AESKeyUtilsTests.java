/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.test.ESTestCase;

import java.security.InvalidKeyException;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AESKeyUtilsTests extends ESTestCase {

    public void testWrapUnwrap() throws Exception {
        byte[] keyToWrapBytes = randomByteArrayOfLength(AESKeyUtils.KEY_LENGTH_IN_BYTES);
        SecretKey keyToWrap = new SecretKeySpec(keyToWrapBytes, "AES");
        byte[] wrappingKeyBytes = randomByteArrayOfLength(AESKeyUtils.KEY_LENGTH_IN_BYTES);
        SecretKey wrappingKey = new SecretKeySpec(wrappingKeyBytes, "AES");
        byte[] wrappedKey = AESKeyUtils.wrap(wrappingKey, keyToWrap);
        assertThat(wrappedKey.length, equalTo(AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES));
        SecretKey unwrappedKey = AESKeyUtils.unwrap(wrappingKey, wrappedKey);
        assertThat(unwrappedKey, equalTo(keyToWrap));
    }

    public void testComputeId() throws Exception {
        byte[] key1Bytes = randomByteArrayOfLength(AESKeyUtils.KEY_LENGTH_IN_BYTES);
        SecretKey key1 = new SecretKeySpec(key1Bytes, "AES");
        byte[] key2Bytes = randomByteArrayOfLength(AESKeyUtils.KEY_LENGTH_IN_BYTES);
        SecretKey key2 = new SecretKeySpec(key2Bytes, "AES");
        assertThat(AESKeyUtils.computeId(key1), not(equalTo(AESKeyUtils.computeId(key2))));
        assertThat(AESKeyUtils.computeId(key1), equalTo(AESKeyUtils.computeId(key1)));
        assertThat(AESKeyUtils.computeId(key2), equalTo(AESKeyUtils.computeId(key2)));
    }

    public void testFailedWrapUnwrap() throws Exception {
        byte[] toWrapBytes = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 };
        SecretKey keyToWrap = new SecretKeySpec(toWrapBytes, "AES");
        byte[] wrapBytes = new byte[] { 0, 0, 0, 0, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 0, 0, 0, 0 };
        SecretKey wrappingKey = new SecretKeySpec(wrapBytes, "AES");
        byte[] wrappedKey = AESKeyUtils.wrap(wrappingKey, keyToWrap);
        for (int i = 0; i < wrappedKey.length; i++) {
            wrappedKey[i] ^= 0xFFFFFFFF;
            expectThrows(InvalidKeyException.class, () -> AESKeyUtils.unwrap(wrappingKey, wrappedKey));
            wrappedKey[i] ^= 0xFFFFFFFF;
        }
    }
}
