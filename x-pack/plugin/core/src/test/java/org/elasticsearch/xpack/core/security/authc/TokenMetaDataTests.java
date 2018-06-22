/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TokenMetaDataTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        final int numKeyAndTimestamps = scaledRandomIntBetween(1, 8);
        final List<KeyAndTimestamp> keyAndTimestampList = generateKeyAndTimestampListOfSize(numKeyAndTimestamps);
        final byte[] currentKeyHash = randomByteArrayOfLength(8);
        final TokenMetaData original = new TokenMetaData(keyAndTimestampList, currentKeyHash);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, tokenMetaData -> {
            final List<KeyAndTimestamp> copiedList = new ArrayList<>(keyAndTimestampList);
            final byte[] copyKeyHash = Arrays.copyOf(currentKeyHash, currentKeyHash.length);
            return new TokenMetaData(copiedList, copyKeyHash);
        }, tokenMetaData -> {
            final List<KeyAndTimestamp> modifiedList = generateKeyAndTimestampListOfSize(numKeyAndTimestamps);
            return new TokenMetaData(modifiedList, currentKeyHash);
        });

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, tokenMetaData -> {
            BytesStreamOutput out = new BytesStreamOutput();
            tokenMetaData.writeTo(out);
            return new TokenMetaData(out.bytes().streamInput());
        }, tokenMetaData -> {
            final byte[] modifiedKeyHash = randomByteArrayOfLength(8);
            return new TokenMetaData(keyAndTimestampList, modifiedKeyHash);
        });
    }

    private List<KeyAndTimestamp> generateKeyAndTimestampListOfSize(int size) {
        final List<KeyAndTimestamp> keyAndTimestampList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            keyAndTimestampList.add(
                new KeyAndTimestamp(new SecureString(randomAlphaOfLengthBetween(1, 12).toCharArray()), randomNonNegativeLong()));
        }
        return keyAndTimestampList;
    }
}
