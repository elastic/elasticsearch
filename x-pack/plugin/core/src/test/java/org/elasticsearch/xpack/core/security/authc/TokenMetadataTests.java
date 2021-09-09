/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TokenMetadataTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        final int numKeyAndTimestamps = scaledRandomIntBetween(1, 8);
        final List<KeyAndTimestamp> keyAndTimestampList = generateKeyAndTimestampListOfSize(numKeyAndTimestamps);
        final byte[] currentKeyHash = randomByteArrayOfLength(8);
        final TokenMetadata original = new TokenMetadata(keyAndTimestampList, currentKeyHash);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, tokenMetadata -> {
            final List<KeyAndTimestamp> copiedList = new ArrayList<>(keyAndTimestampList);
            final byte[] copyKeyHash = Arrays.copyOf(currentKeyHash, currentKeyHash.length);
            return new TokenMetadata(copiedList, copyKeyHash);
        }, tokenMetadata -> {
            final List<KeyAndTimestamp> modifiedList = generateKeyAndTimestampListOfSize(numKeyAndTimestamps);
            return new TokenMetadata(modifiedList, currentKeyHash);
        });

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, tokenMetadata -> {
            BytesStreamOutput out = new BytesStreamOutput();
            tokenMetadata.writeTo(out);
            return new TokenMetadata(out.bytes().streamInput());
        }, tokenMetadata -> {
            final byte[] modifiedKeyHash = randomByteArrayOfLength(8);
            return new TokenMetadata(keyAndTimestampList, modifiedKeyHash);
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
