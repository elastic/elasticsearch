/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategory.TokenAndWeight;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class TokenListCategoryTests extends CategorizationTestCase {

    public void testCommonTokensSameOrder() {

        String baseString = "she sells seashells on the seashore";
        List<TokenAndWeight> baseTokenIds = List.of(
            tw("she", 2),
            tw("sells", 2),
            tw("seashells", 2),
            tw("on", 2),
            tw("the", 2),
            tw("seashore", 2)
        );

        List<TokenAndWeight> baseUniqueTokenIds = baseTokenIds.stream().sorted().distinct().toList();

        TokenListCategory category = new TokenListCategory(1, baseString.length(), baseTokenIds, baseUniqueTokenIds, 1);

        String newString = "she sells ice cream on the seashore";
        List<TokenAndWeight> newTokenIds = List.of(
            tw("she", 2),
            tw("sells", 2),
            tw("ice", 2),
            tw("cream", 2),
            tw("on", 2),
            tw("the", 2),
            tw("seashore", 2)
        );
        List<TokenAndWeight> newUniqueTokenIds = newTokenIds.stream().sorted().distinct().toList();

        category.addString(newString.length(), newTokenIds, newUniqueTokenIds, 1);

        assertThat(category.getId(), equalTo(1));
        assertThat(category.getBaseWeightedTokenIds(), equalTo(baseTokenIds));
        assertThat(category.getBaseWeight(), equalTo(baseTokenIds.size() * 2));
        List<TokenAndWeight> expectedCommonUniqueTokenIds = List.of(
            tw("she", 2),
            tw("sells", 2),
            tw("on", 2),
            tw("the", 2),
            tw("seashore", 2)
        );
        assertThat(category.getCommonUniqueTokenIds(), equalTo(expectedCommonUniqueTokenIds));
        assertThat(category.getCommonUniqueTokenWeight(), equalTo(expectedCommonUniqueTokenIds.size() * 2));
        assertThat(category.getOrigUniqueTokenWeight(), equalTo(baseUniqueTokenIds.size() * 2));
        assertThat(category.getMaxUnfilteredStringLength(), equalTo(Math.max(baseString.length(), newString.length())));
        assertThat(category.getOrderedCommonTokenBeginIndex(), equalTo(0));
        assertThat(category.getOrderedCommonTokenEndIndex(), equalTo(6));
        assertThat(category.ramBytesUsed(), equalTo(category.ramBytesUsedSlow()));
    }

    public void testCommonTokensDifferentOrder() {

        String baseString = "she sells seashells on the seashore";
        List<TokenAndWeight> baseTokenIds = List.of(
            tw("she", 2),
            tw("sells", 2),
            tw("seashells", 2),
            tw("on", 2),
            tw("the", 2),
            tw("seashore", 2)
        );
        List<TokenAndWeight> baseUniqueTokenIds = baseTokenIds.stream().sorted().distinct().toList();

        TokenListCategory category = new TokenListCategory(1, baseString.length(), baseTokenIds, baseUniqueTokenIds, 1);

        String newString1 = "sells seashells on the seashore, she does";
        List<TokenAndWeight> newTokenIds1 = List.of(
            tw("sells", 2),
            tw("seashells", 2),
            tw("on", 2),
            tw("the", 2),
            tw("seashore", 2),
            tw("she", 2),
            tw("does", 2)
        );
        List<TokenAndWeight> newUniqueTokenIds1 = newTokenIds1.stream().sorted().distinct().toList();

        category.addString(newString1.length(), newTokenIds1, newUniqueTokenIds1, 1);

        assertThat(category.getId(), equalTo(1));
        assertThat(category.getBaseWeightedTokenIds(), equalTo(baseTokenIds));
        assertThat(category.getBaseWeight(), equalTo(baseTokenIds.size() * 2));
        assertThat(category.getCommonUniqueTokenIds(), equalTo(baseUniqueTokenIds));
        assertThat(category.getCommonUniqueTokenWeight(), equalTo(baseUniqueTokenIds.size() * 2));
        assertThat(category.getOrigUniqueTokenWeight(), equalTo(baseUniqueTokenIds.size() * 2));
        assertThat(category.getMaxUnfilteredStringLength(), equalTo(Math.max(baseString.length(), newString1.length())));
        assertThat(category.getOrderedCommonTokenBeginIndex(), equalTo(1));
        assertThat(category.getOrderedCommonTokenEndIndex(), equalTo(6));
        assertThat(category.ramBytesUsed(), equalTo(category.ramBytesUsedSlow()));

        String newString2 = "nice seashells can be found near the seashore";
        List<TokenAndWeight> newTokenIds2 = List.of(
            tw("nice", 2),
            tw("seashells", 2),
            tw("can", 2),
            tw("be", 2),
            tw("found", 2),
            tw("near", 2),
            tw("the", 2),
            tw("seashore", 2)
        );
        List<TokenAndWeight> newUniqueTokenIds2 = newTokenIds2.stream().sorted().distinct().toList();

        category.addString(newString2.length(), newTokenIds2, newUniqueTokenIds2, 1);

        assertThat(category.getId(), equalTo(1));
        assertThat(category.getBaseWeightedTokenIds(), equalTo(baseTokenIds));
        assertThat(category.getBaseWeight(), equalTo(baseTokenIds.size() * 2));
        List<TokenAndWeight> expectedCommonUniqueTokenIds = List.of(tw("seashells", 2), tw("the", 2), tw("seashore", 2));
        assertThat(category.getCommonUniqueTokenIds(), equalTo(expectedCommonUniqueTokenIds));
        assertThat(category.getCommonUniqueTokenWeight(), equalTo(expectedCommonUniqueTokenIds.size() * 2));
        assertThat(category.getOrigUniqueTokenWeight(), equalTo(baseUniqueTokenIds.size() * 2));
        assertThat(category.getMaxUnfilteredStringLength(), equalTo(Math.max(newString1.length(), newString2.length())));
        // The bounds go from {1, 6} to {2, 6} even though there are now only 3
        // common tokens, because the bounds reference the base token indices,
        // and the range needs to be filtered to exclude tokens that are not common.
        // (When the real reverse search is created tokens may also be filtered if
        // their cost is too high for the available budget, so this doesn't create
        // too much complexity outside of the unit test.)
        assertThat(category.getOrderedCommonTokenBeginIndex(), equalTo(2));
        assertThat(category.getOrderedCommonTokenEndIndex(), equalTo(6));
        assertThat(category.ramBytesUsed(), equalTo(category.ramBytesUsedSlow()));

        String newString3 = "the rock";
        List<TokenAndWeight> newTokenIds3 = List.of(tw("the", 2), tw("rock", 2));
        List<TokenAndWeight> newUniqueTokenIds3 = newTokenIds3.stream().sorted().distinct().toList();

        category.addString(newString3.length(), newTokenIds3, newUniqueTokenIds3, 1);

        assertThat(category.getId(), equalTo(1));
        assertThat(category.getBaseWeightedTokenIds(), equalTo(baseTokenIds));
        assertThat(category.getBaseWeight(), equalTo(baseTokenIds.size() * 2));
        expectedCommonUniqueTokenIds = List.of(tw("the", 2));
        assertThat(category.getCommonUniqueTokenIds(), equalTo(expectedCommonUniqueTokenIds));
        assertThat(category.getCommonUniqueTokenWeight(), equalTo(expectedCommonUniqueTokenIds.size() * 2));
        assertThat(category.getOrigUniqueTokenWeight(), equalTo(baseUniqueTokenIds.size() * 2));
        assertThat(category.getMaxUnfilteredStringLength(), equalTo(Math.max(newString2.length(), newString3.length())));
        // The bounds go from {2, 6} to {4, 5} as there's now only one common token
        // and it's in position 4.
        assertThat(category.getOrderedCommonTokenBeginIndex(), equalTo(4));
        assertThat(category.getOrderedCommonTokenEndIndex(), equalTo(5));
        assertThat(category.ramBytesUsed(), equalTo(category.ramBytesUsedSlow()));
    }

    public void testRoundTripBetweenNodes() {

        // Create two random categories
        TokenListCategory node1Category1 = createTestInstance(bytesRefHash, 1);
        TokenListCategory node1Category2 = createTestInstance(bytesRefHash, 2);

        // Serialize them
        SerializableTokenListCategory node1SerializableCategory1 = new SerializableTokenListCategory(node1Category1, bytesRefHash);
        SerializableTokenListCategory node1SerializableCategory2 = new SerializableTokenListCategory(node1Category2, bytesRefHash);

        SerializableTokenListCategory node2SerializableCategory1;
        SerializableTokenListCategory node2SerializableCategory2;
        try (
            CategorizationBytesRefHash node2BytesRefHash = new CategorizationBytesRefHash(
                new BytesRefHash(2048, BigArrays.NON_RECYCLING_INSTANCE)
            )
        ) {
            // By deserializing in the opposite order we'll make node2BytesRefHash give different IDs to bytesRefHash for the same token
            TokenListCategory node2Category2 = new TokenListCategory(2, node1SerializableCategory2, node2BytesRefHash);
            TokenListCategory node2Category1 = new TokenListCategory(1, node1SerializableCategory1, node2BytesRefHash);

            // The categories should be _effectively_ equal, but won't be exactly equal as they're using different token IDs
            assertThat(node2Category1, not(equalTo(node1Category1)));
            assertThat(node2Category2, not(equalTo(node1Category2)));

            // Serialize again on node 2
            node2SerializableCategory1 = new SerializableTokenListCategory(node2Category1, node2BytesRefHash);
            node2SerializableCategory2 = new SerializableTokenListCategory(node2Category2, node2BytesRefHash);
        }

        TokenListCategory node1RoundTrippedCategory1 = new TokenListCategory(1, node2SerializableCategory1, bytesRefHash);
        TokenListCategory node1RoundTrippedCategory2 = new TokenListCategory(2, node2SerializableCategory2, bytesRefHash);

        assertThat(node1RoundTrippedCategory1, equalTo(node1Category1));
        assertThat(node1RoundTrippedCategory1.ramBytesUsed(), equalTo(node1Category1.ramBytesUsed()));
        assertThat(node1RoundTrippedCategory1.ramBytesUsed(), equalTo(node1RoundTrippedCategory1.ramBytesUsedSlow()));
        assertThat(node1RoundTrippedCategory2, equalTo(node1Category2));
        assertThat(node1RoundTrippedCategory2.ramBytesUsed(), equalTo(node1Category2.ramBytesUsed()));
        assertThat(node1RoundTrippedCategory2.ramBytesUsed(), equalTo(node1RoundTrippedCategory2.ramBytesUsedSlow()));
    }

    public void testMissingCommonTokenWeightZeroForSupersets() {
        TokenListCategory category = createTestInstance(bytesRefHash, 1);
        List<TokenAndWeight> uniqueTokenIds = new ArrayList<>(category.getCommonUniqueTokenIds());
        for (int i = 0; i < 5; ++i) {
            assertThat(category.missingCommonTokenWeight(uniqueTokenIds), is(0));
            uniqueTokenIds.add(new TokenAndWeight(randomIntBetween(1, 10), randomIntBetween(1, 10)));
            uniqueTokenIds.sort(TokenAndWeight::compareTo);
        }
    }

    public static TokenListCategory createTestInstance(CategorizationBytesRefHash bytesRefHash, int id) {

        int unfilteredStringLength = 0;
        int numBaseTokens = randomIntBetween(3, 15);
        int numDeliberateDups = randomIntBetween(1, numBaseTokens / 2);
        List<TokenAndWeight> baseWeightedTokenIds = new ArrayList<>();
        for (int i = numDeliberateDups; i < numBaseTokens; ++i) {
            int stringLen = randomIntBetween(3, 20);
            unfilteredStringLength += stringLen;
            BytesRef token = new BytesRef(randomAlphaOfLength(stringLen));
            baseWeightedTokenIds.add(new TokenAndWeight(bytesRefHash.put(token), randomIntBetween(1, 5)));
        }
        // Add some deliberate duplicates
        for (int i = 0; i < numDeliberateDups; ++i) {
            baseWeightedTokenIds.add(baseWeightedTokenIds.get(randomIntBetween(0, baseWeightedTokenIds.size() - 1)));
        }
        unfilteredStringLength += randomIntBetween(numBaseTokens, numBaseTokens + 100);
        List<TokenAndWeight> uniqueWeightedTokenIds = baseWeightedTokenIds.stream()
            .collect(Collectors.groupingBy(TokenAndWeight::getTokenId, TreeMap::new, Collectors.summingInt(TokenAndWeight::getWeight)))
            .entrySet()
            .stream()
            .map(entry -> new TokenAndWeight(entry.getKey(), entry.getValue()))
            .toList();
        return new TokenListCategory(id, unfilteredStringLength, baseWeightedTokenIds, uniqueWeightedTokenIds, randomLongBetween(1, 10));
    }
}
