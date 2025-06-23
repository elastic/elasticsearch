/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class SerializableTokenListCategoryTests extends AbstractWireSerializingTestCase<SerializableTokenListCategory> {

    private CategorizationBytesRefHash bytesRefHash;

    @Before
    public void createHash() {
        bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(2048, BigArrays.NON_RECYCLING_INSTANCE));
    }

    @After
    public void destroyHash() {
        bytesRefHash.close();
    }

    public void testGetRegex() {
        StringBuilder expectedResult = new StringBuilder();

        int unfilteredStringLength = 0;
        int numBaseTokens = randomIntBetween(3, 15);
        List<TokenListCategory.TokenAndWeight> baseWeightedTokenIds = new ArrayList<>();
        for (int i = 0; i < numBaseTokens; ++i) {
            int stringLen = randomIntBetween(3, 20);
            unfilteredStringLength += stringLen;
            String string = randomAlphaOfLength(stringLen);
            BytesRef token = new BytesRef(string);
            baseWeightedTokenIds.add(new TokenListCategory.TokenAndWeight(bytesRefHash.put(token), randomIntBetween(1, 5)));
            expectedResult.append(i == 0 ? ".*?" : ".+?").append(string);
        }
        expectedResult.append(".*?");
        unfilteredStringLength += randomIntBetween(numBaseTokens, numBaseTokens + 100);
        List<TokenListCategory.TokenAndWeight> uniqueWeightedTokenIds = baseWeightedTokenIds.stream()
            .collect(
                Collectors.groupingBy(
                    TokenListCategory.TokenAndWeight::getTokenId,
                    TreeMap::new,
                    Collectors.summingInt(TokenListCategory.TokenAndWeight::getWeight)
                )
            )
            .entrySet()
            .stream()
            .map(entry -> new TokenListCategory.TokenAndWeight(entry.getKey(), entry.getValue()))
            .toList();
        TokenListCategory category = new TokenListCategory(
            1,
            unfilteredStringLength,
            baseWeightedTokenIds,
            uniqueWeightedTokenIds,
            randomLongBetween(1, 10)
        );

        assertThat(new SerializableTokenListCategory(category, bytesRefHash).getRegex(), equalTo(expectedResult.toString()));
    }

    @Override
    protected Writeable.Reader<SerializableTokenListCategory> instanceReader() {
        return SerializableTokenListCategory::new;
    }

    @Override
    protected SerializableTokenListCategory createTestInstance() {
        return createTestInstance(bytesRefHash);
    }

    @Override
    protected SerializableTokenListCategory mutateInstance(SerializableTokenListCategory instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static SerializableTokenListCategory createTestInstance(CategorizationBytesRefHash bytesRefHash) {
        return new SerializableTokenListCategory(
            TokenListCategoryTests.createTestInstance(bytesRefHash, randomIntBetween(1, 1000)),
            bytesRefHash
        );
    }
}
