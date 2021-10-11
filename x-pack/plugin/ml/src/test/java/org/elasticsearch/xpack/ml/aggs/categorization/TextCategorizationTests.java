/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class TextCategorizationTests extends ESTestCase {

    static BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private CategorizationBytesRefHash bytesRefHash;

    @Before
    public void createRefHash() {
        bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(1L, mockBigArrays()));
    }

    @After
    public void closeRefHash() throws IOException {
        bytesRefHash.close();
    }

    public void testSimilarity() {
        TextCategorization lg = new TextCategorization(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 1, 1);
        TextCategorization.Similarity sims = lg.calculateSimilarity(getTokens(bytesRefHash, "not", "matching", "anything", "nope"));
        assertThat(sims.getSimilarity(), equalTo(0.0));
        assertThat(sims.getWildCardCount(), equalTo(0));

        sims = lg.calculateSimilarity(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"));
        assertThat(sims.getSimilarity(), equalTo(1.0));
        assertThat(sims.getWildCardCount(), equalTo(0));

        sims = lg.calculateSimilarity(getTokens(bytesRefHash, "foo", "fooagain", "notbar", "biz"));
        assertThat(sims.getSimilarity(), closeTo(0.5, 0.0001));
        assertThat(sims.getWildCardCount(), equalTo(0));
    }

    public void testAddTokens() {
        TextCategorization lg = new TextCategorization(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 1, 1);
        lg.addTokens(getTokens(bytesRefHash, "foo", "bar", "baz", "bozo"), 2);
        assertThat(lg.getCount(), equalTo(3L));
        assertArrayEquals(lg.getCategorization(), getTokens(bytesRefHash, "foo", "bar", "baz", "*"));
    }

    static int[] getTokens(CategorizationBytesRefHash bytesRefHash, String... tokens) {
        BytesRef[] refs = new BytesRef[tokens.length];
        int i = 0;
        for (String token : tokens) {
            refs[i++] = new BytesRef(token);
        }
        return bytesRefHash.getIds(refs);
    }

}
