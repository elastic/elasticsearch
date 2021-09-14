/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class TextCategorizationTests extends ESTestCase {

    public void testSimilarity() {
        TextCategorization lg = new TextCategorization(getTokens("foo", "bar", "baz", "biz"), 1, 1);
        TextCategorization.Similarity sims = lg.calculateSimilarity(getTokens("not", "matching", "anything", "nope"));
        assertThat(sims.getSimilarity(), equalTo(0.0));
        assertThat(sims.getWildCardCount(), equalTo(0));

        sims = lg.calculateSimilarity(getTokens("foo", "bar", "baz", "biz"));
        assertThat(sims.getSimilarity(), equalTo(1.0));
        assertThat(sims.getWildCardCount(), equalTo(0));

        sims = lg.calculateSimilarity(getTokens("foo", "fooagain", "notbar", "biz"));
        assertThat(sims.getSimilarity(), closeTo(0.5, 0.0001));
        assertThat(sims.getWildCardCount(), equalTo(0));
    }

    public void testAddLog() {
        TextCategorization lg = new TextCategorization(getTokens("foo", "bar", "baz", "biz"), 1, 1);
        lg.addLog(getTokens("foo", "bar", "baz", "bozo"), 2);
        assertThat(lg.getCount(), equalTo(3L));
        assertThat(lg.getCategorization(), arrayContaining(getTokens("foo", "bar", "baz", "*")));
    }

    static BytesRef[] getTokens(String... tokens) {
        BytesRef[] refs = new BytesRef[tokens.length];
        int i = 0;
        for (String token: tokens) {
            refs[i++] = new BytesRef(token);
        }
        return refs;
    }

}
