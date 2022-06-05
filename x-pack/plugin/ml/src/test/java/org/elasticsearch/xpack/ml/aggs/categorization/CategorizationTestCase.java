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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategory.TokenAndWeight;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

public abstract class CategorizationTestCase extends ESTestCase {

    protected CategorizationBytesRefHash bytesRefHash;

    @Before
    public void createHash() {
        bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(2048, BigArrays.NON_RECYCLING_INSTANCE));
    }

    @After
    public void destroyHash() {
        bytesRefHash.close();
    }

    protected TokenAndWeight tw(String token, int weight) {
        return new TokenAndWeight(bytesRefHash.put(new BytesRef(token.getBytes(StandardCharsets.UTF_8))), weight);
    }
}
