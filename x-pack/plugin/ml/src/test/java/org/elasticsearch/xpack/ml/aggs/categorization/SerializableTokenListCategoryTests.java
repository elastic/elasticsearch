/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.junit.After;
import org.junit.Before;

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

    @Override
    protected Writeable.Reader<SerializableTokenListCategory> instanceReader() {
        return SerializableTokenListCategory::new;
    }

    @Override
    protected SerializableTokenListCategory createTestInstance() {
        return createTestInstance(bytesRefHash);
    }

    public static SerializableTokenListCategory createTestInstance(CategorizationBytesRefHash bytesRefHash) {
        return new SerializableTokenListCategory(
            TokenListCategoryTests.createTestInstance(bytesRefHash, randomIntBetween(1, 1000)),
            bytesRefHash
        );
    }
}
