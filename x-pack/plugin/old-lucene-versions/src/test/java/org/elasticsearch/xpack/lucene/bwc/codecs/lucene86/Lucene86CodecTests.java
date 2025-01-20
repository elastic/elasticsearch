/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene86;

import org.apache.lucene.codecs.Codec;
import org.elasticsearch.test.ESTestCase;

public class Lucene86CodecTests extends ESTestCase {

    private final Codec codec;

    public Lucene86CodecTests() {
        this.codec = new BWCLucene86Codec();
    }

    public void testNormsFormatUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, codec::normsFormat);
    }

    public void testTermVectorsFormatUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, codec::termVectorsFormat);
    }

    public void testKnnVectorsFormatUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, codec::knnVectorsFormat);
    }
}
