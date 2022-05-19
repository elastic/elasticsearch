/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

public class ZeroBytesReferenceTests extends AbstractBytesReferenceTestCase {

    @Override
    protected BytesReference newBytesReference(int length) {
        return new ZeroBytesReference(length);
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) {
        return new ZeroBytesReference(length);
    }

    @Override
    public void testToBytesRefSharedPage() {
        // ZeroBytesReference doesn't share pages
    }

    @Override
    public void testSliceArrayOffset() {
        // the assertions in this test only work on real buffers
    }

    @Override
    public void testSliceToBytesRef() {
        // ZeroBytesReference shifts offsets
    }

}
