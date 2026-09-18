/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MergeInfo;
import org.elasticsearch.index.codec.vectors.es818.DirectIOHint;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

public class DirectIOContextTests extends ESTestCase {

    public void testReadContextsCarryTheHintAndDescribeNoMerge() {
        DirectIOContext search = DirectIOContext.searchRead(Set.of(DataAccessHint.RANDOM));
        assertEquals(IOContext.Context.DEFAULT, search.context());
        assertNull(search.mergeInfo());
        assertEquals(Set.of(DataAccessHint.RANDOM, DirectIOHint.INSTANCE), search.hints());

        DirectIOContext merge = DirectIOContext.mergeRead(Set.of());
        assertEquals(IOContext.Context.MERGE, merge.context());
        assertNull(merge.mergeInfo());
        assertEquals(Set.of(DirectIOHint.INSTANCE), merge.hints());
        expectThrows(UnsupportedOperationException.class, () -> merge.hints().add(DataAccessHint.RANDOM));
    }

    public void testMergeWriteCopiesTheMergeContext() {
        MergeInfo mergeInfo = new MergeInfo(randomIntBetween(1, 1000), randomLongBetween(1, 1 << 20), randomBoolean(), -1);
        DirectIOContext write = DirectIOContext.mergeWrite(IOContext.merge(mergeInfo));
        assertEquals(IOContext.Context.MERGE, write.context());
        assertSame(mergeInfo, write.mergeInfo());
        assertEquals(Set.of(DirectIOHint.INSTANCE), write.hints());
    }

    public void testWithHintsKeepsTheDirectIOHintAndTheMerge() {
        MergeInfo mergeInfo = new MergeInfo(1, 1, false, -1);
        IOContext rehinted = DirectIOContext.mergeWrite(IOContext.merge(mergeInfo)).withHints(DataAccessHint.SEQUENTIAL);
        assertEquals(IOContext.Context.MERGE, rehinted.context());
        assertSame(mergeInfo, rehinted.mergeInfo());
        assertEquals(Set.of(DataAccessHint.SEQUENTIAL, DirectIOHint.INSTANCE), rehinted.hints());
        assertEquals(Set.of(DirectIOHint.INSTANCE), rehinted.withHints().hints());
    }
}
