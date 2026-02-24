/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.AbstractBlockLoaderTestCase;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDoublesFromDocValuesBlockLoaderTests extends AbstractBlockLoaderTestCase {
    public AbstractDoublesFromDocValuesBlockLoaderTests(boolean multiValues, boolean missingValues) {
        super(multiValues, missingValues);
    }

    protected abstract void innerTest(CircuitBreaker breaker, LeafReaderContext ctx, int mvCount) throws IOException;

    @Override
    protected final void test(CircuitBreaker breaker, CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrap)
        throws IOException {
        int mvCount = 0;
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 10_000;
            for (int i = 0; i < docCount; i++) {
                List<IndexableField> doc = new ArrayList<>(2);
                doc.add(field(i));
                if (multiValues && i % 100 == 0) {
                    doc.add(field((i % 100) + 1));
                    mvCount++;
                }
                iw.addDocument(doc);
            }
            if (missingValues) {
                iw.addDocument(List.of());
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = wrap.apply(iw.getReader())) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                innerTest(breaker, ctx, mvCount);
            }
        }
    }

    protected final TestBlock read(BlockLoader.AllReader reader, BlockLoader.Docs docs) throws IOException {
        return (TestBlock) reader.read(TestBlock.factory(), docs, 0, false);
    }

    private static DoubleField field(int i) {
        return new DoubleField("field", i * 1.1, Field.Store.NO);
    }
}
