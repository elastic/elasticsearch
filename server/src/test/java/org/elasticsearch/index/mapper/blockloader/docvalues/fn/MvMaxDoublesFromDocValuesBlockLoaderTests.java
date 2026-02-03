/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoublesBlockLoader;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class MvMaxDoublesFromDocValuesBlockLoaderTests extends AbstractIntsFromDocValuesBlockLoaderTests {
    public MvMaxDoublesFromDocValuesBlockLoaderTests(boolean blockAtATime, boolean multiValues, boolean missingValues) {
        super(blockAtATime, multiValues, missingValues);
    }

    @Override
    protected void innerTest(LeafReaderContext ctx, int mvCount) throws IOException {
        var doublesLoader = new DoublesBlockLoader("field", Double::longBitsToDouble);
        var mvMaxDoublesLoader = new MvMaxDoublesFromDocValuesBlockLoader("field", Double::longBitsToDouble);

        var doublesReader = doublesLoader.reader(ctx);
        var mvMaxDoublesReader = mvMaxDoublesLoader.reader(ctx);
        assertThat(mvMaxDoublesReader, readerMatcher());
        BlockLoader.Docs docs = TestBlock.docs(ctx);
        try (
            TestBlock doubles = read(doublesLoader, doublesReader, ctx, docs);
            TestBlock maxDoubles = read(mvMaxDoublesLoader, mvMaxDoublesReader, ctx, docs);
        ) {
            checkBlocks(doubles, maxDoubles);
        }

        doublesReader = doublesLoader.reader(ctx);
        mvMaxDoublesReader = mvMaxDoublesLoader.reader(ctx);
        for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
            int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
            for (int d = 0; d < docsArray.length; d++) {
                docsArray[d] = i + d;
            }
            docs = TestBlock.docs(docsArray);
            try (
                TestBlock doubles = read(doublesLoader, doublesReader, ctx, docs);
                TestBlock maxDoubles = read(mvMaxDoublesLoader, mvMaxDoublesReader, ctx, docs);
            ) {
                checkBlocks(doubles, maxDoubles);
            }
        }
    }

    private Matcher<Object> readerMatcher() {
        if (multiValues) {
            return hasToString("MvMaxDoublesFromDocValues.Sorted");
        }
        return hasToString("DoublesFromDocValues.Singleton");
    }

    private void checkBlocks(TestBlock doubles, TestBlock mvMax) {
        for (int i = 0; i < doubles.size(); i++) {
            Object v = doubles.get(i);
            if (v == null) {
                assertThat(mvMax.get(i), nullValue());
                continue;
            }
            Double max = (Double) (v instanceof List<?> l ? l.stream().map(b -> (Double) b).max(Comparator.naturalOrder()).get() : v);
            assertThat(mvMax.get(i), equalTo(max));
        }
    }
}
