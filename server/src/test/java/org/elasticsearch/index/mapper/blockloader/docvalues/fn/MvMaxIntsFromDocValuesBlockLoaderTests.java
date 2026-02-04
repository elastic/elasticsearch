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
import org.elasticsearch.index.mapper.blockloader.docvalues.IntsBlockLoader;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class MvMaxIntsFromDocValuesBlockLoaderTests extends AbstractIntsFromDocValuesBlockLoaderTests {
    public MvMaxIntsFromDocValuesBlockLoaderTests(boolean blockAtATime, boolean multiValues, boolean missingValues) {
        super(blockAtATime, multiValues, missingValues);
    }

    @Override
    protected void innerTest(LeafReaderContext ctx, int mvCount) throws IOException {
        var intsLoader = new IntsBlockLoader("field");
        var mvMaxIntsLoader = new MvMaxIntsFromDocValuesBlockLoader("field");

        var intsReader = intsLoader.reader(ctx);
        var mvMaxIntsReader = mvMaxIntsLoader.reader(ctx);
        assertThat(mvMaxIntsReader, readerMatcher());
        BlockLoader.Docs docs = TestBlock.docs(ctx);
        try (
            TestBlock ints = read(intsLoader, intsReader, ctx, docs);
            TestBlock minInts = read(mvMaxIntsLoader, mvMaxIntsReader, ctx, docs);
        ) {
            checkBlocks(ints, minInts);
        }

        intsReader = intsLoader.reader(ctx);
        mvMaxIntsReader = mvMaxIntsLoader.reader(ctx);
        for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
            int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
            for (int d = 0; d < docsArray.length; d++) {
                docsArray[d] = i + d;
            }
            docs = TestBlock.docs(docsArray);
            try (
                TestBlock ints = read(intsLoader, intsReader, ctx, docs);
                TestBlock maxInts = read(mvMaxIntsLoader, mvMaxIntsReader, ctx, docs);
            ) {
                checkBlocks(ints, maxInts);
            }
        }
    }

    private Matcher<Object> readerMatcher() {
        if (multiValues) {
            return hasToString("MvMaxIntsFromDocValues.Sorted");
        }
        return hasToString("IntsFromDocValues.Singleton");
    }

    private void checkBlocks(TestBlock ints, TestBlock mvMax) {
        for (int i = 0; i < ints.size(); i++) {
            Object v = ints.get(i);
            if (v == null) {
                assertThat(mvMax.get(i), nullValue());
                continue;
            }
            Integer max = (Integer) (v instanceof List<?> l ? l.stream().map(b -> (Integer) b).max(Comparator.naturalOrder()).get() : v);
            assertThat(mvMax.get(i), equalTo(max));
        }
    }
}
