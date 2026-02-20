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
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class MvMinLongsFromDocValuesBlockLoaderTests extends AbstractIntsFromDocValuesBlockLoaderTests {
    public MvMinLongsFromDocValuesBlockLoaderTests(boolean blockAtATime, boolean multiValues, boolean missingValues) {
        super(blockAtATime, multiValues, missingValues);
    }

    @Override
    protected void innerTest(LeafReaderContext ctx, int mvCount) throws IOException {
        var longsLoader = new LongsBlockLoader("field");
        var mvMinLongsLoader = new MvMinLongsFromDocValuesBlockLoader("field");

        var longsReader = longsLoader.reader(ctx);
        var mvMinLongsReader = mvMinLongsLoader.reader(ctx);
        assertThat(mvMinLongsReader, readerMatcher());
        BlockLoader.Docs docs = TestBlock.docs(ctx);
        try (
            TestBlock longs = read(longsLoader, longsReader, ctx, docs);
            TestBlock minLongs = read(mvMinLongsLoader, mvMinLongsReader, ctx, docs);
        ) {
            checkBlocks(longs, minLongs);
        }

        longsReader = longsLoader.reader(ctx);
        mvMinLongsReader = mvMinLongsLoader.reader(ctx);
        for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
            int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
            for (int d = 0; d < docsArray.length; d++) {
                docsArray[d] = i + d;
            }
            docs = TestBlock.docs(docsArray);
            try (
                TestBlock longs = read(longsLoader, longsReader, ctx, docs);
                TestBlock maxLongs = read(mvMinLongsLoader, mvMinLongsReader, ctx, docs);
            ) {
                checkBlocks(longs, maxLongs);
            }
        }
    }

    private Matcher<Object> readerMatcher() {
        if (multiValues) {
            return hasToString("MvMinLongsFromDocValues.Sorted");
        }
        return hasToString("LongsFromDocValues.Singleton");
    }

    private void checkBlocks(TestBlock longs, TestBlock mvMin) {
        for (int i = 0; i < longs.size(); i++) {
            Object v = longs.get(i);
            if (v == null) {
                assertThat(mvMin.get(i), nullValue());
                continue;
            }
            Long max = (Long) (v instanceof List<?> l ? l.stream().map(b -> (Long) b).min(Comparator.naturalOrder()).get() : v);
            assertThat(mvMin.get(i), equalTo(max));
        }
    }
}
