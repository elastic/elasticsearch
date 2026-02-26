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
import org.elasticsearch.common.breaker.CircuitBreaker;
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

public class MvMinLongsFromDocValuesBlockLoaderTests extends AbstractLongsFromDocValuesBlockLoaderTests {
    public MvMinLongsFromDocValuesBlockLoaderTests(boolean multiValues, boolean missingValues) {
        super(multiValues, missingValues);
    }

    @Override
    protected void innerTest(CircuitBreaker breaker, LeafReaderContext ctx, int mvCount) throws IOException {
        var longsLoader = new LongsBlockLoader("field");
        var mvMinLongsLoader = new MvMinLongsFromDocValuesBlockLoader("field");
        BlockLoader.Docs docs = TestBlock.docs(ctx);

        try (var longsReader = longsLoader.reader(breaker, ctx); var mvMinLongsReader = mvMinLongsLoader.reader(breaker, ctx);) {
            assertThat(mvMinLongsReader, readerMatcher());
            try (TestBlock longs = read(longsReader, docs); TestBlock minLongs = read(mvMinLongsReader, docs);) {
                checkBlocks(longs, minLongs);
            }
        }

        try (var longsReader = longsLoader.reader(breaker, ctx); var mvMinLongsReader = mvMinLongsLoader.reader(breaker, ctx);) {
            for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
                int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
                for (int d = 0; d < docsArray.length; d++) {
                    docsArray[d] = i + d;
                }
                docs = TestBlock.docs(docsArray);
                try (TestBlock longs = read(longsReader, docs); TestBlock maxLongs = read(mvMinLongsReader, docs);) {
                    checkBlocks(longs, maxLongs);
                }
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
