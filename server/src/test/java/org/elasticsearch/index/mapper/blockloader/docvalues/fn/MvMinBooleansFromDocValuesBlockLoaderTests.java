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
import org.elasticsearch.index.mapper.blockloader.docvalues.BooleansBlockLoader;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class MvMinBooleansFromDocValuesBlockLoaderTests extends AbstractBooleansFromDocValuesBlockLoaderTests {
    public MvMinBooleansFromDocValuesBlockLoaderTests(boolean blockAtATime, boolean multiValues, boolean missingValues) {
        super(blockAtATime, multiValues, missingValues);
    }

    @Override
    protected void innerTest(LeafReaderContext ctx, int mvCount) throws IOException {
        var booleansLoader = new BooleansBlockLoader("field");
        var mvMinBooleansLoader = new MvMinBooleansBlockLoader("field");

        var booleansReader = booleansLoader.reader(ctx);
        var mvMinBooleansReader = mvMinBooleansLoader.reader(ctx);
        assertThat(mvMinBooleansReader, readerMatcher());
        BlockLoader.Docs docs = TestBlock.docs(ctx);
        try (
            TestBlock doubles = read(booleansLoader, booleansReader, ctx, docs);
            TestBlock minDoubles = read(mvMinBooleansLoader, mvMinBooleansReader, ctx, docs);
        ) {
            checkBlocks(doubles, minDoubles);
        }

        booleansReader = booleansLoader.reader(ctx);
        mvMinBooleansReader = mvMinBooleansLoader.reader(ctx);
        for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
            int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
            for (int d = 0; d < docsArray.length; d++) {
                docsArray[d] = i + d;
            }
            docs = TestBlock.docs(docsArray);
            try (
                TestBlock booleans = read(booleansLoader, booleansReader, ctx, docs);
                TestBlock minBooleans = read(mvMinBooleansLoader, mvMinBooleansReader, ctx, docs);
            ) {
                checkBlocks(booleans, minBooleans);
            }
        }
    }

    private Matcher<Object> readerMatcher() {
        if (multiValues) {
            return hasToString("MvMinBooleansFromDocValues.Sorted");
        }
        return hasToString("BooleansFromDocValues.Singleton");
    }

    private void checkBlocks(TestBlock booleans, TestBlock mvMin) {
        for (int i = 0; i < booleans.size(); i++) {
            Object v = booleans.get(i);
            if (v == null) {
                assertThat(mvMin.get(i), nullValue());
                continue;
            }
            Boolean max = (Boolean) (v instanceof List<?> l ? l.stream().allMatch(b -> (Boolean) b) : v);
            assertThat(mvMin.get(i), equalTo(max));
        }
    }
}
