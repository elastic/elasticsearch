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

public class MvMaxBooleansFromDocValuesBlockLoaderTests extends AbstractBooleansFromDocValuesBlockLoaderTests {
    public MvMaxBooleansFromDocValuesBlockLoaderTests(boolean blockAtATime, boolean multiValues, boolean missingValues) {
        super(blockAtATime, multiValues, missingValues);
    }

    @Override
    protected void innerTest(LeafReaderContext ctx, int mvCount) throws IOException {
        var booleansLoader = new BooleansBlockLoader("field");
        var mvMaxBooleansLoader = new MvMaxBooleansBlockLoader("field");

        var booleansReader = booleansLoader.reader(ctx);
        var mvMaxBooleansReader = mvMaxBooleansLoader.reader(ctx);
        assertThat(mvMaxBooleansReader, readerMatcher());
        BlockLoader.Docs docs = TestBlock.docs(ctx);
        try (
            TestBlock doubles = read(booleansLoader, booleansReader, ctx, docs);
            TestBlock maxDoubles = read(mvMaxBooleansLoader, mvMaxBooleansReader, ctx, docs);
        ) {
            checkBlocks(doubles, maxDoubles);
        }

        booleansReader = booleansLoader.reader(ctx);
        mvMaxBooleansReader = mvMaxBooleansLoader.reader(ctx);
        for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
            int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
            for (int d = 0; d < docsArray.length; d++) {
                docsArray[d] = i + d;
            }
            docs = TestBlock.docs(docsArray);
            try (
                TestBlock booleans = read(booleansLoader, booleansReader, ctx, docs);
                TestBlock maxBooleans = read(mvMaxBooleansLoader, mvMaxBooleansReader, ctx, docs);
            ) {
                checkBlocks(booleans, maxBooleans);
            }
        }
    }

    private Matcher<Object> readerMatcher() {
        if (multiValues) {
            return hasToString("MvMaxBooleansFromDocValues.Sorted");
        }
        return hasToString("BooleansFromDocValues.Singleton");
    }

    private void checkBlocks(TestBlock booleans, TestBlock mvMax) {
        for (int i = 0; i < booleans.size(); i++) {
            Object v = booleans.get(i);
            if (v == null) {
                assertThat(mvMax.get(i), nullValue());
                continue;
            }
            Boolean max = (Boolean) (v instanceof List<?> l ? l.stream().anyMatch(b -> (Boolean) b) : v);
            assertThat(mvMax.get(i), equalTo(max));
        }
    }
}
