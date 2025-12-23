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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class MvMaxBytesRefsFromOrdsBlockLoaderTests extends AbstractFromOrdsBlockLoaderTests {
    public MvMaxBytesRefsFromOrdsBlockLoaderTests(
        boolean blockAtATime,
        boolean highCardinality,
        boolean multiValues,
        boolean missingValues
    ) {
        super(blockAtATime, highCardinality, multiValues, missingValues);
    }

    @Override
    protected void innerTest(LeafReaderContext ctx, int mvCount) throws IOException {
        var stringsLoader = new BytesRefsFromOrdsBlockLoader("field");
        var mvMaxLoader = new MvMaxBytesRefsFromOrdsBlockLoader("field");

        var stringsReader = stringsLoader.reader(ctx);
        var mvMaxReader = mvMaxLoader.reader(ctx);
        assertThat(mvMaxReader, readerMatcher());
        BlockLoader.Docs docs = TestBlock.docs(ctx);
        try (
            TestBlock strings = read(stringsLoader, stringsReader, ctx, docs);
            TestBlock maxStrings = read(mvMaxLoader, mvMaxReader, ctx, docs);
        ) {
            checkBlocks(strings, maxStrings);
        }

        stringsReader = stringsLoader.reader(ctx);
        mvMaxReader = mvMaxLoader.reader(ctx);
        for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
            int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
            for (int d = 0; d < docsArray.length; d++) {
                docsArray[d] = i + d;
            }
            docs = TestBlock.docs(docsArray);
            try (
                TestBlock strings = read(stringsLoader, stringsReader, ctx, docs);
                TestBlock maxStrings = read(mvMaxLoader, mvMaxReader, ctx, docs);
            ) {
                checkBlocks(strings, maxStrings);
            }
        }
    }

    private Matcher<Object> readerMatcher() {
        if (multiValues) {
            return hasToString("MvMaxBytesRefsFromOrds.SortedSet");
        }
        return hasToString("BytesRefsFromOrds.Singleton");
    }

    private void checkBlocks(TestBlock strings, TestBlock mvMin) {
        for (int i = 0; i < strings.size(); i++) {
            Object str = strings.get(i);
            if (str == null) {
                assertThat(mvMin.get(i), nullValue());
                continue;
            }
            BytesRef bytes = (BytesRef) (str instanceof List<?> l
                ? l.stream().map(b -> (BytesRef) b).max(Comparator.naturalOrder()).get()
                : str);
            assertThat(mvMin.get(i), equalTo(bytes));
        }
    }
}
