/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.AbstractBlockLoaderTestCase;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class BinaryMvMinBytesRefsBlockLoaderTests extends AbstractBlockLoaderTestCase {
    public BinaryMvMinBytesRefsBlockLoaderTests(boolean multiValues, boolean missingValues) {
        super(multiValues, missingValues);
    }

    @Override
    protected void test(CircuitBreaker breaker, CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrap) throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 10_000;
            int cardinality = between(16, 2048);
            for (int i = 0; i < docCount; i++) {
                var field = new MultiValuedBinaryDocValuesField.SeparateCount("field", false);
                int baseLength = (i % cardinality) + 1;
                char c = 'z';
                field.add(new BytesRef(("" + c).repeat(baseLength)));
                NumericDocValuesField countField;
                if (multiValues && i % cardinality == 0) {
                    int numValues = randomIntBetween(2, 8);
                    for (int j = 1; j <= numValues; j++) {
                        c--;
                        field.add(new BytesRef(("" + c).repeat(baseLength + j)));
                    }
                    countField = NumericDocValuesField.indexedField(field.countFieldName(), numValues);
                } else {
                    countField = NumericDocValuesField.indexedField(field.countFieldName(), 1);
                }
                iw.addDocument(List.of(field, countField));
            }
            if (missingValues) {
                iw.addDocument(List.of());
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = wrap.apply(iw.getReader())) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();

                var stringsLoader = new BytesRefsFromBinaryMultiSeparateCountBlockLoader("field");
                var mvMinLoader = new MvMinBytesRefsFromBinaryBlockLoader("field");
                try (var stringsReader = stringsLoader.reader(breaker, ctx); var mvMinReader = mvMinLoader.reader(breaker, ctx)) {
                    assertThat(mvMinReader, readerMatcher());
                    BlockLoader.Docs docs = TestBlock.docs(ctx);
                    try (TestBlock strings = read(stringsReader, docs); TestBlock mins = read(mvMinReader, docs)) {
                        checkBlocks(strings, mins);
                    }
                }
                try (var stringsReader = stringsLoader.reader(breaker, ctx); var mvMinReader = mvMinLoader.reader(breaker, ctx)) {
                    for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
                        int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
                        for (int d = 0; d < docsArray.length; d++) {
                            docsArray[d] = i + d;
                        }
                        BlockLoader.Docs docs = TestBlock.docs(docsArray);
                        try (TestBlock strings = read(stringsReader, docs); TestBlock mins = read(mvMinReader, docs)) {
                            checkBlocks(strings, mins);
                        }
                    }
                }

                try (var stringsReader = stringsLoader.reader(breaker, ctx); var mvMinReader = mvMinLoader.reader(breaker, ctx)) {
                    for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                        BlockLoader.Docs docs = TestBlock.docs(docId);
                        try (TestBlock strings = read(stringsReader, docs); TestBlock mins = read(mvMinReader, docs)) {
                            checkBlocks(strings, mins);
                        }
                    }
                }
            }
        }
    }

    private Matcher<Object> readerMatcher() {
        if (multiValues) {
            return hasToString("MvMinBytesRefsFromBinary.SeparateCount");
        }
        return hasToString("BlockDocValuesReader.Bytes");
    }

    private TestBlock read(BlockLoader.AllReader reader, BlockLoader.Docs docs) throws IOException {
        return (TestBlock) reader.read(TestBlock.factory(), docs, 0, false);
    }

    private void checkBlocks(TestBlock strings, TestBlock mvMin) {
        for (int i = 0; i < strings.size(); i++) {
            Object str = strings.get(i);
            if (str == null) {
                assertThat(mvMin.get(i), nullValue());
                continue;
            }
            BytesRef bytes = (BytesRef) (str instanceof List<?> list
                ? list.stream().map(b -> (BytesRef) b).min(Comparator.naturalOrder()).get()
                : str);
            assertThat(mvMin.get(i), equalTo(bytes));
        }
    }
}
