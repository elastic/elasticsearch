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
import org.elasticsearch.index.mapper.blockloader.MockWarnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class BinaryByteLengthBlockLoaderTests extends AbstractBlockLoaderTestCase {
    public BinaryByteLengthBlockLoaderTests(boolean blockAtATime, boolean multiValues, boolean missingValues) {
        super(blockAtATime, multiValues, missingValues);
    }

    @Override
    protected void test(CircuitBreaker breaker, CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrap) throws IOException {
        int mvCount = 0;
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 10_000;
            int cardinality = between(16, 2048);
            for (int i = 0; i < docCount; i++) {
                var field = new MultiValuedBinaryDocValuesField.SeparateCount("field", false);
                field.add(new BytesRef("a".repeat(i % cardinality)));
                NumericDocValuesField countField;
                if (multiValues && i % cardinality == 0) {
                    field.add(new BytesRef("a".repeat((i % cardinality) + 1)));
                    mvCount++;
                    countField = NumericDocValuesField.indexedField(field.countFieldName(), 2);
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
                List<MockWarnings.MockWarning> expectedWarnings = new ArrayList<>();
                for (int i = 0; i < mvCount; i++) {
                    expectedWarnings.add(
                        new MockWarnings.MockWarning(IllegalArgumentException.class, "single-value function encountered multi-value")
                    );
                }

                var warnings = new MockWarnings();
                var stringsLoader = new BytesRefsFromBinaryMultiSeparateCountBlockLoader("field");
                var lengthLoader = new ByteLengthFromBytesRefDocValuesBlockLoader(warnings, "field");

                try (var stringsReader = stringsLoader.reader(breaker, ctx); var lengthReader = lengthLoader.reader(breaker, ctx)) {
                    if (multiValues) {
                        assertThat(lengthReader, hasToString("ByteLengthFromBytesRef.MultiValuedBinaryWithSeparateCounts"));
                    } else {
                        assertThat(lengthReader, hasToString("ByteLengthFromBytesRef.SingleValued"));
                    }

                    BlockLoader.Docs docs = TestBlock.docs(ctx);
                    try (
                        TestBlock strings = read(stringsLoader, stringsReader, docs);
                        TestBlock codePoints = read(lengthLoader, lengthReader, docs);
                    ) {
                        checkBlocks(strings, codePoints);
                    }
                    assertThat(warnings.warnings(), equalTo(expectedWarnings));
                    warnings.warnings().clear();
                }

                try (var stringsReader = stringsLoader.reader(breaker, ctx); var lengthReader = lengthLoader.reader(breaker, ctx)) {
                    for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
                        int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
                        for (int d = 0; d < docsArray.length; d++) {
                            docsArray[d] = i + d;
                        }
                        BlockLoader.Docs docs = TestBlock.docs(docsArray);
                        try (
                            TestBlock strings = read(stringsLoader, stringsReader, docs);
                            TestBlock codePoints = read(lengthLoader, lengthReader, docs);
                        ) {
                            checkBlocks(strings, codePoints);
                        }
                    }
                    assertThat(warnings.warnings(), equalTo(expectedWarnings));
                    warnings.warnings().clear();
                }

                // Testing fetching a single doc, which has a different code path
                try (var stringsReader = stringsLoader.reader(breaker, ctx); var lengthReader = lengthLoader.reader(breaker, ctx)) {
                    for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                        BlockLoader.Docs docs = TestBlock.docs(docId);
                        try (
                            TestBlock strings = read(stringsLoader, stringsReader, docs);
                            TestBlock codePoints = read(lengthLoader, lengthReader, docs);
                        ) {
                            checkBlocks(strings, codePoints);
                        }
                    }
                    assertThat(warnings.warnings(), equalTo(expectedWarnings));
                }
            }
        }
    }

    private TestBlock read(BlockLoader loader, BlockLoader.AllReader reader, BlockLoader.Docs docs) throws IOException {
        BlockLoader.AllReader toUse = blockAtATime
            ? reader
            : new ForceDocAtATime(() -> loader.builder(TestBlock.factory(), docs.count()), reader);

        return (TestBlock) toUse.read(TestBlock.factory(), docs, 0, false);
    }

    private void checkBlocks(TestBlock strings, TestBlock lengths) {
        for (int i = 0; i < strings.size(); i++) {
            Object str = strings.get(i);
            if (str instanceof List<?> || str == null) {
                assertThat(lengths.get(i), nullValue());
                continue;
            }
            BytesRef bytes = (BytesRef) str;
            assertThat(lengths.get(i), equalTo(bytes.length));
        }
    }
}
