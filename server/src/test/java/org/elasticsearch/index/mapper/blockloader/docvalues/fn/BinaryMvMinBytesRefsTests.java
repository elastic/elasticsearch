/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class BinaryMvMinBytesRefsTests extends ESTestCase {

    @ParametersFactory(argumentFormatting = "blockAtATime=%s, multiValues=%s, missingValues=%s")
    public static List<Object[]> parameters() throws IOException {
        List<Object[]> parameters = new ArrayList<>();
        for (boolean blockAtATime : new boolean[] { true, false }) {
            for (boolean multiValues : new boolean[] { true, false }) {
                for (boolean missingValues : new boolean[] { true, false }) {
                    parameters.add(new Object[] { blockAtATime, multiValues, missingValues });
                }
            }
        }
        return parameters;
    }

    private final boolean blockAtATime;
    private final boolean multiValues;
    private final boolean missingValues;

    public BinaryMvMinBytesRefsTests(boolean blockAtATime, boolean multiValues, boolean missingValues) {
        this.blockAtATime = blockAtATime;
        this.multiValues = multiValues;
        this.missingValues = missingValues;
    }

    public void testBinaryMvMinDocValues() throws IOException {
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
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();

                var stringsLoader = new BytesRefsFromBinaryMultiSeparateCountBlockLoader("field");
                var mvMinLoader = new MvMinBytesRefsFromBinaryBlockLoader("field");
                var stringsReader = stringsLoader.reader(ctx);
                var mvMinReader = mvMinLoader.reader(ctx);
                assertThat(mvMinReader, readerMatcher());
                BlockLoader.Docs docs = TestBlock.docs(ctx);
                try (TestBlock strings = read(stringsLoader, stringsReader, docs); TestBlock mins = read(mvMinLoader, mvMinReader, docs)) {
                    checkBlocks(strings, mins);
                }

                stringsReader = stringsLoader.reader(ctx);
                mvMinReader = mvMinLoader.reader(ctx);
                for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
                    int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
                    for (int d = 0; d < docsArray.length; d++) {
                        docsArray[d] = i + d;
                    }
                    docs = TestBlock.docs(docsArray);
                    try (
                        TestBlock strings = read(stringsLoader, stringsReader, docs);
                        TestBlock mins = read(mvMinLoader, mvMinReader, docs)
                    ) {
                        checkBlocks(strings, mins);
                    }
                }

                stringsReader = stringsLoader.reader(ctx);
                mvMinReader = mvMinLoader.reader(ctx);
                for (int docId = 0; docId < ctx.reader().maxDoc(); docId++) {
                    docs = TestBlock.docs(docId);
                    try (
                        TestBlock strings = read(stringsLoader, stringsReader, docs);
                        TestBlock mins = read(mvMinLoader, mvMinReader, docs)
                    ) {
                        checkBlocks(strings, mins);
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

    private TestBlock read(BlockLoader loader, BlockLoader.AllReader reader, BlockLoader.Docs docs) throws IOException {
        BlockLoader.AllReader toUse = blockAtATime
            ? reader
            : new ForceDocAtATime(() -> loader.builder(TestBlock.factory(), docs.count()), reader);

        return (TestBlock) toUse.read(TestBlock.factory(), docs, 0, false);
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
