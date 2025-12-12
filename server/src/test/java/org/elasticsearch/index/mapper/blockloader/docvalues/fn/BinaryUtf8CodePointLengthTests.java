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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.MockWarnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromCustomBinaryBlockLoader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.mapper.KeywordFieldMapper.MultiValuedBinaryDocValuesField.Ordering.NATURAL;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class BinaryUtf8CodePointLengthTests extends ESTestCase {

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

    public BinaryUtf8CodePointLengthTests(boolean blockAtATime, boolean multiValues, boolean missingValues) {
        this.blockAtATime = blockAtATime;
        this.multiValues = multiValues;
        this.missingValues = missingValues;
    }

    public void testCodePointLengthBinaryDocValues() throws IOException {
        int mvCount = 0;
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 10_000;
            int cardinality = between(16, 2048);
            for (int i = 0; i < docCount; i++) {
                var field = new KeywordFieldMapper.MultiValuedBinaryDocValuesField("field", NATURAL);
                field.add(new BytesRef("a".repeat(i % cardinality)));
                if (multiValues && i % cardinality == 0) {
                    field.add(new BytesRef("a".repeat((i % cardinality) + 1)));
                    mvCount++;
                }
                iw.addDocument(List.of(field));
            }
            if (missingValues) {
                iw.addDocument(List.of());
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                List<MockWarnings.MockWarning> expectedWarnings = new ArrayList<>();
                for (int i = 0; i < mvCount; i++) {
                    expectedWarnings.add(
                        new MockWarnings.MockWarning(IllegalArgumentException.class, "single-value function encountered multi-value")
                    );
                }

                var warnings = new MockWarnings();
                var stringsLoader = new BytesRefsFromCustomBinaryBlockLoader("field");
                var codePointsLoader = new Utf8CodePointsFromOrdsBlockLoader(warnings, "field");

                var stringsReader = stringsLoader.reader(ctx);
                var codePointsReader = codePointsLoader.reader(ctx);
                assertThat(codePointsReader, hasToString("Utf8CodePointsFromOrds.Binary"));
                BlockLoader.Docs docs = TestBlock.docs(ctx);
                try (
                    TestBlock strings = read(stringsLoader, stringsReader, docs);
                    TestBlock codePoints = read(codePointsLoader, codePointsReader, docs);
                ) {
                    checkBlocks(strings, codePoints);
                }
                assertThat(warnings.warnings(), equalTo(expectedWarnings));
                warnings.warnings().clear();

                stringsReader = stringsLoader.reader(ctx);
                codePointsReader = codePointsLoader.reader(ctx);
                for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
                    int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
                    for (int d = 0; d < docsArray.length; d++) {
                        docsArray[d] = i + d;
                    }
                    docs = TestBlock.docs(docsArray);
                    try (
                        TestBlock strings = read(stringsLoader, stringsReader, docs);
                        TestBlock codePoints = read(codePointsLoader, codePointsReader, docs);
                    ) {
                        checkBlocks(strings, codePoints);
                    }
                }
                assertThat(warnings.warnings(), equalTo(expectedWarnings));
            }
        }
    }

    private TestBlock read(BlockLoader loader, BlockLoader.AllReader reader, BlockLoader.Docs docs) throws IOException {
        BlockLoader.AllReader toUse = blockAtATime
            ? reader
            : new ForceDocAtATime(() -> loader.builder(TestBlock.factory(), docs.count()), reader);

        return (TestBlock) toUse.read(TestBlock.factory(), docs, 0, false);
    }

    private void checkBlocks(TestBlock strings, TestBlock codePoints) {
        for (int i = 0; i < strings.size(); i++) {
            Object str = strings.get(i);
            if (str instanceof List<?> || str == null) {
                assertThat(codePoints.get(i), nullValue());
                continue;
            }
            BytesRef bytes = (BytesRef) str;
            assertThat(codePoints.get(i), equalTo(bytes.length));
        }
    }
}
