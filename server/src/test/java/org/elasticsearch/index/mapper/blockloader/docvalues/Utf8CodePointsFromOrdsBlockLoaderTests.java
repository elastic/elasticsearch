/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.mapper.blockloader.docvalues.Utf8CodePointsFromOrdsBlockLoader.LOW_CARDINALITY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;

public class Utf8CodePointsFromOrdsBlockLoaderTests extends ESTestCase {
    @ParametersFactory(argumentFormatting = "blockAtATime=%s, lowCardinality=%s, multiValues=%s, missingValues=%s")
    public static List<Object[]> parameters() throws IOException {
        List<Object[]> parameters = new ArrayList<>();
        for (boolean blockAtATime : new boolean[] { true, false }) {
            for (boolean lowCardinality : new boolean[] { true, false }) {
                for (boolean multiValues : new boolean[] { true, false }) {
                    for (boolean missingValues : new boolean[] { true, false }) {
                        parameters.add(new Object[] { blockAtATime, lowCardinality, multiValues, missingValues });
                    }
                }
            }
        }
        return parameters;
    }

    private final boolean blockAtATime;
    private final boolean lowCardinality;
    private final boolean multiValues;
    private final boolean missingValues;

    public Utf8CodePointsFromOrdsBlockLoaderTests(
        boolean blockAtATime,
        boolean highCardinality,
        boolean multiValues,
        boolean missingValues
    ) {
        this.blockAtATime = blockAtATime;
        this.lowCardinality = highCardinality;
        this.multiValues = multiValues;
        this.missingValues = missingValues;
    }

    public void test() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 10_000;
            int cardinality = lowCardinality ? between(1, LOW_CARDINALITY) : between(LOW_CARDINALITY + 1, LOW_CARDINALITY * 2);
            for (int i = 0; i < docCount; i++) {
                List<IndexableField> doc = new ArrayList<>(2);
                doc.add(field(i % cardinality));
                if (multiValues && i % cardinality == 0) {
                    doc.add(field((i % cardinality) + 1));
                }
                iw.addDocument(doc);
            }
            if (missingValues) {
                iw.addDocument(List.of());
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();

                var stringsLoader = new BytesRefsFromOrdsBlockLoader("field");
                var codePointsLoader = new Utf8CodePointsFromOrdsBlockLoader("field");
                var codePointsReader = codePointsLoader.reader(ctx);
                assertThat(codePointsReader, readerMatcher());

                try (
                    TestBlock strings = read(stringsLoader, stringsLoader.reader(ctx), ctx);
                    TestBlock codePoints = read(codePointsLoader, codePointsReader, ctx);
                ) {
                    for (int i = 0; i < strings.size(); i++) {
                        Object str = strings.get(i);
                        if (str instanceof List<?> || str == null) {
                            assertThat(codePoints.get(i), nullValue());
                            continue;
                        }
                        BytesRef bytes = (BytesRef) strings.get(i);
                        assertThat(codePoints.get(i), equalTo(bytes.length));
                    }
                }
            }
        }
    }

    private TestBlock read(BlockLoader loader, BlockLoader.AllReader reader, LeafReaderContext ctx) throws IOException {
        BlockLoader.AllReader toUse = blockAtATime
            ? reader
            : new ForceDocAtATime(() -> loader.builder(TestBlock.factory(), ctx.reader().numDocs()), reader);

        return (TestBlock) toUse.read(TestBlock.factory(), TestBlock.docs(ctx), 0, false);
    }

    private Matcher<Object> readerMatcher() {
        if (lowCardinality == false) {
            return hasToString("Utf8CodePointsFromOrds.Immediate");
        }
        if (multiValues) {
            return hasToString("Utf8CodePointsFromOrds.Ordinals");
        }
        return hasToString("Utf8CodePointsFromOrds.SingletonOrdinals");
    }

    private static KeywordFieldMapper.KeywordField field(int codePointCount) {
        return new KeywordFieldMapper.KeywordField(
            "field",
            new BytesRef("a".repeat(codePointCount)),
            KeywordFieldMapper.Defaults.FIELD_TYPE
        );
    }
}
