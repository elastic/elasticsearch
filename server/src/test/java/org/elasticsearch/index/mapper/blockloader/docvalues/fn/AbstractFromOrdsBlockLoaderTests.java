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

import org.apache.http.annotation.Obsolete;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.AbstractBlockLoaderTestCase;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.TestBlock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.mapper.blockloader.docvalues.fn.Utf8CodePointsFromOrdsBlockLoader.LOW_CARDINALITY;

public abstract class AbstractFromOrdsBlockLoaderTests extends AbstractBlockLoaderTestCase {
    @ParametersFactory(argumentFormatting = "blockAtATime=%s, multiValues=%s, missingValues=%s, highCardinality=%s")
    public static List<Object[]> parameters() throws IOException {
        List<Object[]> parameters = new ArrayList<>();
        for (Object[] superParams : AbstractBlockLoaderTestCase.parameters()) {
            for (boolean lowCardinality : new boolean[] { true, false }) {
                Object[] myParams = new Object[superParams.length + 1];
                System.arraycopy(superParams, 0, myParams, 0, superParams.length);
                myParams[superParams.length] = lowCardinality;
                parameters.add(myParams);
            }
        }
        return parameters;
    }

    protected final boolean lowCardinality;

    public AbstractFromOrdsBlockLoaderTests(boolean blockAtATime, boolean multiValues, boolean missingValues, boolean highCardinality) {
        super(blockAtATime, multiValues, missingValues);
        this.lowCardinality = highCardinality;
    }

    protected abstract void innerTest(CircuitBreaker breaker, LeafReaderContext ctx, int mvCount) throws IOException;

    @Obsolete
    protected final void test(CircuitBreaker breaker, CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrap)
        throws IOException {
        int mvCount = 0;
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 10_000;
            int cardinality = lowCardinality ? between(1, LOW_CARDINALITY) : between(LOW_CARDINALITY + 1, LOW_CARDINALITY * 2);
            for (int i = 0; i < docCount; i++) {
                List<IndexableField> doc = new ArrayList<>(2);
                doc.add(field(i % cardinality));
                if (multiValues && i % cardinality == 0) {
                    doc.add(field((i % cardinality) + 1));
                    mvCount++;
                }
                iw.addDocument(doc);
            }
            if (missingValues) {
                iw.addDocument(List.of());
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = wrap.apply(iw.getReader())) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                innerTest(breaker, ctx, mvCount);
            }
        }
    }

    protected final TestBlock read(BlockLoader loader, BlockLoader.AllReader reader, BlockLoader.Docs docs) throws IOException {
        BlockLoader.AllReader toUse = blockAtATime
            ? reader
            : new ForceDocAtATime(() -> loader.builder(TestBlock.factory(), docs.count()), reader);

        return (TestBlock) toUse.read(TestBlock.factory(), docs, 0, false);
    }

    private static KeywordFieldMapper.KeywordField field(int codePointCount) {
        return new KeywordFieldMapper.KeywordField(
            "field",
            new BytesRef("a".repeat(codePointCount)),
            KeywordFieldMapper.Defaults.FIELD_TYPE
        );
    }
}
