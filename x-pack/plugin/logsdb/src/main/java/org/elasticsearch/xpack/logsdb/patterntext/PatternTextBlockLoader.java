/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;

import java.io.IOException;

public class PatternTextBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    /**
     * Circuit breaker space reserved for each reader. Measured at 16kb in heap dumps but this is
     * an intentional overestimate.
     */
    private static final long ESTIMATED_SIZE = ByteSizeValue.ofKb(30).getBytes();

    private final PatternTextFieldMapper.DocValuesSupplier docValuesSupplier;

    PatternTextBlockLoader(PatternTextFieldMapper.DocValuesSupplier docValuesSupplier) {
        this.docValuesSupplier = docValuesSupplier;
    }

    @Override
    public BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(ESTIMATED_SIZE, "load blocks");
        var dv = docValuesSupplier.get(context.reader());
        return BytesRefsFromBinaryBlockLoader.createReader(breaker, ESTIMATED_SIZE, dv);
    }

}
