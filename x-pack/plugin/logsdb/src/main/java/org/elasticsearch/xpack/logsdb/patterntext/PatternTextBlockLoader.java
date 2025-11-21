/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;

import java.io.IOException;

public class PatternTextBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final PatternTextFieldMapper.DocValuesSupplier docValuesSupplier;

    PatternTextBlockLoader(PatternTextFieldMapper.DocValuesSupplier docValuesSupplier) {
        this.docValuesSupplier = docValuesSupplier;
    }

    @Override
    public BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        var docValues = docValuesSupplier.get(context.reader());
        return BytesRefsFromBinaryBlockLoader.createReader(docValues);
    }

}
