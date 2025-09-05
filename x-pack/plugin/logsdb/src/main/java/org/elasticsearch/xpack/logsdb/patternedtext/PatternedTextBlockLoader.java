/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.BlockDocValuesReader;

import java.io.IOException;

public class PatternedTextBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String templateFieldName;
    private final String argsFieldName;
    private final String argsInfoFieldName;

    PatternedTextBlockLoader(String templateFieldName, String argsFieldName, String argsInfoFieldName) {
        this.templateFieldName = templateFieldName;
        this.argsFieldName = argsFieldName;
        this.argsInfoFieldName = argsInfoFieldName;
    }

    @Override
    public BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        var docValues = PatternedTextDocValues.from(context.reader(), templateFieldName, argsFieldName, argsInfoFieldName);
        if (docValues == null) {
            return new ConstantNullsReader();
        }
        return new BlockDocValuesReader.BytesRefsFromBinary(docValues);
    }
}
