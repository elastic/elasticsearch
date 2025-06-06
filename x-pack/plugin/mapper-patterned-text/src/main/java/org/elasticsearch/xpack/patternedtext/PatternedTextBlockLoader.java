/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.mapper.BlockDocValuesReader;

import java.io.IOException;

public class PatternedTextBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String name;
    private final String templateFieldName;
    private final String timestampFieldName;
    private final String argsFieldName;

    PatternedTextBlockLoader(String name, String templateFieldName, String timestampFieldName, String argsFieldName) {
        this.name = name;
        this.templateFieldName = templateFieldName;
        this.timestampFieldName = timestampFieldName;
        this.argsFieldName = argsFieldName;
    }

    @Override
    public BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        SortedSetDocValues combinedDocValues = ordinals(context);
        if (combinedDocValues != null) {
            SortedDocValues singleton = DocValues.unwrapSingleton(combinedDocValues);
            if (singleton != null) {
                return new BlockDocValuesReader.SingletonOrdinals(singleton);
            }
            return new BlockDocValuesReader.Ordinals(combinedDocValues);
        }
        return new ConstantNullsReader();
    }

    @Override
    public boolean supportsOrdinals() {
        return true;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        return PatternedTextDocValues.from(context.reader(), templateFieldName, timestampFieldName, argsFieldName);
    }

    @Override
    public String toString() {
        return "BytesRefsFromOrds[" + name + "]";
    }
}
