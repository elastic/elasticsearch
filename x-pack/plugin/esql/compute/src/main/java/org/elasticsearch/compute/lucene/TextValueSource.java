/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.BaseKeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

public class TextValueSource extends ValuesSource.Bytes {

    private final IndexFieldData<?> indexFieldData;

    public TextValueSource(IndexFieldData<?> indexFieldData) {
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext leafReaderContext) throws IOException {
        String fieldName = indexFieldData.getFieldName();
        LeafFieldData fieldData = indexFieldData.load(leafReaderContext);
        BaseKeywordDocValuesField keywordDocValuesField = (BaseKeywordDocValuesField) fieldData.getScriptFieldFactory(fieldName);
        return keywordDocValuesField.bytesValues();
    }
}
