/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.fielddata.plain.LeafLongFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;

/**
 * This class loads the timestamp values for the provided field.
 */
class TimestampValueFetcher {
    private final IndexFieldData<?> fieldData;

    TimestampValueFetcher(DateFieldMapper.DateFieldType fieldType, SearchExecutionContext context) {
        fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
    }

    SortedNumericLongValues getLeaf(LeafReaderContext context) {
        LeafLongFieldData numericFieldData = (LeafLongFieldData) fieldData.load(context);
        return numericFieldData.getLongValues();
    }

    static long[] fetch(SortedNumericLongValues timestampDocValues, IntArrayList docIdBuffer) throws IOException {
        long[] timestamps = new long[docIdBuffer.size()];
        for (int i = 0; i < docIdBuffer.size(); i++) {
            int docId = docIdBuffer.get(i);
            if (timestampDocValues.advanceExact(docId) == false) {
                timestamps[i] = -1;
            }
            int docValuesCount = timestampDocValues.docValueCount();
            assert docValuesCount > 0;
            timestamps[i] = timestampDocValues.nextValue();
        }
        return timestamps;
    }
}
