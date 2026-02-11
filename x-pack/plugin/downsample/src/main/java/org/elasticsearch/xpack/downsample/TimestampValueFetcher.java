/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

/**
 * This class loads the timestamp values for the provided field.
 */
public class TimestampValueFetcher {
    private final IndexFieldData<?> fieldData;

    TimestampValueFetcher(DateFieldMapper.DateFieldType fieldType, SearchExecutionContext context) {
        fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
    }

    SortedNumericLongValues getLeaf(LeafReaderContext context) {
        LeafNumericFieldData numericFieldData = (LeafNumericFieldData) fieldData.load(context);
        return numericFieldData.getLongValues();
    }
}
