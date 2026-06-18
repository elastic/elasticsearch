/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

/**
 * Field data for integral types backed by single-valued ({@code doc_values.multi_value: false})
 * NUMERIC doc values. Uses a plain {@link SortField} for index sorting rather than
 * {@link org.apache.lucene.search.SortedNumericSortField}, because Lucene validates that the
 * doc-values type matches the sort-field type at merge time.
 */
final class SingleValuedNumericIndexFieldData extends SortedNumericIndexFieldData {

    SingleValuedNumericIndexFieldData(
        String fieldName,
        NumericType numericType,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<SortedNumericLongValues> toScriptFieldFactory,
        IndexType indexType
    ) {
        super(fieldName, numericType, valuesSourceType, toScriptFieldFactory, indexType);
    }

    @Override
    protected SortField buildIndexSortField(
        IndexNumericFieldData.NumericType targetNumericType,
        MultiValueMode sortMode,
        boolean reverse,
        XFieldComparatorSource source,
        Object missingValue,
        boolean canOptimize
    ) {
        SortField sortField = new SortField(getFieldName(), targetNumericType.getSortFieldType(), reverse);
        sortField.setMissingValue(source.missingObject(missingValue, reverse));
        sortField.setOptimizeSortWithPoints(canOptimize);
        return sortField;
    }
}
