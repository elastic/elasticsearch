/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.search.nested;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.HalfFloatValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;

public class HalfFloatNestedSortingTests extends DoubleNestedSortingTests {

    @Override
    protected String getFieldDataType() {
        return "half_float";
    }

    @Override
    protected XFieldComparatorSource createFieldComparator(String fieldName, MultiValueMode sortMode, Object missingValue, Nested nested) {
        IndexNumericFieldData fieldData = getForField(fieldName);
        return new HalfFloatValuesComparatorSource(fieldData, missingValue, sortMode, nested);
    }

    @Override
    protected IndexableField createField(String name, int value) {
        return new SortedNumericDocValuesField(name, HalfFloatPoint.halfFloatToSortableShort(value));
    }
}
