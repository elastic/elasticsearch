/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.search.nested;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;

public class LongNestedSortingTests extends AbstractNumberNestedSortingTestCase {

    @Override
    protected String getFieldDataType() {
        return "long";
    }

    @Override
    protected IndexFieldData.XFieldComparatorSource createFieldComparator(
        String fieldName,
        MultiValueMode sortMode,
        Object missingValue,
        Nested nested
    ) {
        IndexNumericFieldData fieldData = getForField(fieldName);
        return new LongValuesComparatorSource(fieldData, missingValue, sortMode, nested, null);
    }

    @Override
    protected IndexableField createField(String name, int value) {
        return new SortedNumericDocValuesField(name, value);
    }

}
