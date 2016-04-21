/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.search.nested;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;

/**
 */
public class LongNestedSortingTests extends AbstractNumberNestedSortingTestCase {

    @Override
    protected String getFieldDataType() {
        return "long";
    }

    @Override
    protected IndexFieldData.XFieldComparatorSource createFieldComparator(String fieldName, MultiValueMode sortMode, Object missingValue, Nested nested) {
        IndexNumericFieldData fieldData = getForField(fieldName);
        return new LongValuesComparatorSource(fieldData, missingValue, sortMode, nested);
    }

    @Override
    protected IndexableField createField(String name, int value) {
        return new SortedNumericDocValuesField(name, value);
    }

}
