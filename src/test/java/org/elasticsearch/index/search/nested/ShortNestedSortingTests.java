/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.search.nested;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.ShortValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;

/**
 */
public class ShortNestedSortingTests extends AbstractNumberNestedSortingTests {

    @Override
    protected FieldDataType getFieldDataType() {
        return new FieldDataType("short");
    }

    @Override
    protected IndexFieldData.XFieldComparatorSource createInnerFieldComparator(String fieldName, SortMode sortMode, Object missingValue) {
        IndexNumericFieldData fieldData = getForField(fieldName);
        return new ShortValuesComparatorSource(fieldData, missingValue, sortMode);
    }

    @Override
    protected IndexableField createField(String name, int value, Field.Store store) {
        return new IntField(name, value, store);
    }
}
