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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;

import java.io.IOException;

/**
 */
public class ShortValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData<?> indexFieldData;
    private final Object missingValue;
    private final SortMode sortMode;

    public ShortValuesComparatorSource(IndexNumericFieldData<?> indexFieldData, @Nullable Object missingValue, SortMode sortMode) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.sortMode = sortMode;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.INT;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        assert fieldname.equals(indexFieldData.getFieldNames().indexName());

        short dMissingValue;
        if (missingValue == null || "_last".equals(missingValue)) {
            dMissingValue = reversed ? Short.MIN_VALUE : Short.MAX_VALUE;
        } else if ("_first".equals(missingValue)) {
            dMissingValue = reversed ? Short.MAX_VALUE : Short.MIN_VALUE;
        } else {
            dMissingValue = missingValue instanceof Number ? ((Number) missingValue).shortValue() : Short.parseShort(missingValue.toString());
        }

        return new ShortValuesComparator(indexFieldData, dMissingValue, numHits, sortMode);
    }
}
