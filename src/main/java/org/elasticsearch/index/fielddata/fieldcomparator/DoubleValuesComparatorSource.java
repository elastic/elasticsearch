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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

/**
 */
public class DoubleValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData<?> indexFieldData;
    private final Object missingValue;
    private final MultiValueMode sortMode;

    public DoubleValuesComparatorSource(IndexNumericFieldData<?> indexFieldData, @Nullable Object missingValue, MultiValueMode sortMode) {
        this.indexFieldData = indexFieldData;
        this.missingValue = missingValue;
        this.sortMode = sortMode;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.DOUBLE;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        assert fieldname.equals(indexFieldData.getFieldNames().indexName());

        final double dMissingValue = (Double) missingObject(missingValue, reversed);
        return new DoubleValuesComparator(indexFieldData, dMissingValue, numHits, sortMode);
    }
}
