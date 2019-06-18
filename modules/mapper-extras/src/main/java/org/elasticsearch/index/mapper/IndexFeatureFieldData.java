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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractIndexFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

public class IndexFeatureFieldData extends AbstractIndexFieldData<AtomicNumericFieldData> implements IndexNumericFieldData {

    private DoubleValuesSource valuesSource;

    public IndexFeatureFieldData(IndexSettings indexSettings, String featureName, IndexFieldDataCache cache) {
        super(indexSettings, RankFeatureMetaFieldMapper.NAME, cache);
        this.valuesSource = FeatureField.newDoubleValues(RankFeatureMetaFieldMapper.NAME, featureName);
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.DOUBLE;
    }

    @Override
    public AtomicFeatureFieldData loadDirect(LeafReaderContext context) throws Exception {
        DoubleValues values = valuesSource.getValues(context, null);
        return new AtomicFeatureFieldData(values);
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        return FeatureField.newFeatureSort(RankFeatureMetaFieldMapper.NAME, getFieldName());
    }

    @Override
    protected AtomicFeatureFieldData empty(int maxDoc) {
        return AtomicFeatureFieldData.empty(maxDoc);
    }

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                CircuitBreakerService breakerService, MapperService mapperService) {
            return new IndexFeatureFieldData(indexSettings, fieldType.name(), cache);
        }

    }
}
