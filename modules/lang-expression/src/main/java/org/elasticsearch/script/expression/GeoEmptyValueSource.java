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

package org.elasticsearch.script.expression;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

import java.io.IOException;

/**
 * ValueSource to return non-zero if a field is missing.
 */
final class GeoEmptyValueSource extends FieldDataBasedDoubleValuesSource {

    GeoEmptyValueSource(IndexFieldData<?> fieldData) {
        super(fieldData);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext leaf, DoubleValues scores) {
        LeafGeoPointFieldData leafData = (LeafGeoPointFieldData) fieldData.load(leaf);
        final MultiGeoPointValues values = leafData.getGeoPointValues();
        return new DoubleValues() {
            @Override
            public double doubleValue() {
                return 1;
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }
        };
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode() + fieldData.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        GeoEmptyValueSource other = (GeoEmptyValueSource) obj;
        return fieldData.equals(other.fieldData);
    }

    @Override
    public String toString() {
        return "empty: field(" + fieldData.getFieldName() + ")";
    }

}
