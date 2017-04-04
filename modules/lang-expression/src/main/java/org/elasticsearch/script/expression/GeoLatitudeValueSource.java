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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.elasticsearch.index.fielddata.AtomicGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

/**
 * ValueSource to return latitudes as a double "stream" for geopoint fields
 */
final class GeoLatitudeValueSource extends ValueSource {
    final IndexFieldData<?> fieldData;

    GeoLatitudeValueSource(IndexFieldData<?> fieldData) {
        this.fieldData = Objects.requireNonNull(fieldData);
    }

    @Override
    @SuppressWarnings("rawtypes") // ValueSource uses a rawtype
    public FunctionValues getValues(Map context, LeafReaderContext leaf) throws IOException {
        AtomicGeoPointFieldData leafData = (AtomicGeoPointFieldData) fieldData.load(leaf);
        final MultiGeoPointValues values = leafData.getGeoPointValues();
        return new DoubleDocValues(this) {
            @Override
            public double doubleVal(int doc) {
                values.setDocument(doc);
                if (values.count() == 0) {
                    return 0.0;
                } else {
                    return values.valueAt(0).getLat();
                }
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
        GeoLatitudeValueSource other = (GeoLatitudeValueSource) obj;
        if (!fieldData.equals(other.fieldData)) return false;
        return true;
    }

    @Override
    public String description() {
        return "lat: field(" + fieldData.getFieldName() + ")";
    }
}
