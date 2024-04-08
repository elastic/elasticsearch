/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.plain.LeafGeoPointFieldData;

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
        final MultiGeoPointValues values = leafData.getPointValues(); // shade
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
