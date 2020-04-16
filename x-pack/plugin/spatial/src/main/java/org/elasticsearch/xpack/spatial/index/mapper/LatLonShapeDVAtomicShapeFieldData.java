/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

final class LatLonShapeDVAtomicShapeFieldData extends AbstractAtomicGeoShapeShapeFieldData {
    private final LeafReader reader;
    private final String fieldName;

    LatLonShapeDVAtomicShapeFieldData(LeafReader reader, String fieldName) {
        super();
        this.reader = reader;
        this.fieldName = fieldName;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        // noop
    }

    @Override
    public MultiGeoShapeValues getGeoShapeValues() {
        try {
            final BinaryDocValues binaryValues = DocValues.getBinary(reader, fieldName);
            final TriangleTreeReader reader = new TriangleTreeReader(GeoShapeCoordinateEncoder.INSTANCE);
            final MultiGeoShapeValues.GeoShapeValue geoShapeValue = new MultiGeoShapeValues.GeoShapeValue(reader);
            return new MultiGeoShapeValues() {

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return binaryValues.advanceExact(doc);
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                @Override
                public ValuesSourceType valuesSourceType() {
                    return GeoShapeValuesSourceType.INSTANCE;
                }

                @Override
                public GeoShapeValue nextValue() throws IOException {
                    final BytesRef encoded = binaryValues.binaryValue();
                    reader.reset(encoded);
                    return geoShapeValue;
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}
