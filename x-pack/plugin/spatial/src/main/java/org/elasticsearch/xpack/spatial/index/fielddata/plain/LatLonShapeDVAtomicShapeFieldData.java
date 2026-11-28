/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueFormat;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;

final class LatLonShapeDVAtomicShapeFieldData extends LeafShapeFieldData<GeoShapeValues> {
    private final LeafReader reader;
    private final String fieldName;
    private final GeometryDocValueFormat docValueFormat;

    LatLonShapeDVAtomicShapeFieldData(
        LeafReader reader,
        String fieldName,
        GeometryDocValueFormat docValueFormat,
        ToScriptFieldFactory<GeoShapeValues> toScriptFieldFactory
    ) {
        super(toScriptFieldFactory);
        this.reader = reader;
        this.fieldName = fieldName;
        this.docValueFormat = docValueFormat;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public GeoShapeValues getShapeValues() {
        try {
            final BinaryDocValues binaryValues = DocValues.getBinary(reader, fieldName);
            final GeoShapeValues.GeoShapeValue geoShapeValue = new GeoShapeValues.GeoShapeValue(docValueFormat);
            return new GeoShapeValues() {

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return binaryValues.advanceExact(doc);
                }

                @Override
                public ValuesSourceType valuesSourceType() {
                    return GeoShapeValuesSourceType.instance();
                }

                @Override
                public GeoShapeValue value() throws IOException {
                    geoShapeValue.reset(binaryValues.binaryValue());
                    return geoShapeValue;
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}
