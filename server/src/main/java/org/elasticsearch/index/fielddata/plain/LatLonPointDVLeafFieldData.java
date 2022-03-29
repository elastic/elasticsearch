/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.script.field.ToScriptFieldFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

final class LatLonPointDVLeafFieldData extends AbstractLeafGeoPointFieldData {
    private final LeafReader reader;
    private final String fieldName;

    LatLonPointDVLeafFieldData(LeafReader reader, String fieldName, ToScriptFieldFactory<MultiGeoPointValues> toScriptFieldFactory) {
        super(toScriptFieldFactory);
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
    public MultiGeoPointValues getGeoPointValues() {
        try {
            final SortedNumericDocValues numericValues = DocValues.getSortedNumeric(reader, fieldName);
            return new MultiGeoPointValues() {

                final GeoPoint point = new GeoPoint();

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return numericValues.advanceExact(doc);
                }

                @Override
                public int docValueCount() {
                    return numericValues.docValueCount();
                }

                @Override
                public GeoPoint nextValue() throws IOException {
                    final long encoded = numericValues.nextValue();
                    point.reset(GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32)), GeoEncodingUtils.decodeLongitude((int) encoded));
                    return point;
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}
