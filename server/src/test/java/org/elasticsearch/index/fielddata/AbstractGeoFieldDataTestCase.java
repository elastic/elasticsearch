/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;

import java.io.IOException;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class AbstractGeoFieldDataTestCase extends AbstractFieldDataImplTestCase {
    @Override
    protected abstract String getFieldDataType();

    protected Field randomGeoPointField(String fieldName, Field.Store store) {
        Point point = GeometryTestUtils.randomPoint();
        return new LatLonDocValuesField(fieldName, point.getLat(), point.getLon());
    }

    @Override
    protected boolean hasDocValues() {
        return true;
    }

    @Override
    protected long minRamBytesUsed() {
        return 0;
    }

    @Override
    protected void fillAllMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    public void testSortMultiValuesFields() {
        assumeFalse("Only test on non geo_point fields", getFieldDataType().equals("geo_point"));
    }

    protected void assertValues(MultiGeoPointValues values, int docId) throws IOException {
        assertValues(values, docId, false);
    }

    protected void assertMissing(MultiGeoPointValues values, int docId) throws IOException {
        assertValues(values, docId, true);
    }

    private void assertValues(MultiGeoPointValues values, int docId, boolean missing) throws IOException {
        assertEquals(missing == false, values.advanceExact(docId));
        if (missing == false) {
            final int docCount = values.docValueCount();
            for (int i = 0; i < docCount; ++i) {
                final GeoPoint point = values.nextValue();
                assertThat(point.lat(), allOf(greaterThanOrEqualTo(GeoUtils.MIN_LAT), lessThanOrEqualTo(GeoUtils.MAX_LAT)));
                assertThat(point.lon(), allOf(greaterThanOrEqualTo(GeoUtils.MIN_LON), lessThanOrEqualTo(GeoUtils.MAX_LON)));
            }
        }
    }
}
