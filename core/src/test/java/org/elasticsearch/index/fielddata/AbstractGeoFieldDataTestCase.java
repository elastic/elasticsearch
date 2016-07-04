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
package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;

import static org.elasticsearch.test.geo.RandomShapeGenerator.randomPoint;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 *
 */
public abstract class AbstractGeoFieldDataTestCase extends AbstractFieldDataImplTestCase {
    @Override
    protected abstract String getFieldDataType();

    protected Field randomGeoPointField(String fieldName, Field.Store store) {
        GeoPoint point = randomPoint(random());
        if (indexService.getIndexSettings().getIndexVersionCreated().before(Version.V_2_2_0)) {
            return new StringField(fieldName, point.lat()+","+point.lon(), store);
        }
        final GeoPointField.TermEncoding termEncoding;
        termEncoding = indexService.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_2_3_0) ?
            GeoPointField.TermEncoding.PREFIX : GeoPointField.TermEncoding.NUMERIC;
        return new GeoPointField(fieldName, point.lat(), point.lon(), termEncoding, store);
    }

    @Override
    protected boolean hasDocValues() {
        // prior to 22 docValues were not required
        if (indexService.getIndexSettings().getIndexVersionCreated().before(Version.V_2_2_0)) {
            return false;
        }
        return true;
    }

    @Override
    protected long minRamBytesUsed() {
        if (indexService.getIndexSettings().getIndexVersionCreated().before(Version.V_2_2_0)) {
            return super.minRamBytesUsed();
        }
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

    protected void assertValues(MultiGeoPointValues values, int docId) {
        assertValues(values, docId, false);
    }

    protected void assertMissing(MultiGeoPointValues values, int docId) {
        assertValues(values, docId, true);
    }

    private void assertValues(MultiGeoPointValues values, int docId, boolean missing) {
        values.setDocument(docId);
        int docCount = values.count();
        if (missing) {
            assertThat(docCount, equalTo(0));
        } else {
            assertThat(docCount, greaterThan(0));
            for (int i = 0; i < docCount; ++i) {
                final GeoPoint point = values.valueAt(i);
                assertThat(point.lat(), allOf(greaterThanOrEqualTo(GeoUtils.MIN_LAT), lessThanOrEqualTo(GeoUtils.MAX_LAT)));
                assertThat(point.lon(), allOf(greaterThanOrEqualTo(GeoUtils.MIN_LON), lessThanOrEqualTo(GeoUtils.MAX_LON)));
            }
        }
    }
}
