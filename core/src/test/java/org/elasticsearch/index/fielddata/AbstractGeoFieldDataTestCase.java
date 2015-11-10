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
import org.apache.lucene.document.GeoPointField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.GeoUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.VersionUtils;

import static org.elasticsearch.test.geo.RandomShapeGenerator.randomPoint;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public abstract class AbstractGeoFieldDataTestCase extends AbstractFieldDataImplTestCase {
    protected Version version = VersionUtils.randomVersionBetween(random(), Version.V_1_0_0, Version.CURRENT);

    @Override
    protected abstract FieldDataType getFieldDataType();

    protected Settings.Builder getFieldDataSettings() {
        return Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version);
    }

    protected Field randomGeoPointField(String fieldName, Field.Store store) {
        GeoPoint point = randomPoint(random());
        // norelease move to .before(Version.2_2_0) once GeoPointV2 is fully merged
        if (version.onOrBefore(Version.CURRENT)) {
            return new StringField(fieldName, point.lat()+","+point.lon(), store);
        }
        return new GeoPointField(fieldName, point.lon(), point.lat(), store);
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
                assertThat(values.valueAt(i).lat(), allOf(greaterThanOrEqualTo(GeoUtils.MIN_LAT_INCL), lessThanOrEqualTo(GeoUtils.MAX_LAT_INCL)));
                assertThat(values.valueAt(i).lat(), allOf(greaterThanOrEqualTo(GeoUtils.MIN_LON_INCL), lessThanOrEqualTo(GeoUtils.MAX_LON_INCL)));
            }
        }
    }
}