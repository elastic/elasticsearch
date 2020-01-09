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

package org.elasticsearch.search.geo;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class GeoPointShapeQueryTests extends GeoQueryTests {

    @Override
    protected XContentBuilder createMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("location")
            .field("type", "geo_point")
            .endObject().endObject().endObject();

        return xcb;
    }

    public void testNullShape() throws Exception {
        super.testNullShape();
    }

    public void testIndexPointsFilterRectangle() throws Exception {
        //super.testIndexPointsFilterRectangle(Strings.toString(createMapping()));
    }
    public void testIndexedShapeReference() throws Exception {
        //super.testIndexedShapeReference(Strings.toString(createMapping()));
    }

    public void testShapeFetchingPath() throws Exception {
        //super.testShapeFetchingPath();
    }

    public void testQueryRandomGeoCollection() throws Exception {
        //super.testQueryRandomGeoCollection(Strings.toString(createMapping()));
    }

    public void testRandomGeoCollectionQuery() throws Exception {
        //super.testRandomGeoCollectionQuery();
    }

    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        //super.testShapeFilterWithDefinedGeoCollection(Strings.toString(createMapping()));
    }

    public void testFieldAlias() throws IOException {
        /* super.testFieldAlias(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_point")
            .endObject()
            .startObject("alias")
            .field("type", "alias")
            .field("path", "location")
            .endObject()
            .endObject()
            .endObject()
            .endObject()); */
    }

    // Test for issue #34418
    public void testEnvelopeSpanningDateline() throws Exception {
        //super.testEnvelopeSpanningDateline();
    }

    public void testGeometryCollectionRelations() throws Exception {
        //super.testGeometryCollectionRelations();
    }

}
