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
package org.elasticsearch.common.geo;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class GeoTestUtils {

    public static void assertRelation(GeoRelation expectedRelation, TriangleTreeReader reader, Extent extent) throws IOException {
        GeoRelation actualRelation = reader.relateTile(extent.minX(), extent.minY(), extent.maxX(), extent.maxY());
        assertThat(actualRelation, equalTo(expectedRelation));
    }

    public static ShapeField.DecodedTriangle[] toDecodedTriangles(Geometry geometry) throws IOException {
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        geometry = indexer.prepareForIndexing(geometry);
        List<IndexableField> fields = indexer.indexShape(null, geometry);
        ShapeField.DecodedTriangle[] triangles = new ShapeField.DecodedTriangle[fields.size()];
        final byte[] scratch = new byte[7 * Integer.BYTES];
        for (int i = 0; i < fields.size(); i++) {
            BytesRef bytesRef = fields.get(i).binaryValue();
            assert bytesRef.length == 7 * Integer.BYTES;
            System.arraycopy(bytesRef.bytes, bytesRef.offset, scratch, 0, 7 * Integer.BYTES);
            ShapeField.decodeTriangle(scratch, triangles[i] = new ShapeField.DecodedTriangle());
        }
        return triangles;
    }

    public static TriangleTreeReader triangleTreeReader(Geometry geometry, CoordinateEncoder encoder) throws IOException {
        ShapeField.DecodedTriangle[] triangles = toDecodedTriangles(geometry);
        TriangleTreeWriter writer = new TriangleTreeWriter(Arrays.asList(triangles), encoder, new CentroidCalculator(geometry));
        ByteBuffersDataOutput output = new ByteBuffersDataOutput();
        writer.writeTo(output);
        TriangleTreeReader reader = new TriangleTreeReader(encoder);
        reader.reset(new BytesRef(output.toArrayCopy(), 0, Math.toIntExact(output.size())));
        return reader;
    }

    public static String toGeoJsonString(Geometry geometry) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        GeoJson.toXContent(geometry, builder, ToXContent.EMPTY_PARAMS);
        return XContentHelper.convertToJson(BytesReference.bytes(builder), true, false, XContentType.JSON);
    }

    public static Geometry fromGeoJsonString(String geoJson) throws Exception {
        XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(geoJson), XContentType.JSON);
        parser.nextToken();
        Geometry geometry = new GeometryParser(true, true, true).parse(parser);
        return new GeoShapeIndexer(true, "indexer").prepareForIndexing(geometry);
    }

    public static Polygon polyFrom(Rectangle rectangle) {
        return new Polygon(new LinearRing(
            new double[] { rectangle.getMinX(), rectangle.getMaxX(), rectangle.getMaxX(), rectangle.getMinX(), rectangle.getMinX()},
            new double[] { rectangle.getMinY(), rectangle.getMinY(), rectangle.getMaxY(), rectangle.getMaxY(), rectangle.getMinY()}));
    }
}
