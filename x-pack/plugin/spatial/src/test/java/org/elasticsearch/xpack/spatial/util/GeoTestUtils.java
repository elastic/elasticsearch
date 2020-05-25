/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.util;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.xpack.spatial.index.fielddata.CentroidCalculator;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeCoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeReader;
import org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GeoTestUtils {

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


    public static GeoBoundingBox randomBBox() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        return new GeoBoundingBox(new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon()));
    }

    public static double encodeDecodeLat(double lat) {
        return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
    }

    public static double encodeDecodeLon(double lon) {
        return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
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

    public static TriangleTreeReader triangleTreeReader(Geometry geometry, GeoShapeCoordinateEncoder encoder) throws IOException {
        ShapeField.DecodedTriangle[] triangles = toDecodedTriangles(geometry);
        TriangleTreeWriter writer = new TriangleTreeWriter(Arrays.asList(triangles), encoder, new CentroidCalculator(geometry));
        ByteBuffersDataOutput output = new ByteBuffersDataOutput();
        writer.writeTo(output);
        TriangleTreeReader reader = new TriangleTreeReader(encoder);
        reader.reset(new BytesRef(output.toArrayCopy(), 0, Math.toIntExact(output.size())));
        return reader;
    }
}
