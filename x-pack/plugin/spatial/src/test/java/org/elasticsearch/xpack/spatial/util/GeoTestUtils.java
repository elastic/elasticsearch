/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.util;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.index.fielddata.CentroidCalculator;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeometryDocValueReader;
import org.elasticsearch.xpack.spatial.index.fielddata.GeometryDocValueWriter;
import org.elasticsearch.xpack.spatial.index.mapper.BinaryShapeDocValuesField;

import java.io.IOException;

public class GeoTestUtils {

    public static GeometryDocValueReader geometryDocValueReader(Geometry geometry, CoordinateEncoder encoder) throws IOException {
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(GeometryDocValueWriter.write(indexer.indexShape(geometry), encoder, centroidCalculator));
        return reader;
    }

    public static BinaryShapeDocValuesField binaryGeoShapeDocValuesField(String name, Geometry geometry) {
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, name);
        BinaryShapeDocValuesField field = new BinaryShapeDocValuesField(name, CoordinateEncoder.GEO);
        field.add(indexer.indexShape(geometry), geometry);
        return field;
    }

    public static GeoShapeValues.GeoShapeValue geoShapeValue(Geometry geometry) throws IOException {
        GeoShapeValues.GeoShapeValue value = new GeoShapeValues.GeoShapeValue();
        value.reset(binaryGeoShapeDocValuesField("test", geometry).binaryValue());
        return value;
    }

    public static GeoBoundingBox randomBBox() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        return new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
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
        XContentParser parser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(geoJson),
            XContentType.JSON
        );
        parser.nextToken();
        Geometry geometry = new GeometryParser(true, true, true).parse(parser);
        return GeometryNormalizer.apply(Orientation.CCW, geometry);
    }
}
