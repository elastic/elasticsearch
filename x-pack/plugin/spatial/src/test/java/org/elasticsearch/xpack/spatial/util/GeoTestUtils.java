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
import org.elasticsearch.lucene.spatial.BinaryShapeDocValuesField;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueFormat;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;
import java.util.List;

public class GeoTestUtils {

    public static GeometryDocValueReader geometryDocValueReader(Geometry geometry, CoordinateEncoder encoder) throws IOException {
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(geometry);
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);
        GeometryDocValueReader reader = new GeometryDocValueReader(GeometryDocValueFormat.V2);
        reader.reset(GeometryDocValueWriter.write(indexer.getIndexableFields(normalized), encoder, centroidCalculator, List.of(geometry)));
        return reader;
    }

    public static BinaryShapeDocValuesField binaryGeoShapeDocValuesField(String name, Geometry geometry) {
        return binaryGeoShapeDocValuesField(name, geometry, GeometryDocValueFormat.V2);
    }

    public static BinaryShapeDocValuesField binaryGeoShapeDocValuesField(String name, Geometry geometry, GeometryDocValueFormat format) {
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, name);
        Geometry normalized = indexer.normalize(geometry);
        BinaryShapeDocValuesField field = new BinaryShapeDocValuesField(name, CoordinateEncoder.GEO, format);
        field.add(indexer.getIndexableFields(normalized), geometry);
        return field;
    }

    // TODO: Should this be in a cartesian specific test class, and if so, which?
    public static BinaryShapeDocValuesField binaryCartesianShapeDocValuesField(String name, Geometry geometry) {
        return binaryCartesianShapeDocValuesField(name, geometry, GeometryDocValueFormat.V2);
    }

    public static BinaryShapeDocValuesField binaryCartesianShapeDocValuesField(
        String name,
        Geometry geometry,
        GeometryDocValueFormat format
    ) {
        CartesianShapeIndexer indexer = new CartesianShapeIndexer(name);
        BinaryShapeDocValuesField field = new BinaryShapeDocValuesField(name, CoordinateEncoder.CARTESIAN, format);
        field.add(indexer.indexShape(geometry), geometry);
        return field;
    }

    public static GeoShapeValues.GeoShapeValue geoShapeValue(Geometry geometry) throws IOException {
        return geoShapeValue(geometry, GeometryDocValueFormat.V2);
    }

    public static GeoShapeValues.GeoShapeValue geoShapeValue(Geometry geometry, GeometryDocValueFormat format) throws IOException {
        GeoShapeValues.GeoShapeValue value = new GeoShapeValues.GeoShapeValue(format);
        value.reset(binaryGeoShapeDocValuesField("test", geometry, format).binaryValue());
        return value;
    }

    public static CartesianShapeValues.CartesianShapeValue cartesianShapeValue(Geometry geometry) throws IOException {
        return cartesianShapeValue(geometry, GeometryDocValueFormat.V2);
    }

    public static CartesianShapeValues.CartesianShapeValue cartesianShapeValue(Geometry geometry, GeometryDocValueFormat format)
        throws IOException {
        CartesianShapeValues.CartesianShapeValue value = new CartesianShapeValues.CartesianShapeValue(format);
        value.reset(binaryCartesianShapeDocValuesField("test", geometry, format).binaryValue());
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
