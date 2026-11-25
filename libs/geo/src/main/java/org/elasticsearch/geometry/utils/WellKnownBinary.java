/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry.utils;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Utility class for converting {@link Geometry} to and from WKB
 */
public class WellKnownBinary {

    private WellKnownBinary() {}

    /**
     * Converts the given {@link Geometry} to WKB with the provided {@link ByteOrder}
     */
    public static byte[] toWKB(Geometry geometry, ByteOrder byteOrder) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            toWKB(geometry, outputStream, ByteBuffer.allocate(8).order(byteOrder));
            return outputStream.toByteArray();
        } catch (IOException ioe) {
            // Should never happen as the only method throwing IOException is ByteArrayOutputStream#close and it is a NOOP
            throw new UncheckedIOException(ioe);
        }
    }

    private static void toWKB(Geometry geometry, ByteArrayOutputStream out, ByteBuffer scratch) {
        out.write(scratch.order() == ByteOrder.BIG_ENDIAN ? 0 : 1);
        geometry.visit(new GeometryVisitor<Void, RuntimeException>() {
            @Override
            public Void visit(Point point) {
                if (point.isEmpty()) {
                    throw new IllegalArgumentException("Empty " + point.type() + " cannot be represented in WKB");
                }
                writeInt(out, scratch, point.hasZ() ? 1001 : 1);
                writeDouble(out, scratch, point.getX());
                writeDouble(out, scratch, point.getY());
                if (point.hasZ()) {
                    writeDouble(out, scratch, point.getZ());
                }
                return null;
            }

            @Override
            public Void visit(Line line) {
                writeInt(out, scratch, line.hasZ() ? 1002 : 2);
                writeInt(out, scratch, line.length());
                for (int i = 0; i < line.length(); ++i) {
                    writeDouble(out, scratch, line.getX(i));
                    writeDouble(out, scratch, line.getY(i));
                    if (line.hasZ()) {
                        writeDouble(out, scratch, line.getZ(i));
                    }
                }
                return null;
            }

            @Override
            public Void visit(Polygon polygon) {
                writeInt(out, scratch, polygon.hasZ() ? 1003 : 3);
                if (polygon.isEmpty()) {
                    writeInt(out, scratch, 0);
                    return null;
                }
                writeInt(out, scratch, polygon.getNumberOfHoles() + 1);
                visitLinearRing(polygon.getPolygon());
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    visitLinearRing(polygon.getHole(i));
                }
                return null;
            }

            @Override
            public Void visit(MultiPoint multiPoint) {
                writeInt(out, scratch, multiPoint.hasZ() ? 1004 : 4);
                writeInt(out, scratch, multiPoint.size());
                for (Point point : multiPoint) {
                    toWKB(point, out, scratch);
                }
                return null;
            }

            @Override
            public Void visit(MultiLine multiLine) {
                writeInt(out, scratch, multiLine.hasZ() ? 1005 : 5);
                writeInt(out, scratch, multiLine.size());
                for (Line line : multiLine) {
                    toWKB(line, out, scratch);
                }
                return null;
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) {
                writeInt(out, scratch, multiPolygon.hasZ() ? 1006 : 6);
                writeInt(out, scratch, multiPolygon.size());
                for (Polygon polygon : multiPolygon) {
                    toWKB(polygon, out, scratch);
                }
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) {
                writeInt(out, scratch, collection.hasZ() ? 1007 : 7);
                writeInt(out, scratch, collection.size());
                for (Geometry geometry : collection) {
                    toWKB(geometry, out, scratch);
                }
                return null;
            }

            @Override
            public Void visit(Circle circle) {
                if (circle.isEmpty()) {
                    throw new IllegalArgumentException("Empty " + circle.type() + " cannot be represented in WKB");
                }
                writeInt(out, scratch, circle.hasZ() ? 1017 : 17);
                writeDouble(out, scratch, circle.getX());
                writeDouble(out, scratch, circle.getY());
                if (circle.hasZ()) {
                    writeDouble(out, scratch, circle.getZ());
                }
                writeDouble(out, scratch, circle.getRadiusMeters());
                return null;
            }

            @Override
            public Void visit(Rectangle rectangle) {
                if (rectangle.isEmpty()) {
                    throw new IllegalArgumentException("Empty " + rectangle.type() + " cannot be represented in WKB");
                }
                writeInt(out, scratch, rectangle.hasZ() ? 1018 : 18);
                // minX, maxX, maxY, minY
                writeDouble(out, scratch, rectangle.getMinX());
                writeDouble(out, scratch, rectangle.getMaxX());
                writeDouble(out, scratch, rectangle.getMaxY());
                writeDouble(out, scratch, rectangle.getMinY());
                if (rectangle.hasZ()) {
                    writeDouble(out, scratch, rectangle.getMinZ());
                    writeDouble(out, scratch, rectangle.getMaxZ());
                }
                return null;
            }

            @Override
            public Void visit(LinearRing ring) {
                throw new IllegalArgumentException("Linear ring is not supported by WKB");
            }

            private void visitLinearRing(LinearRing ring) {
                writeInt(out, scratch, ring.length());
                for (int i = 0; i < ring.length(); i++) {
                    writeDouble(out, scratch, ring.getX(i));
                    writeDouble(out, scratch, ring.getY(i));
                    if (ring.hasZ()) {
                        writeDouble(out, scratch, ring.getZ(i));
                    }
                }
            }
        });
    }

    private static void writeInt(ByteArrayOutputStream out, ByteBuffer scratch, int i) {
        scratch.clear();
        scratch.putInt(i);
        out.write(scratch.array(), 0, 4);
    }

    private static void writeDouble(ByteArrayOutputStream out, ByteBuffer scratch, double d) {
        scratch.clear();
        scratch.putDouble(d);
        out.write(scratch.array(), 0, 8);
    }

    /**
     * Converts a WKT string directly to WKB with the provided {@link ByteOrder}, without building
     * intermediate {@link Geometry} objects. Each parsed coordinate is validated inline via
     * {@link GeometryValidator#validateCoordinate}; pass {@link GeometryValidator#NOOP} to skip validation.
     *
     * @param wkt the WKT string to parse
     * @param byteOrder the byte order for the WKB output
     * @param coerce if true, unclosed polygon rings are automatically closed
     * @param validator called for each coordinate as it is parsed
     * @throws IOException if an I/O error occurs while reading the WKT string
     * @throws ParseException if the WKT string is malformed
     */
    public static byte[] fromWKT(String wkt, ByteOrder byteOrder, boolean coerce, GeometryValidator validator) throws IOException,
        ParseException {
        StringReader reader = new StringReader(wkt);
        try {
            StreamTokenizer stream = WellKnownText.newTokenizer(reader);
            ByteBuffer scratch = ByteBuffer.allocate(8).order(byteOrder);
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                writeWKBGeometry(stream, out, scratch, coerce, 0, validator);
                return out.toByteArray();
            }
        } finally {
            reader.close();
        }
    }

    /**
     * Converts a WKT string directly to WKB with the provided {@link ByteOrder}, without building
     * intermediate {@link Geometry} objects. No coordinate validation is performed.
     *
     * @param wkt the WKT string to parse
     * @param byteOrder the byte order for the WKB output
     * @param coerce if true, unclosed polygon rings are automatically closed
     * @throws IOException if an I/O error occurs while reading the WKT string
     * @throws ParseException if the WKT string is malformed
     */
    public static byte[] fromWKT(String wkt, ByteOrder byteOrder, boolean coerce) throws IOException, ParseException {
        return fromWKT(wkt, byteOrder, coerce, GeometryValidator.NOOP);
    }

    private static void writeWKBGeometry(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean coerce,
        int depth,
        GeometryValidator validator
    ) throws IOException, ParseException {
        final String type = WellKnownText.nextWord(stream).toLowerCase(Locale.ROOT);
        final boolean explicitZ = WellKnownText.isZOrMNext(stream);
        out.write(scratch.order() == ByteOrder.BIG_ENDIAN ? 0 : 1);
        switch (type) {
            case "point" -> writeWKBPoint(stream, out, scratch, explicitZ, validator);
            case "multipoint" -> writeWKBMultiPoint(stream, out, scratch, explicitZ, validator);
            case "linestring" -> writeWKBLineString(stream, out, scratch, explicitZ, validator);
            case "multilinestring" -> writeWKBMultiLineString(stream, out, scratch, explicitZ, validator);
            case "polygon" -> writeWKBPolygon(stream, out, scratch, coerce, explicitZ, validator);
            case "multipolygon" -> writeWKBMultiPolygon(stream, out, scratch, coerce, explicitZ, validator);
            case "geometrycollection" -> writeWKBGeometryCollection(stream, out, scratch, coerce, depth, explicitZ, validator);
            case "circle" -> writeWKBCircle(stream, out, scratch, explicitZ, validator);
            case "bbox" -> writeWKBBBox(stream, out, scratch, explicitZ, validator);
            default -> throw new ParseException("Unknown geometry type: " + type, stream.lineno());
        }
    }

    private static void writeWKBPoint(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean explicitZ,
        GeometryValidator validator
    ) throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            throw new IllegalArgumentException("Empty POINT cannot be represented in WKB");
        }
        double x = WellKnownText.nextNumber(stream);
        double y = WellKnownText.nextNumber(stream);
        double z = Double.NaN;
        if (WellKnownText.isNumberNext(stream)) {
            z = WellKnownText.nextNumber(stream);
        }
        WellKnownText.nextCloser(stream);
        WellKnownText.checkZorMAttribute(explicitZ, Double.isNaN(z) == false);
        validator.validateCoordinate(x, y, z);
        boolean hasZ = Double.isNaN(z) == false;
        writeInt(out, scratch, hasZ ? 1001 : 1);
        writeDouble(out, scratch, x);
        writeDouble(out, scratch, y);
        if (hasZ) {
            writeDouble(out, scratch, z);
        }
    }

    private static void writeWKBLineString(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean explicitZ,
        GeometryValidator validator
    ) throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            writeInt(out, scratch, 2);
            writeInt(out, scratch, 0);
            return;
        }
        CoordsList coords = wktReadCoordinates(stream, validator);
        WellKnownText.checkZorMAttribute(explicitZ, coords.hasZ());
        writeInt(out, scratch, coords.hasZ() ? 1002 : 2);
        writeInt(out, scratch, coords.size());
        writeCoordinateList(out, scratch, coords);
    }

    private static void writeWKBPolygon(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean coerce,
        boolean explicitZ,
        GeometryValidator validator
    ) throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            writeInt(out, scratch, 3);
            writeInt(out, scratch, 0);
            return;
        }
        List<CoordsList> rings = new ArrayList<>();
        WellKnownText.nextOpener(stream);
        rings.add(wktReadRing(stream, coerce, validator));
        while (WellKnownText.nextCloserOrComma(stream).equals(WellKnownText.COMMA)) {
            WellKnownText.nextOpener(stream);
            rings.add(wktReadRing(stream, coerce, validator));
        }
        boolean hasZ = rings.isEmpty() == false && rings.get(0).hasZ();
        for (CoordsList ring : rings) {
            if (ring.hasZ() != hasZ) {
                throw new IllegalArgumentException("holes must have the same number of dimensions as the polygon");
            }
        }
        WellKnownText.checkZorMAttribute(explicitZ, hasZ);
        writeInt(out, scratch, hasZ ? 1003 : 3);
        writeInt(out, scratch, rings.size());
        for (CoordsList ring : rings) {
            writeInt(out, scratch, ring.size());
            writeCoordinateList(out, scratch, ring);
        }
    }

    private static void writeWKBMultiPoint(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean explicitZ,
        GeometryValidator validator
    ) throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            writeInt(out, scratch, 4);
            writeInt(out, scratch, 0);
            return;
        }
        CoordsList coords = wktReadCoordinates(stream, validator);
        boolean hasZ = coords.hasZ();
        WellKnownText.checkZorMAttribute(explicitZ, hasZ);
        writeInt(out, scratch, hasZ ? 1004 : 4);
        writeInt(out, scratch, coords.size());
        byte byteOrderByte = (byte) (scratch.order() == ByteOrder.BIG_ENDIAN ? 0 : 1);
        for (int i = 0; i < coords.size(); i++) {
            out.write(byteOrderByte);
            writeInt(out, scratch, hasZ ? 1001 : 1);
            writeDouble(out, scratch, coords.lons.get(i));
            writeDouble(out, scratch, coords.lats.get(i));
            if (hasZ) {
                writeDouble(out, scratch, coords.alts.get(i));
            }
        }
    }

    private static void writeWKBMultiLineString(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean explicitZ,
        GeometryValidator validator
    ) throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            writeInt(out, scratch, 5);
            writeInt(out, scratch, 0);
            return;
        }
        List<CoordsList> lines = new ArrayList<>();
        CoordsList firstLine = wktReadLineStringCoords(stream, validator);
        lines.add(firstLine);
        boolean hasZ = firstLine.hasZ();
        while (WellKnownText.nextCloserOrComma(stream).equals(WellKnownText.COMMA)) {
            CoordsList line = wktReadLineStringCoords(stream, validator);
            lines.add(line);
            if (line.hasZ() != hasZ) {
                throw new IllegalArgumentException("all elements of the collection should have the same number of dimension");
            }
        }
        WellKnownText.checkZorMAttribute(explicitZ, hasZ);
        writeInt(out, scratch, hasZ ? 1005 : 5);
        writeInt(out, scratch, lines.size());
        byte byteOrderByte = (byte) (scratch.order() == ByteOrder.BIG_ENDIAN ? 0 : 1);
        for (CoordsList line : lines) {
            out.write(byteOrderByte);
            writeInt(out, scratch, hasZ ? 1002 : 2);
            writeInt(out, scratch, line.size());
            writeCoordinateList(out, scratch, line);
        }
    }

    private static void writeWKBMultiPolygon(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean coerce,
        boolean explicitZ,
        GeometryValidator validator
    ) throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            writeInt(out, scratch, 6);
            writeInt(out, scratch, 0);
            return;
        }
        List<List<CoordsList>> polygons = new ArrayList<>();
        List<CoordsList> firstPolygon = wktReadPolygonRings(stream, coerce, validator);
        polygons.add(firstPolygon);
        boolean hasZ = firstPolygon.isEmpty() == false && firstPolygon.get(0).hasZ();
        while (WellKnownText.nextCloserOrComma(stream).equals(WellKnownText.COMMA)) {
            List<CoordsList> polygon = wktReadPolygonRings(stream, coerce, validator);
            polygons.add(polygon);
            boolean polygonHasZ = polygon.isEmpty() == false && polygon.get(0).hasZ();
            if (polygonHasZ != hasZ) {
                throw new IllegalArgumentException("all elements of the collection should have the same number of dimension");
            }
        }
        WellKnownText.checkZorMAttribute(explicitZ, hasZ);
        writeInt(out, scratch, hasZ ? 1006 : 6);
        writeInt(out, scratch, polygons.size());
        byte byteOrderByte = (byte) (scratch.order() == ByteOrder.BIG_ENDIAN ? 0 : 1);
        for (List<CoordsList> polygon : polygons) {
            out.write(byteOrderByte);
            writeInt(out, scratch, hasZ ? 1003 : 3);
            writeInt(out, scratch, polygon.size());
            for (CoordsList ring : polygon) {
                writeInt(out, scratch, ring.size());
                writeCoordinateList(out, scratch, ring);
            }
        }
    }

    private static void writeWKBGeometryCollection(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean coerce,
        int depth,
        boolean explicitZ,
        GeometryValidator validator
    ) throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            writeInt(out, scratch, 7);
            writeInt(out, scratch, 0);
            return;
        }
        if (depth >= WellKnownText.MAX_NESTED_DEPTH) {
            throw new ParseException("maximum nested depth of " + WellKnownText.MAX_NESTED_DEPTH + " exceeded", stream.lineno());
        }
        List<byte[]> subGeometries = new ArrayList<>();
        ByteArrayOutputStream subOut = new ByteArrayOutputStream();
        writeWKBGeometry(stream, subOut, scratch, coerce, depth + 1, validator);
        byte[] subBytes = subOut.toByteArray();
        subGeometries.add(subBytes);
        boolean hasZ = wkbTypeHasZ(subBytes);
        while (WellKnownText.nextCloserOrComma(stream).equals(WellKnownText.COMMA)) {
            subOut = new ByteArrayOutputStream();
            writeWKBGeometry(stream, subOut, scratch, coerce, depth + 1, validator);
            subBytes = subOut.toByteArray();
            subGeometries.add(subBytes);
            if (wkbTypeHasZ(subBytes) != hasZ) {
                throw new IllegalArgumentException("all elements of the collection should have the same number of dimension");
            }
        }
        WellKnownText.checkZorMAttribute(explicitZ, hasZ);
        writeInt(out, scratch, hasZ ? 1007 : 7);
        writeInt(out, scratch, subGeometries.size());
        for (byte[] subGeom : subGeometries) {
            out.write(subGeom, 0, subGeom.length);
        }
    }

    private static void writeWKBCircle(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean explicitZ,
        GeometryValidator validator
    ) throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            throw new IllegalArgumentException("Empty CIRCLE cannot be represented in WKB");
        }
        double x = WellKnownText.nextNumber(stream);
        double y = WellKnownText.nextNumber(stream);
        // WKT circle format: (x y r [z]) — radius comes before optional z
        double r = WellKnownText.nextNumber(stream);
        double z = Double.NaN;
        if (WellKnownText.isNumberNext(stream)) {
            z = WellKnownText.nextNumber(stream);
        }
        WellKnownText.nextCloser(stream);
        validator.validateCoordinate(x, y, z);
        boolean hasZ = Double.isNaN(z) == false;
        WellKnownText.checkZorMAttribute(explicitZ, hasZ);
        // WKB circle format: type x y [z] r — z comes before radius
        writeInt(out, scratch, hasZ ? 1017 : 17);
        writeDouble(out, scratch, x);
        writeDouble(out, scratch, y);
        if (hasZ) {
            writeDouble(out, scratch, z);
        }
        writeDouble(out, scratch, r);
    }

    private static void writeWKBBBox(
        StreamTokenizer stream,
        ByteArrayOutputStream out,
        ByteBuffer scratch,
        boolean explicitZ,
        GeometryValidator validator
    ) throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            throw new IllegalArgumentException("Empty ENVELOPE cannot be represented in WKB");
        }
        // TODO: Add 3D bbox support (consistent with WellKnownText.parseBBox)
        double minX = WellKnownText.nextNumber(stream);
        WellKnownText.nextComma(stream);
        double maxX = WellKnownText.nextNumber(stream);
        WellKnownText.nextComma(stream);
        double maxY = WellKnownText.nextNumber(stream);
        WellKnownText.nextComma(stream);
        double minY = WellKnownText.nextNumber(stream);
        WellKnownText.nextCloser(stream);
        validator.validateBBox(minX, maxX, maxY, minY);
        validator.validateCoordinate(minX, minY, Double.NaN);
        validator.validateCoordinate(maxX, maxY, Double.NaN);
        WellKnownText.checkZorMAttribute(explicitZ, false);
        writeInt(out, scratch, 18);
        writeDouble(out, scratch, minX);
        writeDouble(out, scratch, maxX);
        writeDouble(out, scratch, maxY);
        writeDouble(out, scratch, minY);
    }

    /** Returns true if the given WKB byte array represents a geometry type with Z coordinates. */
    private static boolean wkbTypeHasZ(byte[] wkb) {
        if (wkb.length < 5) return false;
        ByteOrder bo = wkb[0] == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        int type = ByteBuffer.wrap(wkb, 1, 4).order(bo).getInt();
        return type >= 1001;
    }

    /** Holds parallel x (lons), y (lats) and optional z (alts) coordinate lists parsed from WKT. */
    private record CoordsList(ArrayList<Double> lons, ArrayList<Double> lats, ArrayList<Double> alts) {
        boolean hasZ() {
            return alts.isEmpty() == false;
        }

        int size() {
            return lons.size();
        }
    }

    /** Reads a linestring body from WKT: either EMPTY or ( coords ) */
    private static CoordsList wktReadLineStringCoords(StreamTokenizer stream, GeometryValidator validator) throws IOException,
        ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            return new CoordsList(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
        return wktReadCoordinates(stream, validator);
    }

    /** Reads polygon rings from WKT: either EMPTY or ( ( outer ) (, ( hole ) )* ) */
    private static List<CoordsList> wktReadPolygonRings(StreamTokenizer stream, boolean coerce, GeometryValidator validator)
        throws IOException, ParseException {
        if (WellKnownText.nextEmptyOrOpen(stream).equals(WellKnownText.EMPTY)) {
            return Collections.emptyList();
        }
        List<CoordsList> rings = new ArrayList<>();
        WellKnownText.nextOpener(stream);
        rings.add(wktReadRing(stream, coerce, validator));
        while (WellKnownText.nextCloserOrComma(stream).equals(WellKnownText.COMMA)) {
            WellKnownText.nextOpener(stream);
            rings.add(wktReadRing(stream, coerce, validator));
        }
        return rings;
    }

    /** Reads coordinate pairs until the closing ) which is consumed, validating each coordinate. */
    private static CoordsList wktReadCoordinates(StreamTokenizer stream, GeometryValidator validator) throws IOException, ParseException {
        ArrayList<Double> lons = new ArrayList<>(), lats = new ArrayList<>(), alts = new ArrayList<>();
        WellKnownText.parseCoordinates(stream, lats, lons, alts);
        for (int i = 0; i < lons.size(); i++) {
            validator.validateCoordinate(lons.get(i), lats.get(i), alts.isEmpty() ? Double.NaN : alts.get(i));
        }
        return new CoordsList(lons, lats, alts);
    }

    /** Reads ring coordinates (like wktReadCoordinates but auto-closes the ring if coerce is true). */
    private static CoordsList wktReadRing(StreamTokenizer stream, boolean coerce, GeometryValidator validator) throws IOException,
        ParseException {
        ArrayList<Double> lons = new ArrayList<>(), lats = new ArrayList<>(), alts = new ArrayList<>();
        WellKnownText.parseCoordinates(stream, lats, lons, alts);
        WellKnownText.closeLinearRingIfCoerced(lats, lons, alts, coerce);
        for (int i = 0; i < lons.size(); i++) {
            validator.validateCoordinate(lons.get(i), lats.get(i), alts.isEmpty() ? Double.NaN : alts.get(i));
        }
        return new CoordsList(lons, lats, alts);
    }

    private static void writeCoordinateList(ByteArrayOutputStream out, ByteBuffer scratch, CoordsList coords) throws IOException {
        boolean hasZ = coords.hasZ();
        for (int i = 0; i < coords.size(); i++) {
            writeDouble(out, scratch, coords.lons.get(i));
            writeDouble(out, scratch, coords.lats.get(i));
            if (hasZ) {
                writeDouble(out, scratch, coords.alts.get(i));
            }
        }
    }

    /**
     * Reads a {@link Geometry} from the given WKB byte array.
     */
    public static Geometry fromWKB(GeometryValidator validator, boolean coerce, byte[] wkb) {
        return fromWKB(validator, coerce, wkb, 0, wkb.length);
    }

    /**
     * Reads a {@link Geometry} from the given WKB byte array with offset.
     */
    public static Geometry fromWKB(GeometryValidator validator, boolean coerce, byte[] wkb, int offset, int length) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(wkb, offset, length);
        final Geometry geometry = parseGeometry(byteBuffer, coerce);
        validator.validate(geometry);
        return geometry;
    }

    private static Geometry parseGeometry(ByteBuffer byteBuffer, boolean coerce) {
        byteBuffer.order(byteBuffer.get() == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        final int type = byteBuffer.getInt();
        return switch (type) {
            case 1 -> parsePoint(byteBuffer, false);
            case 1001 -> parsePoint(byteBuffer, true);
            case 2 -> parseLine(byteBuffer, false);
            case 1002 -> parseLine(byteBuffer, true);
            case 3 -> parsePolygon(byteBuffer, false, coerce);
            case 1003 -> parsePolygon(byteBuffer, true, coerce);
            case 4, 1004 -> parseMultiPoint(byteBuffer);
            case 5, 1005 -> parseMultiLine(byteBuffer);
            case 6, 1006 -> parseMultiPolygon(byteBuffer, coerce);
            case 7, 1007 -> parseGeometryCollection(byteBuffer, coerce);
            case 17 -> parseCircle(byteBuffer, false);
            case 1017 -> parseCircle(byteBuffer, true);
            case 18 -> parseBBox(byteBuffer, false);
            case 1018 -> parseBBox(byteBuffer, true);
            default -> throw new IllegalArgumentException("Unknown geometry type: " + type);
        };
    }

    private static Point parsePoint(ByteBuffer byteBuffer, boolean hasZ) {
        if (hasZ) {
            return new Point(byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble());
        } else {
            return new Point(byteBuffer.getDouble(), byteBuffer.getDouble());
        }
    }

    private static Line parseLine(ByteBuffer byteBuffer, boolean hasZ) {
        final int length = byteBuffer.getInt();
        if (length == 0) {
            return Line.EMPTY;
        }
        final double[] lats = new double[length];
        final double[] lons = new double[length];
        final double[] alts = hasZ ? new double[length] : null;
        for (int i = 0; i < length; i++) {
            lons[i] = byteBuffer.getDouble();
            lats[i] = byteBuffer.getDouble();
            if (hasZ) {
                alts[i] = byteBuffer.getDouble();
            }
        }
        if (hasZ) {
            return new Line(lons, lats, alts);
        } else {
            return new Line(lons, lats);
        }
    }

    private static Polygon parsePolygon(ByteBuffer byteBuffer, boolean hasZ, boolean coerce) {
        final int rings = byteBuffer.getInt();
        if (rings == 0) {
            return Polygon.EMPTY;
        }
        final LinearRing shell = parseLinearRing(byteBuffer, hasZ, coerce);
        final List<LinearRing> holes = new ArrayList<>();
        for (int i = 1; i < rings; i++) {
            holes.add(parseLinearRing(byteBuffer, hasZ, coerce));
        }
        if (holes.isEmpty()) {
            return new Polygon(shell);
        } else {
            return new Polygon(shell, Collections.unmodifiableList(holes));
        }
    }

    private static MultiPoint parseMultiPoint(ByteBuffer byteBuffer) {
        final int numPoints = byteBuffer.getInt();
        if (numPoints == 0) {
            return MultiPoint.EMPTY;
        }
        final List<Point> points = new ArrayList<>(numPoints);
        for (int i = 0; i < numPoints; i++) {
            final Geometry geometry = parseGeometry(byteBuffer, false);
            if (geometry instanceof Point p) {
                points.add(p);
            } else {
                throw new IllegalArgumentException("Expected a " + ShapeType.POINT + ", got [" + geometry.type() + "]");
            }

        }
        return new MultiPoint(Collections.unmodifiableList(points));
    }

    private static MultiLine parseMultiLine(ByteBuffer byteBuffer) {
        final int numLines = byteBuffer.getInt();
        if (numLines == 0) {
            return MultiLine.EMPTY;
        }
        final List<Line> lines = new ArrayList<>(numLines);
        for (int i = 0; i < numLines; i++) {
            final Geometry geometry = parseGeometry(byteBuffer, false);
            if (geometry instanceof Line l) {
                lines.add(l);
            } else {
                throw new IllegalArgumentException("Expected a " + ShapeType.LINESTRING + ", got [" + geometry.type() + "]");
            }
        }
        return new MultiLine(Collections.unmodifiableList(lines));
    }

    private static MultiPolygon parseMultiPolygon(ByteBuffer byteBuffer, boolean coerce) {
        final int numPolygons = byteBuffer.getInt();
        if (numPolygons == 0) {
            return MultiPolygon.EMPTY;
        }
        final List<Polygon> polygons = new ArrayList<>(numPolygons);
        for (int i = 0; i < numPolygons; i++) {
            final Geometry geometry = parseGeometry(byteBuffer, coerce);
            if (geometry instanceof Polygon p) {
                polygons.add(p);
            } else {
                throw new IllegalArgumentException("Expected a " + ShapeType.POLYGON + ", got [" + geometry.type() + "]");
            }

        }
        return new MultiPolygon(Collections.unmodifiableList(polygons));
    }

    private static GeometryCollection<Geometry> parseGeometryCollection(ByteBuffer byteBuffer, boolean coerce) {
        final int numShapes = byteBuffer.getInt();
        if (numShapes == 0) {
            return GeometryCollection.EMPTY;
        }
        final List<Geometry> shapes = new ArrayList<>(numShapes);
        for (int i = 0; i < numShapes; i++) {
            shapes.add(parseGeometry(byteBuffer, coerce));
        }
        return new GeometryCollection<>(shapes);
    }

    private static LinearRing parseLinearRing(ByteBuffer byteBuffer, boolean hasZ, boolean coerce) {
        final int length = byteBuffer.getInt();
        if (length == 0) {
            return LinearRing.EMPTY;
        }
        double[] lons = new double[length];
        double[] lats = new double[length];
        double[] alts = hasZ ? new double[length] : null;
        for (int i = 0; i < length; i++) {
            lons[i] = byteBuffer.getDouble();
            lats[i] = byteBuffer.getDouble();
            if (hasZ) {
                alts[i] = byteBuffer.getDouble();
            }
        }
        if (linearRingNeedsCoerced(lats, lons, alts, coerce)) {
            lons = coerce(lons);
            lats = coerce(lats);
            if (hasZ) {
                alts = coerce(alts);
            }
        }
        if (hasZ) {
            return new LinearRing(lons, lats, alts);
        } else {
            return new LinearRing(lons, lats);
        }
    }

    private static boolean linearRingNeedsCoerced(double[] lons, double[] lats, double[] alts, boolean coerce) {
        assert lats.length == lons.length && (alts == null || alts.length == lats.length);
        assert lats.length > 0;
        if (coerce == false) {
            return false;
        }
        final int last = lons.length - 1;
        return lons[0] != lons[last] || lats[0] != lats[last] || (alts != null && alts[0] != alts[last]);
    }

    private static double[] coerce(double[] array) {
        double[] copy = new double[array.length + 1];
        System.arraycopy(array, 0, copy, 0, array.length);
        copy[array.length] = copy[0];
        return copy;
    }

    private static Rectangle parseBBox(ByteBuffer byteBuffer, boolean hasZ) {
        if (hasZ) {
            return new Rectangle(
                byteBuffer.getDouble(),
                byteBuffer.getDouble(),
                byteBuffer.getDouble(),
                byteBuffer.getDouble(),
                byteBuffer.getDouble(),
                byteBuffer.getDouble()
            );
        } else {
            return new Rectangle(byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble());
        }
    }

    private static Circle parseCircle(ByteBuffer byteBuffer, boolean hasZ) {
        if (hasZ) {
            return new Circle(byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble());
        } else {
            return new Circle(byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble());
        }
    }
}
