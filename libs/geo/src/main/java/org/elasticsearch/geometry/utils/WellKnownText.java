/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Utility class for converting to and from WKT
 */
public class WellKnownText {

    public static final String EMPTY = "EMPTY";
    public static final String SPACE = " ";
    public static final String LPAREN = "(";
    public static final String RPAREN = ")";
    public static final String COMMA = ",";
    public static final String NAN = "NaN";

    private static final String NUMBER = "<NUMBER>";
    private static final String EOF = "END-OF-STREAM";
    private static final String EOL = "END-OF-LINE";

    private WellKnownText() {
    }

    public static String toWKT(Geometry geometry) {
        StringBuilder builder = new StringBuilder();
        toWKT(geometry, builder);
        return builder.toString();
    }

    private static void toWKT(Geometry geometry, StringBuilder sb) {
        sb.append(getWKTName(geometry));
        sb.append(SPACE);
        if (geometry.isEmpty()) {
            sb.append(EMPTY);
        } else {
            geometry.visit(new GeometryVisitor<Void, RuntimeException>() {
                @Override
                public Void visit(Circle circle) {
                    sb.append(LPAREN);
                    visitPoint(circle.getX(), circle.getY(), Double.NaN);
                    sb.append(SPACE);
                    sb.append(circle.getRadiusMeters());
                    if (circle.hasZ()) {
                        sb.append(SPACE);
                        sb.append(circle.getZ());
                    }
                    sb.append(RPAREN);
                    return null;
                }

                @Override
                public Void visit(GeometryCollection<?> collection) {
                    if (collection.size() == 0) {
                        sb.append(EMPTY);
                    } else {
                        sb.append(LPAREN);
                        toWKT(collection.get(0), sb);
                        for (int i = 1; i < collection.size(); ++i) {
                            sb.append(COMMA);
                            toWKT(collection.get(i), sb);
                        }
                        sb.append(RPAREN);
                    }
                    return null;
                }

                @Override
                public Void visit(Line line) {
                    sb.append(LPAREN);
                    visitPoint(line.getX(0), line.getY(0), line.getZ(0));
                    for (int i = 1; i < line.length(); ++i) {
                        sb.append(COMMA);
                        sb.append(SPACE);
                        visitPoint(line.getX(i), line.getY(i), line.getZ(i));
                    }
                    sb.append(RPAREN);
                    return null;
                }

                @Override
                public Void visit(LinearRing ring) {
                    throw new IllegalArgumentException("Linear ring is not supported by WKT");
                }

                @Override
                public Void visit(MultiLine multiLine) {
                    visitCollection(multiLine);
                    return null;
                }

                @Override
                public Void visit(MultiPoint multiPoint) {
                    if (multiPoint.isEmpty()) {
                        sb.append(EMPTY);
                        return null;
                    }
                    // walk through coordinates:
                    sb.append(LPAREN);
                    visitPoint(multiPoint.get(0).getX(), multiPoint.get(0).getY(), multiPoint.get(0).getZ());
                    for (int i = 1; i < multiPoint.size(); ++i) {
                        sb.append(COMMA);
                        sb.append(SPACE);
                        Point point = multiPoint.get(i);
                        visitPoint(point.getX(), point.getY(), point.getZ());
                    }
                    sb.append(RPAREN);
                    return null;
                }

                @Override
                public Void visit(MultiPolygon multiPolygon) {
                    visitCollection(multiPolygon);
                    return null;
                }

                @Override
                public Void visit(Point point) {
                    if (point.isEmpty()) {
                        sb.append(EMPTY);
                    } else {
                        sb.append(LPAREN);
                        visitPoint(point.getX(), point.getY(), point.getZ());
                        sb.append(RPAREN);
                    }
                    return null;
                }

                private void visitPoint(double lon, double lat, double alt) {
                    sb.append(lon).append(SPACE).append(lat);
                    if (Double.isNaN(alt) == false) {
                        sb.append(SPACE).append(alt);
                    }
                }

                private void visitCollection(GeometryCollection<?> collection) {
                    if (collection.size() == 0) {
                        sb.append(EMPTY);
                    } else {
                        sb.append(LPAREN);
                        collection.get(0).visit(this);
                        for (int i = 1; i < collection.size(); ++i) {
                            sb.append(COMMA);
                            collection.get(i).visit(this);
                        }
                        sb.append(RPAREN);
                    }
                }

                @Override
                public Void visit(Polygon polygon) {
                    sb.append(LPAREN);
                    visit((Line) polygon.getPolygon());
                    int numberOfHoles = polygon.getNumberOfHoles();
                    for (int i = 0; i < numberOfHoles; ++i) {
                        sb.append(", ");
                        visit((Line) polygon.getHole(i));
                    }
                    sb.append(RPAREN);
                    return null;
                }

                @Override
                public Void visit(Rectangle rectangle) {
                    sb.append(LPAREN);
                    // minX, maxX, maxY, minY
                    sb.append(rectangle.getMinX());
                    sb.append(COMMA);
                    sb.append(SPACE);
                    sb.append(rectangle.getMaxX());
                    sb.append(COMMA);
                    sb.append(SPACE);
                    sb.append(rectangle.getMaxY());
                    sb.append(COMMA);
                    sb.append(SPACE);
                    sb.append(rectangle.getMinY());
                    if (rectangle.hasZ()) {
                        sb.append(COMMA);
                        sb.append(SPACE);
                        sb.append(rectangle.getMinZ());
                        sb.append(COMMA);
                        sb.append(SPACE);
                        sb.append(rectangle.getMaxZ());
                    }
                    sb.append(RPAREN);
                    return null;
                }
            });
        }
    }

    public static Geometry fromWKT(GeometryValidator validator, boolean coerce, String wkt) throws IOException, ParseException {
        StringReader reader = new StringReader(wkt);
        try {
            // setup the tokenizer; configured to read words w/o numbers
            StreamTokenizer tokenizer = new StreamTokenizer(reader);
            tokenizer.resetSyntax();
            tokenizer.wordChars('a', 'z');
            tokenizer.wordChars('A', 'Z');
            tokenizer.wordChars(128 + 32, 255);
            tokenizer.wordChars('0', '9');
            tokenizer.wordChars('-', '-');
            tokenizer.wordChars('+', '+');
            tokenizer.wordChars('.', '.');
            tokenizer.whitespaceChars(' ', ' ');
            tokenizer.whitespaceChars('\t', '\t');
            tokenizer.whitespaceChars('\r', '\r');
            tokenizer.whitespaceChars('\n', '\n');
            tokenizer.commentChar('#');
            Geometry geometry = parseGeometry(tokenizer, coerce);
            validator.validate(geometry);
            return geometry;
        } finally {
            reader.close();
        }
    }

    /**
     * parse geometry from the stream tokenizer
     */
    private static Geometry parseGeometry(StreamTokenizer stream, boolean coerce) throws IOException, ParseException {
        final String type = nextWord(stream).toLowerCase(Locale.ROOT);
        switch (type) {
            case "point":
                return parsePoint(stream);
            case "multipoint":
                return parseMultiPoint(stream);
            case "linestring":
                return parseLine(stream);
            case "multilinestring":
                return parseMultiLine(stream);
            case "polygon":
                return parsePolygon(stream, coerce);
            case "multipolygon":
                return parseMultiPolygon(stream, coerce);
            case "bbox":
                return parseBBox(stream);
            case "geometrycollection":
                return parseGeometryCollection(stream, coerce);
            case "circle": // Not part of the standard, but we need it for internal serialization
                return parseCircle(stream);
        }
        throw new IllegalArgumentException("Unknown geometry type: " + type);
    }

    private static GeometryCollection<Geometry> parseGeometryCollection(StreamTokenizer stream, boolean coerce)
        throws IOException, ParseException {
        if (nextEmptyOrOpen(stream).equals(EMPTY)) {
            return GeometryCollection.EMPTY;
        }
        List<Geometry> shapes = new ArrayList<>();
        shapes.add(parseGeometry(stream, coerce));
        while (nextCloserOrComma(stream).equals(COMMA)) {
            shapes.add(parseGeometry(stream, coerce));
        }
        return new GeometryCollection<>(shapes);
    }

    private static Point parsePoint(StreamTokenizer stream) throws IOException, ParseException {
        if (nextEmptyOrOpen(stream).equals(EMPTY)) {
            return Point.EMPTY;
        }
        double lon = nextNumber(stream);
        double lat = nextNumber(stream);
        Point pt;
        if (isNumberNext(stream)) {
            pt = new Point(lon, lat, nextNumber(stream));
        } else {
            pt = new Point(lon, lat);
        }
        nextCloser(stream);
        return pt;
    }

    private static void parseCoordinates(StreamTokenizer stream, ArrayList<Double> lats, ArrayList<Double> lons, ArrayList<Double> alts)
        throws IOException, ParseException {
        parseCoordinate(stream, lats, lons, alts);
        while (nextCloserOrComma(stream).equals(COMMA)) {
            parseCoordinate(stream, lats, lons, alts);
        }
    }

    private static void parseCoordinate(StreamTokenizer stream, ArrayList<Double> lats, ArrayList<Double> lons, ArrayList<Double> alts)
        throws IOException, ParseException {
        lons.add(nextNumber(stream));
        lats.add(nextNumber(stream));
        if (isNumberNext(stream)) {
            alts.add(nextNumber(stream));
        }
        if (alts.isEmpty() == false && alts.size() != lons.size()) {
            throw new ParseException("coordinate dimensions do not match: " + tokenString(stream), stream.lineno());
        }
    }

    private static MultiPoint parseMultiPoint(StreamTokenizer stream) throws IOException, ParseException {
        String token = nextEmptyOrOpen(stream);
        if (token.equals(EMPTY)) {
            return MultiPoint.EMPTY;
        }
        ArrayList<Double> lats = new ArrayList<>();
        ArrayList<Double> lons = new ArrayList<>();
        ArrayList<Double> alts = new ArrayList<>();
        ArrayList<Point> points = new ArrayList<>();
        parseCoordinates(stream, lats, lons, alts);
        for (int i = 0; i < lats.size(); i++) {
            if (alts.isEmpty()) {
                points.add(new Point(lons.get(i), lats.get(i)));
            } else {
                points.add(new Point(lons.get(i), lats.get(i), alts.get(i)));
            }
        }
        return new MultiPoint(Collections.unmodifiableList(points));
    }

    private static Line parseLine(StreamTokenizer stream) throws IOException, ParseException {
        String token = nextEmptyOrOpen(stream);
        if (token.equals(EMPTY)) {
            return Line.EMPTY;
        }
        ArrayList<Double> lats = new ArrayList<>();
        ArrayList<Double> lons = new ArrayList<>();
        ArrayList<Double> alts = new ArrayList<>();
        parseCoordinates(stream, lats, lons, alts);
        if (alts.isEmpty()) {
            return new Line(toArray(lons), toArray(lats));
        } else {
            return new Line(toArray(lons), toArray(lats), toArray(alts));
        }
    }

    private static MultiLine parseMultiLine(StreamTokenizer stream) throws IOException, ParseException {
        String token = nextEmptyOrOpen(stream);
        if (token.equals(EMPTY)) {
            return MultiLine.EMPTY;
        }
        ArrayList<Line> lines = new ArrayList<>();
        lines.add(parseLine(stream));
        while (nextCloserOrComma(stream).equals(COMMA)) {
            lines.add(parseLine(stream));
        }
        return new MultiLine(Collections.unmodifiableList(lines));
    }

    private static LinearRing parsePolygonHole(StreamTokenizer stream, boolean coerce) throws IOException, ParseException {
        nextOpener(stream);
        ArrayList<Double> lats = new ArrayList<>();
        ArrayList<Double> lons = new ArrayList<>();
        ArrayList<Double> alts = new ArrayList<>();
        parseCoordinates(stream, lats, lons, alts);
        closeLinearRingIfCoerced(lats, lons, alts, coerce);
        if (alts.isEmpty()) {
            return new LinearRing(toArray(lons), toArray(lats));
        } else {
            return new LinearRing(toArray(lons), toArray(lats), toArray(alts));
        }
    }

    private static Polygon parsePolygon(StreamTokenizer stream, boolean coerce) throws IOException, ParseException {
        if (nextEmptyOrOpen(stream).equals(EMPTY)) {
            return Polygon.EMPTY;
        }
        nextOpener(stream);
        ArrayList<Double> lats = new ArrayList<>();
        ArrayList<Double> lons = new ArrayList<>();
        ArrayList<Double> alts = new ArrayList<>();
        parseCoordinates(stream, lats, lons, alts);
        ArrayList<LinearRing> holes = new ArrayList<>();
        while (nextCloserOrComma(stream).equals(COMMA)) {
            holes.add(parsePolygonHole(stream, coerce));
        }
        closeLinearRingIfCoerced(lats, lons, alts, coerce);
        LinearRing shell;
        if (alts.isEmpty()) {
            shell = new LinearRing(toArray(lons), toArray(lats));
        } else {
            shell = new LinearRing(toArray(lons), toArray(lats), toArray(alts));
        }
        if (holes.isEmpty()) {
            return new Polygon(shell);
        } else {
            return new Polygon(shell, Collections.unmodifiableList(holes));
        }
    }

    /**
     * Treats supplied arrays as coordinates of a linear ring. If the ring is not closed and coerce is set to true,
     * the first set of coordinates (lat, lon and alt if available) are added to the end of the arrays.
     */
    private static void closeLinearRingIfCoerced(ArrayList<Double> lats, ArrayList<Double> lons, ArrayList<Double> alts, boolean coerce) {
        if (coerce && lats.isEmpty() == false && lons.isEmpty() == false) {
            int last = lats.size() - 1;
            if (lats.get(0).equals(lats.get(last)) == false || lons.get(0).equals(lons.get(last)) == false ||
                (alts.isEmpty() == false && alts.get(0).equals(alts.get(last)) == false)) {
                lons.add(lons.get(0));
                lats.add(lats.get(0));
                if (alts.isEmpty() == false) {
                    alts.add(alts.get(0));
                }
            }
        }
    }

    private static MultiPolygon parseMultiPolygon(StreamTokenizer stream, boolean coerce) throws IOException, ParseException {
        String token = nextEmptyOrOpen(stream);
        if (token.equals(EMPTY)) {
            return MultiPolygon.EMPTY;
        }
        ArrayList<Polygon> polygons = new ArrayList<>();
        polygons.add(parsePolygon(stream, coerce));
        while (nextCloserOrComma(stream).equals(COMMA)) {
            polygons.add(parsePolygon(stream, coerce));
        }
        return new MultiPolygon(Collections.unmodifiableList(polygons));
    }

    private static Rectangle parseBBox(StreamTokenizer stream) throws IOException, ParseException {
        if (nextEmptyOrOpen(stream).equals(EMPTY)) {
            return Rectangle.EMPTY;
        }
        // TODO: Add 3D support
        double minLon = nextNumber(stream);
        nextComma(stream);
        double maxLon = nextNumber(stream);
        nextComma(stream);
        double maxLat = nextNumber(stream);
        nextComma(stream);
        double minLat = nextNumber(stream);
        nextCloser(stream);
        return new Rectangle(minLon, maxLon, maxLat, minLat);
    }


    private static Circle parseCircle(StreamTokenizer stream) throws IOException, ParseException {
        if (nextEmptyOrOpen(stream).equals(EMPTY)) {
            return Circle.EMPTY;
        }
        double lon = nextNumber(stream);
        double lat = nextNumber(stream);
        double radius = nextNumber(stream);
        double alt = Double.NaN;
        if (isNumberNext(stream)) {
            alt = nextNumber(stream);
        }
        Circle circle = new Circle(lon, lat, alt, radius);
        nextCloser(stream);
        return circle;
    }

    /**
     * next word in the stream
     */
    private static String nextWord(StreamTokenizer stream) throws ParseException, IOException {
        switch (stream.nextToken()) {
            case StreamTokenizer.TT_WORD:
                final String word = stream.sval;
                return word.equalsIgnoreCase(EMPTY) ? EMPTY : word;
            case '(':
                return LPAREN;
            case ')':
                return RPAREN;
            case ',':
                return COMMA;
        }
        throw new ParseException("expected word but found: " + tokenString(stream), stream.lineno());
    }

    private static double nextNumber(StreamTokenizer stream) throws IOException, ParseException {
        if (stream.nextToken() == StreamTokenizer.TT_WORD) {
            if (stream.sval.equalsIgnoreCase(NAN)) {
                return Double.NaN;
            } else {
                try {
                    return Double.parseDouble(stream.sval);
                } catch (NumberFormatException e) {
                    throw new ParseException("invalid number found: " + stream.sval, stream.lineno());
                }
            }
        }
        throw new ParseException("expected number but found: " + tokenString(stream), stream.lineno());
    }

    private static String tokenString(StreamTokenizer stream) {
        switch (stream.ttype) {
            case StreamTokenizer.TT_WORD:
                return stream.sval;
            case StreamTokenizer.TT_EOF:
                return EOF;
            case StreamTokenizer.TT_EOL:
                return EOL;
            case StreamTokenizer.TT_NUMBER:
                return NUMBER;
        }
        return "'" + (char) stream.ttype + "'";
    }

    private static boolean isNumberNext(StreamTokenizer stream) throws IOException {
        final int type = stream.nextToken();
        stream.pushBack();
        return type == StreamTokenizer.TT_WORD;
    }

    private static String nextEmptyOrOpen(StreamTokenizer stream) throws IOException, ParseException {
        final String next = nextWord(stream);
        if (next.equals(EMPTY) || next.equals(LPAREN)) {
            return next;
        }
        throw new ParseException("expected " + EMPTY + " or " + LPAREN
            + " but found: " + tokenString(stream), stream.lineno());
    }

    private static String nextCloser(StreamTokenizer stream) throws IOException, ParseException {
        if (nextWord(stream).equals(RPAREN)) {
            return RPAREN;
        }
        throw new ParseException("expected " + RPAREN + " but found: " + tokenString(stream), stream.lineno());
    }

    private static String nextComma(StreamTokenizer stream) throws IOException, ParseException {
        if (nextWord(stream).equals(COMMA)) {
            return COMMA;
        }
        throw new ParseException("expected " + COMMA + " but found: " + tokenString(stream), stream.lineno());
    }

    private static String nextOpener(StreamTokenizer stream) throws IOException, ParseException {
        if (nextWord(stream).equals(LPAREN)) {
            return LPAREN;
        }
        throw new ParseException("expected " + LPAREN + " but found: " + tokenString(stream), stream.lineno());
    }

    private static String nextCloserOrComma(StreamTokenizer stream) throws IOException, ParseException {
        String token = nextWord(stream);
        if (token.equals(COMMA) || token.equals(RPAREN)) {
            return token;
        }
        throw new ParseException("expected " + COMMA + " or " + RPAREN
            + " but found: " + tokenString(stream), stream.lineno());
    }

    private static String getWKTName(Geometry geometry) {
        return geometry.visit(new GeometryVisitor<String, RuntimeException>() {
            @Override
            public String visit(Circle circle) {
                return "CIRCLE";
            }

            @Override
            public String visit(GeometryCollection<?> collection) {
                return "GEOMETRYCOLLECTION";
            }

            @Override
            public String visit(Line line) {
                return "LINESTRING";
            }

            @Override
            public String visit(LinearRing ring) {
                throw new UnsupportedOperationException("line ring cannot be serialized using WKT");
            }

            @Override
            public String visit(MultiLine multiLine) {
                return "MULTILINESTRING";
            }

            @Override
            public String visit(MultiPoint multiPoint) {
                return "MULTIPOINT";
            }

            @Override
            public String visit(MultiPolygon multiPolygon) {
                return "MULTIPOLYGON";
            }

            @Override
            public String visit(Point point) {
                return "POINT";
            }

            @Override
            public String visit(Polygon polygon) {
                return "POLYGON";
            }

            @Override
            public String visit(Rectangle rectangle) {
                return "BBOX";
            }
        });
    }

    private static double[] toArray(ArrayList<Double> doubles) {
        return doubles.stream().mapToDouble(i -> i).toArray();
    }

}
