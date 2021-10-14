/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.builders;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.legacygeo.GeoShapeType;
import org.elasticsearch.legacygeo.parsers.GeoWKTParser;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class MultiLineStringBuilder extends ShapeBuilder<JtsGeometry, org.elasticsearch.geometry.Geometry, MultiLineStringBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.MULTILINESTRING;

    private final ArrayList<LineStringBuilder> lines = new ArrayList<>();

    /**
     * Read from a stream.
     */
    public MultiLineStringBuilder(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            linestring(new LineStringBuilder(in));
        }
    }

    public MultiLineStringBuilder() {
        super();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(lines.size());
        for (LineStringBuilder line : lines) {
            line.writeTo(out);
        }
    }

    public MultiLineStringBuilder linestring(LineStringBuilder line) {
        this.lines.add(line);
        return this;
    }

    public Coordinate[][] coordinates() {
        Coordinate[][] result = new Coordinate[lines.size()][];
        for (int i = 0; i < result.length; i++) {
            result[i] = lines.get(i).coordinates(false);
        }
        return result;
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    protected StringBuilder contentToWKT() {
        final StringBuilder sb = new StringBuilder();
        if (lines.isEmpty()) {
            sb.append(GeoWKTParser.EMPTY);
        } else {
            sb.append(GeoWKTParser.LPAREN);
            if (lines.size() > 0) {
                sb.append(ShapeBuilder.coordinateListToWKT(lines.get(0).coordinates));
            }
            for (int i = 1; i < lines.size(); ++i) {
                sb.append(GeoWKTParser.COMMA);
                sb.append(ShapeBuilder.coordinateListToWKT(lines.get(i).coordinates));
            }
            sb.append(GeoWKTParser.RPAREN);
        }
        return sb;
    }

    public int numDimensions() {
        if (lines == null || lines.isEmpty()) {
            throw new IllegalStateException("unable to get number of dimensions, " + "LineStrings have not yet been initialized");
        }
        return lines.get(0).numDimensions();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
        builder.startArray();
        for (LineStringBuilder line : lines) {
            line.coordinatesToXcontent(builder, false);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public JtsGeometry buildS4J() {
        final Geometry geometry;
        if (wrapdateline) {
            ArrayList<LineString> parts = new ArrayList<>();
            for (LineStringBuilder line : lines) {
                LineStringBuilder.decomposeS4J(FACTORY, line.coordinates(false), parts);
            }
            if (parts.size() == 1) {
                geometry = parts.get(0);
            } else {
                LineString[] lineStrings = parts.toArray(new LineString[parts.size()]);
                geometry = FACTORY.createMultiLineString(lineStrings);
            }
        } else {
            LineString[] lineStrings = new LineString[lines.size()];
            Iterator<LineStringBuilder> iterator = lines.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                lineStrings[i] = FACTORY.createLineString(iterator.next().coordinates(false));
            }
            geometry = FACTORY.createMultiLineString(lineStrings);
        }
        return jtsGeometry(geometry);
    }

    @Override
    public org.elasticsearch.geometry.Geometry buildGeometry() {
        if (lines.isEmpty()) {
            return MultiLine.EMPTY;
        }
        List<Line> linestrings = new ArrayList<>(lines.size());
        for (int i = 0; i < lines.size(); ++i) {
            LineStringBuilder lsb = lines.get(i);
            linestrings.add(
                new Line(lsb.coordinates.stream().mapToDouble(c -> c.x).toArray(), lsb.coordinates.stream().mapToDouble(c -> c.y).toArray())
            );
        }
        return new MultiLine(linestrings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lines);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MultiLineStringBuilder other = (MultiLineStringBuilder) obj;
        return Objects.equals(lines, other.lines);
    }
}
