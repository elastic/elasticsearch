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

package org.elasticsearch.geo.geometry;

import java.io.IOException;

import org.elasticsearch.geo.geometry.GeoShape.ConnectedComponent;
import org.elasticsearch.geo.parsers.WKBParser;
import org.apache.lucene.store.OutputStreamDataOutput;

/**
 * Represents a Line on the earth's surface in lat/lon decimal degrees.
 */
public class Line extends MultiPoint implements ConnectedComponent {
    EdgeTree tree;

    public Line(double[] lats, double[] lons) {
        super(lats, lons);
    }

    @Override
    public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
        if (tree == null) {
            tree = createEdgeTree();
        }
        return tree.relate(minLat, maxLat, minLon, maxLon);
    }

    @Override
    public ShapeType type() {
        return ShapeType.LINESTRING;
    }

    public Relation relate(GeoShape other) {
        // not yet implemented
        throw new UnsupportedOperationException("not yet able to relate other GeoShape types to linestrings");
    }

    @Override
    public EdgeTree createEdgeTree() {
        return new EdgeTree(this);

        // NOCOMMIT
//    EdgeTree components[] = new EdgeTree[lines.length];
//    for (int i = 0; i < components.length; i++) {
//      Line gon = lines[i];
//      components[i] = new EdgeTree(gon);
//    }
//    return EdgeTree.createTree(components, 0, components.length - 1, false);
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other) == false) return false;
        Line o = getClass().cast(other);
        if ((tree == null) != (o.tree == null)) return false;
        return tree != null ? tree.equals(o.tree) : true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (tree != null ? tree.hashCode() : 0);
        return result;
    }

    @Override
    protected void appendWKBContent(OutputStreamDataOutput out) throws IOException {
        lineToWKB(lats, lons, out, false);
    }

    public static void lineToWKB(final double[] lats, final double[] lons, OutputStreamDataOutput out, boolean writeHeader) throws IOException {
        if (writeHeader == true) {
            out.writeVInt(WKBParser.ByteOrder.XDR.ordinal());
            out.writeVInt(ShapeType.LINESTRING.wkbOrdinal());
        }
        out.writeVInt(lats.length);  // number of points
        pointsToWKB(lats, lons, out, false);
    }
}
