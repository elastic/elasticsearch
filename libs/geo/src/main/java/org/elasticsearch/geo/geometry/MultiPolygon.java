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

import org.elasticsearch.geo.parsers.WKTParser;
import org.apache.lucene.store.OutputStreamDataOutput;

/**
 * Created by nknize on 2/27/17.
 */
public class MultiPolygon extends MultiLine {
    Predicate.PolygonPredicate predicate;

    public MultiPolygon(Polygon... polygons) {
        super(polygons);
    }

    @Override
    public ShapeType type() {
        return ShapeType.MULTIPOLYGON;
    }

    @Override
    public int length() {
        return lines.length;
    }

    @Override
    public Polygon get(int index) {
        checkVertexIndex(index);
        return (Polygon) (lines[index]);
    }

    @Override
    public EdgeTree createEdgeTree() {
        Polygon[] polygons = (Polygon[]) this.lines;
        this.tree = Polygon.createEdgeTree(polygons);
        predicate = Predicate.PolygonPredicate.create(this.boundingBox, tree);
        return predicate.tree;
    }

    public boolean pointInside(int encodedLat, int encodedLon) {
        return predicate.test(encodedLat, encodedLon);
    }

    @Override
    public boolean hasArea() {
        return true;
    }

    @Override
    public double computeArea() {
        assertEdgeTree();
        return this.tree.getArea();
    }

    protected void assertEdgeTree() {
        if (this.tree == null) {
            final Polygon[] polygons = (Polygon[]) this.lines;
            tree = Polygon.createEdgeTree(polygons);
        }
    }

    //  private EdgeTree createEdgeTree(Polygon... polygons) {
//    EdgeTree components[] = new EdgeTree[polygons.length];
//    for (int i = 0; i < components.length; i++) {
//      Polygon gon = polygons[i];
//      Polygon gonHoles[] = gon.getHoles();
//      EdgeTree holes = null;
//      if (gonHoles.length > 0) {
//        holes = createEdgeTree(gonHoles);
//      }
//      components[i] = new EdgeTree(gon, holes);
//    }
//    return EdgeTree.createTree(components, 0, components.length - 1, false);
//  }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        MultiPolygon that = (MultiPolygon) o;
        return predicate.equals(that.predicate);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + predicate.hashCode();
        return result;
    }

    @Override
    protected StringBuilder contentToWKT() {
        final StringBuilder sb = new StringBuilder();
        Polygon[] polygons = (Polygon[]) lines;
        if (polygons.length == 0) {
            sb.append(WKTParser.EMPTY);
        } else {
            sb.append(WKTParser.LPAREN);
            if (polygons.length > 0) {
                sb.append(Polygon.polygonToWKT(polygons[0]));
            }
            for (int i = 1; i < polygons.length; ++i) {
                sb.append(WKTParser.COMMA);
                sb.append(Polygon.polygonToWKT(polygons[i]));
            }
            sb.append(WKTParser.RPAREN);
        }
        return sb;
    }

    @Override
    protected void appendWKBContent(OutputStreamDataOutput out) throws IOException {
        int numPolys = length();
        out.writeVInt(numPolys);
        for (int i = 0; i < numPolys; ++i) {
            Polygon polygon = this.get(i);
            Polygon.polygonToWKB(polygon, out, true);
        }
    }
}
