/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.document;

import org.apache.lucene.document.XLatLonShape.QueryRelation;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

import java.util.Arrays;

/**
 * Finds all previously indexed shapes that intersect the specified arbitrary.
 *
 * <p>The field must be indexed using
 * {@link XLatLonShape#createIndexableFields} added per document.
 *
 **/
final class XLatLonShapePolygonQuery extends XLatLonShapeQuery {
  final Polygon[] polygons;
  private final Polygon2D poly2D;

  /**
   * Creates a query that matches all indexed shapes to the provided polygons
   */
  XLatLonShapePolygonQuery(String field, QueryRelation queryRelation, Polygon... polygons) {
    super(field, queryRelation);
    if (polygons == null) {
      throw new IllegalArgumentException("polygons must not be null");
    }
    if (polygons.length == 0) {
      throw new IllegalArgumentException("polygons must not be empty");
    }
    for (int i = 0; i < polygons.length; i++) {
      if (polygons[i] == null) {
        throw new IllegalArgumentException("polygon[" + i + "] must not be null");
      } else if (polygons[i].minLon > polygons[i].maxLon) {
        throw new IllegalArgumentException("LatLonShapePolygonQuery does not currently support querying across dateline.");
      }
    }
    this.polygons = polygons.clone();
    this.poly2D = Polygon2D.create(polygons);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {

    double minLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return poly2D.relate(minLat, maxLat, minLon, maxLon);
  }

  @Override
  protected boolean queryMatches(byte[] t, int[] scratchTriangle) {
    XLatLonShape.decodeTriangle(t, scratchTriangle);

    double alat = GeoEncodingUtils.decodeLatitude(scratchTriangle[0]);
    double alon = GeoEncodingUtils.decodeLongitude(scratchTriangle[1]);
    double blat = GeoEncodingUtils.decodeLatitude(scratchTriangle[2]);
    double blon = GeoEncodingUtils.decodeLongitude(scratchTriangle[3]);
    double clat = GeoEncodingUtils.decodeLatitude(scratchTriangle[4]);
    double clon = GeoEncodingUtils.decodeLongitude(scratchTriangle[5]);

    if (queryRelation == QueryRelation.WITHIN) {
      return poly2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
    }
    // INTERSECTS
    return poly2D.relateTriangle(alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append("Polygon(" + polygons[0].toGeoJSON() + ")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && Arrays.equals(polygons, ((XLatLonShapePolygonQuery)o).polygons);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(polygons);
    return hash;
  }
}
