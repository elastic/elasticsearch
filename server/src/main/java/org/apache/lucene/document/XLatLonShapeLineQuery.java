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
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Line2D;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

import java.util.Arrays;

/**
 * Finds all previously indexed shapes that intersect the specified arbitrary {@code Line}.
 * <p>
 * Note:
 * <ul>
 *    <li>{@code QueryRelation.WITHIN} queries are not yet supported</li>
 *    <li>Dateline crossing is not yet supported</li>
 * </ul>
 * <p>
 * todo:
 * <ul>
 *   <li>Add distance support for buffered queries</li>
 * </ul>
 * <p>The field must be indexed using
 * {@link XLatLonShape#createIndexableFields} added per document.
 *
 **/
final class XLatLonShapeLineQuery extends XLatLonShapeQuery {
  final Line[] lines;
  private final Line2D line2D;

  XLatLonShapeLineQuery(String field, QueryRelation queryRelation, Line... lines) {
    super(field, queryRelation);
    /** line queries do not support within relations, only intersects and disjoint */
    if (queryRelation == QueryRelation.WITHIN) {
      throw new IllegalArgumentException("LatLonShapeLineQuery does not support " + QueryRelation.WITHIN + " queries");
    }

    if (lines == null) {
      throw new IllegalArgumentException("lines must not be null");
    }
    if (lines.length == 0) {
      throw new IllegalArgumentException("lines must not be empty");
    }
    for (int i = 0; i < lines.length; ++i) {
      if (lines[i] == null) {
        throw new IllegalArgumentException("line[" + i + "] must not be null");
      } else if (lines[i].minLon > lines[i].maxLon) {
        throw new IllegalArgumentException("LatLonShapeLineQuery does not currently support querying across dateline.");
      }
    }
    this.lines = lines.clone();
    this.line2D = Line2D.create(lines);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    double minLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return line2D.relate(minLat, maxLat, minLon, maxLon);
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

    if (queryRelation == XLatLonShape.QueryRelation.WITHIN) {
      return line2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
    }
    // INTERSECTS
    return line2D.relateTriangle(alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
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
    sb.append("Line(" + lines[0].toGeoJSON() + ")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && Arrays.equals(lines, ((XLatLonShapeLineQuery)o).lines);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(lines);
    return hash;
  }
}
