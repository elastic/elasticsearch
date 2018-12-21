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

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.XRectangle2D;
import org.apache.lucene.index.PointValues.Relation;

/**
 * Finds all previously indexed shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link XLatLonShape#createIndexableFields} added per document.
 *
 **/
final class XLatLonShapeBoundingBoxQuery extends XLatLonShapeQuery {
  final XRectangle2D rectangle2D;

  XLatLonShapeBoundingBoxQuery(String field, XLatLonShape.QueryRelation queryRelation,
                               double minLat, double maxLat, double minLon, double maxLon) {
    super(field, queryRelation);
    Rectangle rectangle = new Rectangle(minLat, maxLat, minLon, maxLon);
    this.rectangle2D = XRectangle2D.create(rectangle);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    return rectangle2D.relateRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
  }

  /** returns true if the query matches the encoded triangle */
  @Override
  protected boolean queryMatches(byte[] t, int[] scratchTriangle) {
    // decode indexed triangle
    XLatLonShape.decodeTriangle(t, scratchTriangle);

    int aY = scratchTriangle[0];
    int aX = scratchTriangle[1];
    int bY = scratchTriangle[2];
    int bX = scratchTriangle[3];
    int cY = scratchTriangle[4];
    int cX = scratchTriangle[5];

    if (queryRelation == XLatLonShape.QueryRelation.WITHIN) {
      return rectangle2D.containsTriangle(aX, aY, bX, bY, cX, cY);
    }
    return rectangle2D.intersectsTriangle(aX, aY, bX, bY, cX, cY);
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && rectangle2D.equals(((XLatLonShapeBoundingBoxQuery)o).rectangle2D);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + rectangle2D.hashCode();
    return hash;
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
    sb.append(rectangle2D.toString());
    return sb.toString();
  }
}
