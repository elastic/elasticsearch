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

package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.ToStringUtils;

/** Implements a simple bounding box query on a GeoPoint field. This is inspired by
 * {@link org.apache.lucene.search.NumericRangeQuery} and is implemented using a
 * two phase approach. First, candidate terms are queried using a numeric
 * range based on the morton codes of the min and max lat/lon pairs. Terms
 * passing this initial filter are passed to a final check that verifies whether
 * the decoded lat/lon falls within (or on the boundary) of the query bounding box.
 * The value comparisons are subject to a precision tolerance defined in
 * {@value org.apache.lucene.util.XGeoUtils#TOLERANCE}
 *
 * NOTES:
 *    1.  All latitude/longitude values must be in decimal degrees.
 *    2.  Complex computational geometry (e.g., dateline wrapping) is not supported
 *    3.  For more advanced GeoSpatial indexing and query operations see spatial module
 *    4.  This is well suited for small rectangles, large bounding boxes may result
 *        in many terms, depending whether the bounding box falls on the boundary of
 *        many cells (degenerate case)
 *
 * @lucene.experimental
 */
public class XGeoPointInBBoxQuery extends Query {
  protected final String field;
  protected final double minLon;
  protected final double minLat;
  protected final double maxLon;
  protected final double maxLat;

  public XGeoPointInBBoxQuery(final String field, final double minLon, final double minLat, final double maxLon, final double maxLat) {
    this.field = field;
    this.minLon = minLon;
    this.minLat = minLat;
    this.maxLon = maxLon;
    this.maxLat = maxLat;
  }

  @Override
  public Query rewrite(IndexReader reader) {
    if (maxLon < minLon) {
      BooleanQuery q = new BooleanQuery(true);

      XGeoPointInBBoxQueryImpl left = new XGeoPointInBBoxQueryImpl(field, -180.0D, minLat, maxLon, maxLat);
      left.setBoost(getBoost());
      q.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));
      XGeoPointInBBoxQueryImpl right = new XGeoPointInBBoxQueryImpl(field, minLon, minLat, 180.0D, maxLat);
      right.setBoost(getBoost());
      q.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));
      return q;
    }
    return new XGeoPointInBBoxQueryImpl(field, minLon, minLat, maxLon, maxLat);
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (!this.field.equals(field)) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    return sb.append(" Lower Left: [")
        .append(minLon)
        .append(',')
        .append(minLat)
        .append(']')
        .append(" Upper Right: [")
        .append(maxLon)
        .append(',')
        .append(maxLat)
        .append("]")
        .append(ToStringUtils.boost(getBoost()))
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof XGeoPointInBBoxQuery)) return false;
    if (!super.equals(o)) return false;

    XGeoPointInBBoxQuery that = (XGeoPointInBBoxQuery) o;

    if (Double.compare(that.maxLat, maxLat) != 0) return false;
    if (Double.compare(that.maxLon, maxLon) != 0) return false;
    if (Double.compare(that.minLat, minLat) != 0) return false;
    if (Double.compare(that.minLon, minLon) != 0) return false;
    if (!field.equals(that.field)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    long temp;
    result = 31 * result + field.hashCode();
    temp = Double.doubleToLongBits(minLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(minLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLon);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(maxLat);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  public final String getField() {
    return this.field;
  }

  public final double getMinLon() {
    return this.minLon;
  }

  public final double getMinLat() {
    return this.minLat;
  }

  public final double getMaxLon() {
    return this.maxLon;
  }

  public final double getMaxLat() {
    return this.maxLat;
  }
}
