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

package org.apache.lucene.document;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.XGeoUtils;

/**
 * <p>
 * Field that indexes <code>latitude</code> <code>longitude</code> decimal-degree values
 * for efficient encoding, sorting, and querying. This Geo capability is intended
 * to provide a basic and efficient out of the box field type for indexing and
 * querying 2 dimensional points in WGS-84 decimal degrees. An example usage is as follows:
 *
 * <pre class="prettyprint">
 *  document.add(new GeoPointField(name, -96.33, 32.66, Field.Store.NO));
 * </pre>
 *
 * <p>To perform simple geospatial queries against a <code>GeoPointField</code>,
 * see {@link org.apache.lucene.search.XGeoPointInBBoxQuery}, {@link org.apache.lucene.search.XGeoPointInPolygonQuery},
 * or {@link org.apache.lucene.search.XGeoPointDistanceQuery}
 *
 * NOTE: This indexes only high precision encoded terms which may result in visiting a high number
 * of terms for large queries. See LUCENE-6481 for a future improvement.
 *
 * @lucene.experimental
 */
public final class XGeoPointField extends Field {
  public static final int PRECISION_STEP = 9;

  /**
   * Type for an GeoPointField that is not stored:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_NOT_STORED = new FieldType();
  static {
    TYPE_NOT_STORED.setTokenized(false);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_NOT_STORED.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE_NOT_STORED.setNumericType(FieldType.NumericType.LONG);
    TYPE_NOT_STORED.setNumericPrecisionStep(PRECISION_STEP);
    TYPE_NOT_STORED.freeze();
  }

  /**
   * Type for a stored GeoPointField:
   * normalization factors, frequencies, and positions are omitted.
   */
  public static final FieldType TYPE_STORED = new FieldType();
  static {
    TYPE_STORED.setTokenized(false);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_STORED.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE_STORED.setNumericType(FieldType.NumericType.LONG);
    TYPE_STORED.setNumericPrecisionStep(PRECISION_STEP);
    TYPE_STORED.setStored(true);
    TYPE_STORED.freeze();
  }

  /** Creates a stored or un-stored GeoPointField with the provided value
   *  and default <code>precisionStep</code> set to 64 to avoid wasteful
   *  indexing of lower precision terms.
   *  @param name field name
   *  @param lon longitude double value [-180.0 : 180.0]
   *  @param lat latitude double value [-90.0 : 90.0]
   *  @param stored Store.YES if the content should also be stored
   *  @throws IllegalArgumentException if the field name is null.
   */
  public XGeoPointField(String name, double lon, double lat, Store stored) {
    super(name, stored == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
    fieldsData = XGeoUtils.mortonHash(lon, lat);
  }

  /** Expert: allows you to customize the {@link
   *  FieldType}.
   *  @param name field name
   *  @param lon longitude double value [-180.0 : 180.0]
   *  @param lat latitude double value [-90.0 : 90.0]
   *  @param type customized field type: must have {@link FieldType#numericType()}
   *         of {@link FieldType.NumericType#LONG}.
   *  @throws IllegalArgumentException if the field name or type is null, or
   *          if the field type does not have a LONG numericType()
   */
  public XGeoPointField(String name, double lon, double lat, FieldType type) {
    super(name, type);
    if (type.numericType() != FieldType.NumericType.LONG) {
      throw new IllegalArgumentException("type.numericType() must be LONG but got " + type.numericType());
    }
    fieldsData = XGeoUtils.mortonHash(lon, lat);
  }

  public double getLon() {
    return XGeoUtils.mortonUnhashLon((long) fieldsData);
  }

  public double getLat() {
    return XGeoUtils.mortonUnhashLat((long) fieldsData);
  }

  @Override
  public String toString() {
    if (fieldsData == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    sb.append(XGeoUtils.mortonUnhashLon((long) fieldsData));
    sb.append(',');
    sb.append(XGeoUtils.mortonUnhashLat((long) fieldsData));
    return sb.toString();
  }
}
