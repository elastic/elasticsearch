/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.geo;

import java.io.IOException;

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

/**
 *
 */
public class GeoPoint {

    public static final String LATITUDE = GeoPointFieldMapper.Names.LAT;
    public static final String LONGITUDE = GeoPointFieldMapper.Names.LON;
    
    private double lat;
    private double lon;

    public GeoPoint() {
    }

    public GeoPoint(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public GeoPoint reset(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
        return this;
    }

    public GeoPoint resetLat(double lat) {
        this.lat = lat;
        return this;
    }

    public GeoPoint resetLon(double lon) {
        this.lon = lon;
        return this;
    }

    public GeoPoint resetFromString(String value) {
        int comma = value.indexOf(',');
        if (comma != -1) {
            lat = Double.parseDouble(value.substring(0, comma).trim());
            lon = Double.parseDouble(value.substring(comma + 1).trim());
        } else {
            resetFromGeoHash(value);
        }
        return this;
    }

    public GeoPoint resetFromGeoHash(String hash) {
        GeoHashUtils.decode(hash, this);
        return this;
    }

    void latlon(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public final double lat() {
        return this.lat;
    }

    public final double getLat() {
        return this.lat;
    }

    public final double lon() {
        return this.lon;
    }

    public final double getLon() {
        return this.lon;
    }

    public final String geohash() {
        return GeoHashUtils.encode(lat, lon);
    }

    public final String getGeohash() {
        return GeoHashUtils.encode(lat, lon);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoPoint geoPoint = (GeoPoint) o;

        if (Double.compare(geoPoint.lat, lat) != 0) return false;
        if (Double.compare(geoPoint.lon, lon) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = lat != +0.0d ? Double.doubleToLongBits(lat) : 0L;
        result = (int) (temp ^ (temp >>> 32));
        temp = lon != +0.0d ? Double.doubleToLongBits(lon) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public String toString() {
        return "[" + lat + ", " + lon + "]";
    }
    
    /**
     * Parse a {@link GeoPoint} with a {@link XContentParser}:
     * 
     * @param parser {@link XContentParser} to parse the value from
     * @return new {@link GeoPoint} parsed from the parse
     * 
     * @throws IOException
     * @throws ElasticSearchParseException
     */
    public static GeoPoint parse(XContentParser parser) throws IOException, ElasticSearchParseException {
        return parse(parser, new GeoPoint());
    }

    /**
     * Parse a {@link GeoPoint} with a {@link XContentParser}. A geopoint has one of the following forms:
     * 
     * <ul>
     *     <li>Object: <pre>{&quot;lat&quot;: <i>&lt;latitude&gt;</i>, &quot;lon&quot;: <i>&lt;longitude&gt;</i>}</pre></li>
     *     <li>String: <pre>&quot;<i>&lt;latitude&gt;</i>,<i>&lt;longitude&gt;</i>&quot;</pre></li>
     *     <li>Geohash: <pre>&quot;<i>&lt;geohash&gt;</i>&quot;</pre></li>
     *     <li>Array: <pre>[<i>&lt;longitude&gt;</i>,<i>&lt;latitude&gt;</i>]</pre></li>
     * </ul>
     * 
     * @param parser {@link XContentParser} to parse the value from
     * @param point A {@link GeoPoint} that will be reset by the values parsed
     * @return new {@link GeoPoint} parsed from the parse
     * 
     * @throws IOException
     * @throws ElasticSearchParseException
     */
    public static GeoPoint parse(XContentParser parser, GeoPoint point) throws IOException, ElasticSearchParseException {
        if(parser.currentToken() == Token.START_OBJECT) {
            while(parser.nextToken() != Token.END_OBJECT) {
                if(parser.currentToken() == Token.FIELD_NAME) {
                    String field = parser.text();
                    if(LATITUDE.equals(field)) {
                        if(parser.nextToken() == Token.VALUE_NUMBER) {
                            point.resetLat(parser.doubleValue());
                        } else {
                            throw new ElasticSearchParseException("latitude must be a number");
                        }
                    } else if (LONGITUDE.equals(field)) {
                        if(parser.nextToken() == Token.VALUE_NUMBER) {
                            point.resetLon(parser.doubleValue());
                        } else {
                            throw new ElasticSearchParseException("latitude must be a number");
                        }
                    } else {
                        throw new ElasticSearchParseException("field must be either '"+LATITUDE+"' or '"+LONGITUDE+"'");
                    }
                } else {
                    throw new ElasticSearchParseException("Token '"+parser.currentToken()+"' not allowed");
                }
            }
            return point;
        } else if(parser.currentToken() == Token.START_ARRAY) {
            int element = 0;
            while(parser.nextToken() != Token.END_ARRAY) {
                if(parser.currentToken() == Token.VALUE_NUMBER) {
                    element++;
                    if(element == 1) {
                        point.resetLon(parser.doubleValue());
                    } else if(element == 2) {
                        point.resetLat(parser.doubleValue());
                    } else {
                        throw new ElasticSearchParseException("only two values allowed");
                    }
                } else {
                    throw new ElasticSearchParseException("Numeric value expected");
                }
            }
            return point;
        } else if(parser.currentToken() == Token.VALUE_STRING) {
            String data = parser.text();
            int comma = data.indexOf(',');
            if(comma > 0) {
                double lat = Double.parseDouble(data.substring(0, comma).trim());
                double lon = Double.parseDouble(data.substring(comma+1).trim());
                return point.reset(lat, lon);
            } else {
                point.resetFromGeoHash(data);
                return point;
            }
        } else {
            throw new ElasticSearchParseException("geo_point expected");
        }
    }
}
