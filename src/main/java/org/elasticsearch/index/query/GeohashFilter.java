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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

import java.io.IOException;
import java.util.List;

/**
 * A gehash filter filters {@link GeoPoint}s by their geohashes. Basically the a
 * Geohash prefix is defined by the filter and all geohashes that are matching this
 * prefix will be returned. The <code>neighbors</code> flag allows to filter
 * geohashes that surround the given geohash. In general the neighborhood of a
 * geohash is defined by its eight adjacent cells.<br />
 * The structure of the {@link GeohashFilter} is defined as:
 * <pre>
 * &quot;geohash_bbox&quot; {
 *     &quot;field&quot;:&quot;location&quot;,
 *     &quot;geohash&quot;:&quot;u33d8u5dkx8k&quot;,
 *     &quot;neighbors&quot;:false
 * }
 * </pre>
 */
public class GeohashFilter {

    public static final String NAME = "geohash_cell";
    public static final String NEIGHBORS = "neighbors";
    public static final String PRECISION = "precision";

    /**
     * Create a new geohash filter for a given set of geohashes. In general this method
     * returns a boolean filter combining the geohashes OR-wise.
     *
     * @param context     Context of the filter
     * @param fieldMapper field mapper for geopoints
     * @param geohash     mandatory geohash
     * @param geohashes   optional array of additional geohashes
     * @return a new GeoBoundinboxfilter
     */
    public static Filter create(QueryParseContext context, GeoPointFieldMapper fieldMapper, String geohash, @Nullable List<String> geohashes) {
        if (fieldMapper.geoHashStringMapper() == null) {
            throw new ElasticSearchIllegalArgumentException("geohash filter needs geohash_prefix to be enabled");
        }

        StringFieldMapper geoHashMapper = fieldMapper.geoHashStringMapper();
        if (geohashes == null || geohashes.size() == 0) {
            return geoHashMapper.termFilter(geohash, context);
        } else {
            geohashes.add(geohash);
            return geoHashMapper.termsFilter(geohashes, context);
        }
    }

    /**
     * Builder for a geohashfilter. It needs the fields <code>fieldname</code> and
     * <code>geohash</code> to be set. the default for a neighbor filteing is
     * <code>false</code>.
     */
    public static class Builder extends BaseFilterBuilder {
        // we need to store the geohash rather than the corresponding point,
        // because a transformation from a geohash to a point an back to the
        // geohash will extend the accuracy of the hash to max precision
        // i.e. by filing up with z's.
        private String fieldname;
        private String geohash;
        private int levels = -1;
        private boolean neighbors;

        public Builder(String fieldname) {
            this(fieldname, null, false);
        }

        public Builder(String fieldname, GeoPoint point) {
            this(fieldname, point.geohash(), false);
        }

        public Builder(String fieldname, String geohash) {
            this(fieldname, geohash, false);
        }

        public Builder(String fieldname, String geohash, boolean neighbors) {
            super();
            this.fieldname = fieldname;
            this.geohash = geohash;
            this.neighbors = neighbors;
        }

        public Builder setPoint(GeoPoint point) {
            this.geohash = point.getGeohash();
            return this;
        }

        public Builder setPoint(double lat, double lon) {
            this.geohash = GeoHashUtils.encode(lat, lon);
            return this;
        }

        public Builder setGeohash(String geohash) {
            this.geohash = geohash;
            return this;
        }

        public Builder setPrecision(int levels) {
            this.levels = levels;
            return this;
        }

        public Builder setPrecision(String precision) {
            double meters = DistanceUnit.parse(precision, DistanceUnit.METERS, DistanceUnit.METERS);
            return setPrecision(GeoUtils.geoHashLevelsForPrecision(meters));
        }
        
        public Builder setNeighbors(boolean neighbors) {
            this.neighbors = neighbors;
            return this;
        }

        public Builder setField(String fieldname) {
            this.fieldname = fieldname;
            return this;
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            if (neighbors) {
                builder.field(NEIGHBORS, neighbors);
            }
            if(levels > 0) {
                builder.field(PRECISION, levels);
            }
            builder.field(fieldname, geohash);

            builder.endObject();
        }
    }

    public static class Parser implements FilterParser {

        @Inject
        public Parser() {
        }

        @Override
        public String[] names() {
            return new String[]{NAME, Strings.toCamelCase(NAME)};
        }

        @Override
        public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
            XContentParser parser = parseContext.parser();

            String fieldName = null;
            String geohash = null;
            int levels = -1;
            boolean neighbors = false;

            XContentParser.Token token;
            if ((token = parser.currentToken()) != Token.START_OBJECT) {
                throw new ElasticSearchParseException(NAME + " must be an object");
            }

            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    String field = parser.text();

                    if (PRECISION.equals(field)) {
                        token = parser.nextToken();
                        if(token == Token.VALUE_NUMBER) {
                            levels = parser.intValue();
                        } else if(token == Token.VALUE_STRING) {
                            double meters = DistanceUnit.parse(parser.text(), DistanceUnit.METERS, DistanceUnit.METERS);
                            levels = GeoUtils.geoHashLevelsForPrecision(meters);
                        }
                    } else if (NEIGHBORS.equals(field)) {
                        parser.nextToken();
                        neighbors = parser.booleanValue();
                    } else {
                        fieldName = field;
                        token = parser.nextToken();
                        if(token == Token.VALUE_STRING) {
                            // A string indicates either a gehash or a lat/lon string
                            String location = parser.text();
                            if(location.indexOf(",")>0) {
                                geohash = GeoPoint.parse(parser).geohash();
                            } else {
                                geohash = location;
                            }
                        } else {
                            geohash = GeoPoint.parse(parser).geohash();
                        }
                    }
                } else {
                    throw new ElasticSearchParseException("unexpected token [" + token + "]");
                }
            }

            MapperService.SmartNameFieldMappers smartMappers = parseContext.smartFieldMappers(fieldName);
            if (smartMappers == null || !smartMappers.hasMapper()) {
                throw new QueryParsingException(parseContext.index(), "failed to find geo_point field [" + fieldName + "]");
            }

            FieldMapper<?> mapper = smartMappers.mapper();
            if (!(mapper instanceof GeoPointFieldMapper.GeoStringFieldMapper)) {
                throw new QueryParsingException(parseContext.index(), "field [" + fieldName + "] is not a geo_point field");
            }

            GeoPointFieldMapper geoMapper = ((GeoPointFieldMapper.GeoStringFieldMapper) mapper).geoMapper();

            if(levels > 0) {
                int len = Math.min(levels, geohash.length());
                geohash = geohash.substring(0, len);
            }

            if (neighbors) {
                return create(parseContext, geoMapper, geohash, GeoHashUtils.neighbors(geohash));
            } else {
                return create(parseContext, geoMapper, geohash, null);
            }
        }
    }
}
