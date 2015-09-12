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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.XGeoHashUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A geohash cell filter that filters {@link GeoPoint}s by their geohashes. Basically the a
 * Geohash prefix is defined by the filter and all geohashes that are matching this
 * prefix will be returned. The <code>neighbors</code> flag allows to filter
 * geohashes that surround the given geohash. In general the neighborhood of a
 * geohash is defined by its eight adjacent cells.<br />
 * The structure of the {@link GeohashCellQuery} is defined as:
 * <pre>
 * &quot;geohash_bbox&quot; {
 *     &quot;field&quot;:&quot;location&quot;,
 *     &quot;geohash&quot;:&quot;u33d8u5dkx8k&quot;,
 *     &quot;neighbors&quot;:false
 * }
 * </pre>
 */
public class GeohashCellQuery {

    public static final String NAME = "geohash_cell";
    public static final String NEIGHBORS = "neighbors";
    public static final String PRECISION = "precision";

    /**
     * Create a new geohash filter for a given set of geohashes. In general this method
     * returns a boolean filter combining the geohashes OR-wise.
     *
     * @param context     Context of the filter
     * @param fieldType field mapper for geopoints
     * @param geohash     mandatory geohash
     * @param geohashes   optional array of additional geohashes
     * @return a new GeoBoundinboxfilter
     */
    public static Query create(QueryParseContext context, GeoPointFieldMapper.GeoPointFieldType fieldType, String geohash, @Nullable List<CharSequence> geohashes) {
        MappedFieldType geoHashMapper = fieldType.geohashFieldType();
        if (geoHashMapper == null) {
            throw new IllegalArgumentException("geohash filter needs geohash_prefix to be enabled");
        }

        if (geohashes == null || geohashes.size() == 0) {
            return geoHashMapper.termQuery(geohash, context);
        } else {
            geohashes.add(geohash);
            return geoHashMapper.termsQuery(geohashes, context);
        }
    }

    /**
     * Builder for a geohashfilter. It needs the fields <code>fieldname</code> and
     * <code>geohash</code> to be set. the default for a neighbor filteing is
     * <code>false</code>.
     */
    public static class Builder extends QueryBuilder {
        // we need to store the geohash rather than the corresponding point,
        // because a transformation from a geohash to a point an back to the
        // geohash will extend the accuracy of the hash to max precision
        // i.e. by filing up with z's.
        private String field;
        private String geohash;
        private int levels = -1;
        private boolean neighbors;


        public Builder(String field) {
            this(field, null, false);
        }

        public Builder(String field, GeoPoint point) {
            this(field, point.geohash(), false);
        }

        public Builder(String field, String geohash) {
            this(field, geohash, false);
        }

        public Builder(String field, String geohash, boolean neighbors) {
            super();
            this.field = field;
            this.geohash = geohash;
            this.neighbors = neighbors;
        }

        public Builder point(GeoPoint point) {
            this.geohash = point.getGeohash();
            return this;
        }

        public Builder point(double lat, double lon) {
            this.geohash = XGeoHashUtils.stringEncode(lon, lat);
            return this;
        }

        public Builder geohash(String geohash) {
            this.geohash = geohash;
            return this;
        }

        public Builder precision(int levels) {
            this.levels = levels;
            return this;
        }

        public Builder precision(String precision) {
            double meters = DistanceUnit.parse(precision, DistanceUnit.DEFAULT, DistanceUnit.METERS);
            return precision(GeoUtils.geoHashLevelsForPrecision(meters));
        }

        public Builder neighbors(boolean neighbors) {
            this.neighbors = neighbors;
            return this;
        }

        public Builder field(String field) {
            this.field = field;
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
            builder.field(field, geohash);

            builder.endObject();
        }
    }

    public static class Parser implements QueryParser {

        @Inject
        public Parser() {
        }

        @Override
        public String[] names() {
            return new String[]{NAME, Strings.toCamelCase(NAME)};
        }

        @Override
        public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
            XContentParser parser = parseContext.parser();

            String fieldName = null;
            String geohash = null;
            int levels = -1;
            boolean neighbors = false;


            XContentParser.Token token;
            if ((token = parser.currentToken()) != Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse [{}] query. expected an object but found [{}] instead", NAME, token);
            }

            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    String field = parser.text();

                    if (parseContext.isDeprecatedSetting(field)) {
                        // skip
                    } else if (PRECISION.equals(field)) {
                        token = parser.nextToken();
                        if(token == Token.VALUE_NUMBER) {
                            levels = parser.intValue();
                        } else if(token == Token.VALUE_STRING) {
                            double meters = DistanceUnit.parse(parser.text(), DistanceUnit.DEFAULT, DistanceUnit.METERS);
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
                                geohash = GeoUtils.parseGeoPoint(parser).geohash();
                            } else {
                                geohash = location;
                            }
                        } else {
                            geohash = GeoUtils.parseGeoPoint(parser).geohash();
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse [{}] query. unexpected token [{}]", NAME, token);
                }
            }

            if (geohash == null) {
                throw new QueryParsingException(parseContext, "failed to parse [{}] query. missing geohash value", NAME);
            }

            MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
            if (fieldType == null) {
                throw new QueryParsingException(parseContext, "failed to parse [{}] query. missing [{}] field [{}]", NAME, GeoPointFieldMapper.CONTENT_TYPE, fieldName);
            }

            if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
                throw new QueryParsingException(parseContext, "failed to parse [{}] query. field [{}] is not a geo_point field", NAME, fieldName);
            }

            GeoPointFieldMapper.GeoPointFieldType geoFieldType = ((GeoPointFieldMapper.GeoPointFieldType) fieldType);
            if (!geoFieldType.isGeohashPrefixEnabled()) {
                throw new QueryParsingException(parseContext, "failed to parse [{}] query. [geohash_prefix] is not enabled for field [{}]", NAME, fieldName);
            }

            if(levels > 0) {
                int len = Math.min(levels, geohash.length());
                geohash = geohash.substring(0, len);
            }

            Query filter;
            if (neighbors) {
                filter = create(parseContext, geoFieldType, geohash, XGeoHashUtils.addNeighbors(geohash, new ArrayList<>(8)));
            } else {
                filter = create(parseContext, geoFieldType, geohash, null);
            }

            return filter;
        }
    }
}
