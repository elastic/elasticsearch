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
import org.apache.lucene.util.GeoHashUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.BaseGeoPointFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A geohash cell filter that filters {@link GeoPoint}s by their geohashes. Basically the a
 * Geohash prefix is defined by the filter and all geohashes that are matching this
 * prefix will be returned. The <code>neighbors</code> flag allows to filter
 * geohashes that surround the given geohash. In general the neighborhood of a
 * geohash is defined by its eight adjacent cells.<br>
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
    public static final ParseField NEIGHBORS_FIELD = new ParseField("neighbors");
    public static final ParseField PRECISION_FIELD = new ParseField("precision");
    public static final boolean DEFAULT_NEIGHBORS = false;

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
    public static Query create(QueryShardContext context, BaseGeoPointFieldMapper.GeoPointFieldType fieldType, String geohash, @Nullable List<CharSequence> geohashes) {
        MappedFieldType geoHashMapper = fieldType.geoHashFieldType();
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
    public static class Builder extends AbstractQueryBuilder<Builder> {
        // we need to store the geohash rather than the corresponding point,
        // because a transformation from a geohash to a point an back to the
        // geohash will extend the accuracy of the hash to max precision
        // i.e. by filing up with z's.
        private String fieldName;
        private String geohash;
        private Integer levels = null;
        private boolean neighbors = DEFAULT_NEIGHBORS;
        private static final Builder PROTOTYPE = new Builder("field", new GeoPoint());


        public Builder(String field, GeoPoint point) {
            this(field, point == null ? null : point.geohash(), false);
        }

        public Builder(String field, String geohash) {
            this(field, geohash, false);
        }

        public Builder(String field, String geohash, boolean neighbors) {
            if (Strings.isEmpty(field)) {
                throw new IllegalArgumentException("fieldName must not be null");
            }
            if (Strings.isEmpty(geohash)) {
                throw new IllegalArgumentException("geohash or point must be defined");
            }
            this.fieldName = field;
            this.geohash = geohash;
            this.neighbors = neighbors;
        }

        public Builder point(GeoPoint point) {
            this.geohash = point.getGeohash();
            return this;
        }

        public Builder point(double lat, double lon) {
            this.geohash = GeoHashUtils.stringEncode(lon, lat);
            return this;
        }

        public Builder geohash(String geohash) {
            this.geohash = geohash;
            return this;
        }

        public String geohash() {
            return geohash;
        }

        public Builder precision(int levels) {
            if (levels <= 0) {
                throw new IllegalArgumentException("precision must be greater than 0. Found [" + levels + "]");
            }
            this.levels = levels;
            return this;
        }

        public Integer precision() {
            return levels;
        }

        public Builder precision(String precision) {
            double meters = DistanceUnit.parse(precision, DistanceUnit.DEFAULT, DistanceUnit.METERS);
            return precision(GeoUtils.geoHashLevelsForPrecision(meters));
        }

        public Builder neighbors(boolean neighbors) {
            this.neighbors = neighbors;
            return this;
        }

        public boolean neighbors() {
            return neighbors;
        }

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public String fieldName() {
            return fieldName;
        }

        @Override
        protected Query doToQuery(QueryShardContext context) throws IOException {
            MappedFieldType fieldType = context.fieldMapper(fieldName);
            if (fieldType == null) {
                throw new QueryShardException(context, "failed to parse [{}] query. missing [{}] field [{}]", NAME,
                        BaseGeoPointFieldMapper.CONTENT_TYPE, fieldName);
            }

            if (!(fieldType instanceof BaseGeoPointFieldMapper.GeoPointFieldType)) {
                throw new QueryShardException(context, "failed to parse [{}] query. field [{}] is not a geo_point field", NAME, fieldName);
            }

            BaseGeoPointFieldMapper.GeoPointFieldType geoFieldType = ((BaseGeoPointFieldMapper.GeoPointFieldType) fieldType);
            if (!geoFieldType.isGeoHashPrefixEnabled()) {
                throw new QueryShardException(context, "failed to parse [{}] query. [geohash_prefix] is not enabled for field [{}]", NAME,
                        fieldName);
            }

            String geohash = this.geohash;
            if (levels != null) {
                int len = Math.min(levels, geohash.length());
                geohash = geohash.substring(0, len);
            }

            Query query;
            if (neighbors) {
                query = create(context, geoFieldType, geohash, GeoHashUtils.addNeighbors(geohash, new ArrayList<CharSequence>(8)));
            } else {
                query = create(context, geoFieldType, geohash, null);
            }
            return query;
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field(NEIGHBORS_FIELD.getPreferredName(), neighbors);
            if (levels != null) {
                builder.field(PRECISION_FIELD.getPreferredName(), levels);
            }
            builder.field(fieldName, geohash);
            printBoostAndQueryName(builder);
            builder.endObject();
        }

        @Override
        protected Builder doReadFrom(StreamInput in) throws IOException {
            String field = in.readString();
            String geohash = in.readString();
            Builder builder = new Builder(field, geohash);
            if (in.readBoolean()) {
                builder.precision(in.readVInt());
            }
            builder.neighbors(in.readBoolean());
            return builder;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            out.writeString(geohash);
            boolean hasLevels = levels != null;
            out.writeBoolean(hasLevels);
            if (hasLevels) {
                out.writeVInt(levels);
            }
            out.writeBoolean(neighbors);
        }

        @Override
        protected boolean doEquals(Builder other) {
            return Objects.equals(fieldName, other.fieldName)
                    && Objects.equals(geohash, other.geohash)
                    && Objects.equals(levels, other.levels)
                    && Objects.equals(neighbors, other.neighbors);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(fieldName, geohash, levels, neighbors);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public static class Parser implements QueryParser<Builder> {

        @Inject
        public Parser() {
        }

        @Override
        public String[] names() {
            return new String[]{NAME, Strings.toCamelCase(NAME)};
        }

        @Override
        public Builder fromXContent(QueryParseContext parseContext) throws IOException {
            XContentParser parser = parseContext.parser();

            String fieldName = null;
            String geohash = null;
            Integer levels = null;
            Boolean neighbors = null;
            String queryName = null;
            Float boost = null;

            XContentParser.Token token;
            if ((token = parser.currentToken()) != Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse [{}] query. expected an object but found [{}] instead", NAME, token);
            }

            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    String field = parser.currentName();

                    if (parseContext.isDeprecatedSetting(field)) {
                        // skip
                    } else if (parseContext.parseFieldMatcher().match(field, PRECISION_FIELD)) {
                        token = parser.nextToken();
                        if (token == Token.VALUE_NUMBER) {
                            levels = parser.intValue();
                        } else if (token == Token.VALUE_STRING) {
                            double meters = DistanceUnit.parse(parser.text(), DistanceUnit.DEFAULT, DistanceUnit.METERS);
                            levels = GeoUtils.geoHashLevelsForPrecision(meters);
                        }
                    } else if (parseContext.parseFieldMatcher().match(field, NEIGHBORS_FIELD)) {
                        parser.nextToken();
                        neighbors = parser.booleanValue();
                    } else if (parseContext.parseFieldMatcher().match(field, AbstractQueryBuilder.NAME_FIELD)) {
                        parser.nextToken();
                        queryName = parser.text();
                    } else if (parseContext.parseFieldMatcher().match(field, AbstractQueryBuilder.BOOST_FIELD)) {
                        parser.nextToken();
                        boost = parser.floatValue();
                    } else {
                        if (fieldName == null) {
                            fieldName = field;
                            token = parser.nextToken();
                            if (token == Token.VALUE_STRING) {
                                // A string indicates either a geohash or a
                                // lat/lon
                                // string
                                String location = parser.text();
                                if (location.indexOf(",") > 0) {
                                    geohash = GeoUtils.parseGeoPoint(parser).geohash();
                                } else {
                                    geohash = location;
                                }
                            } else {
                                geohash = GeoUtils.parseGeoPoint(parser).geohash();
                            }
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[" + NAME +
                                    "] field name already set to [" + fieldName + "] but found [" + field + "]");
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse [{}] query. unexpected token [{}]", NAME, token);
                }
            }
            Builder builder = new Builder(fieldName, geohash);
            if (levels != null) {
                builder.precision(levels);
            }
            if (neighbors != null) {
                builder.neighbors(neighbors);
            }
            if (queryName != null) {
                builder.queryName(queryName);
            }
            if (boost != null) {
                builder.boost(boost);
            }
            return builder;
        }

        @Override
        public GeohashCellQuery.Builder getBuilderPrototype() {
            return Builder.PROTOTYPE;
        }
    }
}
