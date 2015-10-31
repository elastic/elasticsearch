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

package org.elasticsearch.search.suggest.completion.context;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.GeoHashUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

import java.io.IOException;
import java.util.*;

/**
 * A {@link ContextMapping} that uses a geo location/area as a
 * criteria.
 * The suggestions can be boosted and/or filtered depending on
 * whether it falls within an area, represented by a query geo hash
 * with a specified precision
 *
 * {@link GeoQueryContext} defines the options for constructing
 * a unit of query context for this context type
 */
public class GeoContextMapping extends ContextMapping {

    public static final String FIELD_PRECISION = "precision";
    public static final String FIELD_FIELDNAME = "path";

    public static final int DEFAULT_PRECISION = 6;

    static final String CONTEXT_VALUE = "context";
    static final String CONTEXT_BOOST = "boost";
    static final String CONTEXT_PRECISION = "precision";
    static final String CONTEXT_NEIGHBOURS = "neighbours";

    private final int precision;
    private final String fieldName;

    private GeoContextMapping(String name, String fieldName, int precision) {
        super(Type.GEO, name);
        this.precision = precision;
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public int getPrecision() {
        return precision;
    }

    protected static GeoContextMapping load(String name, Map<String, Object> config) {
        final GeoContextMapping.Builder builder = new GeoContextMapping.Builder(name);

        if (config != null) {
            final Object configPrecision = config.get(FIELD_PRECISION);
            if (configPrecision != null) {
                if (configPrecision instanceof Integer) {
                    builder.precision((Integer) configPrecision);
                } else if (configPrecision instanceof Long) {
                    builder.precision((Long) configPrecision);
                } else if (configPrecision instanceof Double) {
                    builder.precision((Double) configPrecision);
                } else if (configPrecision instanceof Float) {
                    builder.precision((Float) configPrecision);
                } else {
                    builder.precision(configPrecision.toString());
                }
                config.remove(FIELD_PRECISION);
            }

            final Object fieldName = config.get(FIELD_FIELDNAME);
            if (fieldName != null) {
                builder.field(fieldName.toString());
                config.remove(FIELD_FIELDNAME);
            }
        }
        return builder.build();
    }

    @Override
    protected XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_PRECISION, precision);
        if (fieldName != null) {
            builder.field(FIELD_FIELDNAME, fieldName);
        }
        return builder;
    }

    /**
     * Parse a set of {@link CharSequence} contexts at index-time.
     * Acceptable formats:
     *
     *  <ul>
     *     <li>Array: <pre>[<i>&lt;GEO POINT&gt;</i>, ..]</pre></li>
     *     <li>String/Object/Array: <pre>&quot;GEO POINT&quot;</pre></li>
     *  </ul>
     *
     * see {@link GeoUtils#parseGeoPoint(String, GeoPoint)} for GEO POINT
     */
    @Override
    public Set<CharSequence> parseContext(ParseContext parseContext, XContentParser parser) throws IOException, ElasticsearchParseException {
        if (fieldName != null) {
            FieldMapper mapper = parseContext.docMapper().mappers().getMapper(fieldName);
            if (!(mapper instanceof GeoPointFieldMapper)) {
                throw new ElasticsearchParseException("referenced field must be mapped to geo_point");
            }
        }
        final Set<CharSequence> contexts = new HashSet<>();
        Token token = parser.currentToken();
        if (token == Token.START_ARRAY) {
            token = parser.nextToken();
            // Test if value is a single point in <code>[lon, lat]</code> format
            if (token == Token.VALUE_NUMBER) {
                double lon = parser.doubleValue();
                if (parser.nextToken() == Token.VALUE_NUMBER) {
                    double lat = parser.doubleValue();
                    if (parser.nextToken() == Token.END_ARRAY) {
                        contexts.add(GeoHashUtils.stringEncode(lon, lat, precision));
                    } else {
                        throw new ElasticsearchParseException("only two values [lon, lat] expected");
                    }
                } else {
                    throw new ElasticsearchParseException("latitude must be a numeric value");
                }
            } else {
                while (token != Token.END_ARRAY) {
                    GeoPoint point = GeoUtils.parseGeoPoint(parser);
                    contexts.add(GeoHashUtils.stringEncode(point.getLon(), point.getLat(), precision));
                    token = parser.nextToken();
                }
            }
        } else if (token == Token.VALUE_STRING) {
            final String geoHash = parser.text();
            final CharSequence truncatedGeoHash = geoHash.subSequence(0, Math.min(geoHash.length(), precision));
            contexts.add(truncatedGeoHash);
        } else {
            // or a single location
            GeoPoint point = GeoUtils.parseGeoPoint(parser);
            contexts.add(GeoHashUtils.stringEncode(point.getLon(), point.getLat(), precision));
        }
        return contexts;
    }

    @Override
    public Set<CharSequence> parseContext(Document document) {
        final Set<CharSequence> geohashes = new HashSet<>();

        if (fieldName != null) {
            IndexableField[] fields = document.getFields(fieldName);
            GeoPoint spare = new GeoPoint();
            if (fields.length == 0) {
                IndexableField[] lonFields = document.getFields(fieldName + ".lon");
                IndexableField[] latFields = document.getFields(fieldName + ".lat");
                if (lonFields.length > 0 && latFields.length > 0) {
                    for (int i = 0; i < lonFields.length; i++) {
                        IndexableField lonField = lonFields[i];
                        IndexableField latField = latFields[i];
                        assert lonField.fieldType().docValuesType() == latField.fieldType().docValuesType();
                        // we write doc values fields differently: one field for all values, so we need to only care about indexed fields
                        if (lonField.fieldType().docValuesType() == DocValuesType.NONE) {
                            spare.reset(latField.numericValue().doubleValue(), lonField.numericValue().doubleValue());
                            geohashes.add(GeoHashUtils.stringEncode(spare.getLon(), spare.getLat(), precision));
                        }
                    }
                }
            } else {
                for (IndexableField field : fields) {
                    spare.resetFromString(field.stringValue());
                    geohashes.add(spare.geohash());
                }
            }
        }

        Set<CharSequence> locations = new HashSet<>();
        for (CharSequence geohash : geohashes) {
            int precision = Math.min(this.precision, geohash.length());
            CharSequence truncatedGeohash = geohash.subSequence(0, precision);
            locations.add(truncatedGeohash);
        }
        return locations;
    }

    /**
     * Parse a list of {@link GeoQueryContext}
     * using <code>parser</code>. A QueryContexts accepts one of the following forms:
     *
     * <ul>
     *     <li>Object: GeoQueryContext</li>
     *     <li>String: GeoQueryContext value with boost=1  precision=PRECISION neighbours=[PRECISION]</li>
     *     <li>Array: <pre>[GeoQueryContext, ..]</pre></li>
     * </ul>
     *
     *  A GeoQueryContext has one of the following forms:
     *  <ul>
     *     <li>Object:
     *     <ul>
     *         <li><pre>GEO POINT</pre></li>
     *         <li><pre>{&quot;lat&quot;: <i>&lt;double&gt;</i>, &quot;lon&quot;: <i>&lt;double&gt;</i>, &quot;precision&quot;: <i>&lt;int&gt;</i>, &quot;neighbours&quot;: <i>&lt;[int, ..]&gt;</i>}</pre></li>
     *         <li><pre>{&quot;context&quot;: <i>&lt;string&gt;</i>, &quot;boost&quot;: <i>&lt;int&gt;</i>, &quot;precision&quot;: <i>&lt;int&gt;</i>, &quot;neighbours&quot;: <i>&lt;[int, ..]&gt;</i>}</pre></li>
     *         <li><pre>{&quot;context&quot;: <i>&lt;GEO POINT&gt;</i>, &quot;boost&quot;: <i>&lt;int&gt;</i>, &quot;precision&quot;: <i>&lt;int&gt;</i>, &quot;neighbours&quot;: <i>&lt;[int, ..]&gt;</i>}</pre></li>
     *     </ul>
     *     <li>String: <pre>GEO POINT</pre></li>
     *  </ul>
     * see {@link GeoUtils#parseGeoPoint(String, GeoPoint)} for GEO POINT
     */
    @Override
    public List<CategoryQueryContext> parseQueryContext(XContentParser parser) throws IOException, ElasticsearchParseException {
        List<CategoryQueryContext> queryContexts = new ArrayList<>();
        Token token = parser.nextToken();
        if (token == Token.START_OBJECT || token == Token.VALUE_STRING) {
            queryContexts.add(innerParseQueryContext(parser));
        } else if (token == Token.START_ARRAY) {
            while (parser.nextToken() != Token.END_ARRAY) {
                queryContexts.add(innerParseQueryContext(parser));
            }
        }
        return queryContexts;
    }

    private GeoQueryContext innerParseQueryContext(XContentParser parser) throws IOException, ElasticsearchParseException {
        Token token = parser.currentToken();
        if (token == Token.VALUE_STRING) {
            return new GeoQueryContext(GeoUtils.parseGeoPoint(parser), 1, precision, precision);
        } else if (token == Token.START_OBJECT) {
            String currentFieldName = null;
            GeoPoint point = null;
            double lat = Double.NaN;
            double lon = Double.NaN;
            int precision = this.precision;
            List<Integer> neighbours = new ArrayList<>();
            int boost = 1;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (currentFieldName != null) {
                    if ("lat".equals(currentFieldName)) {
                        if (token == Token.VALUE_STRING || token == Token.VALUE_NUMBER) {
                            if (point == null) {
                                lat = parser.doubleValue(true);
                            } else {
                                throw new ElasticsearchParseException("context must have either lat/lon or geohash");
                            }
                        } else {
                            throw new ElasticsearchParseException("lat must be a number");
                        }
                    } else if ("lon".equals(currentFieldName)) {
                        if (token == Token.VALUE_STRING || token == Token.VALUE_NUMBER) {
                            if (point == null) {
                                lon = parser.doubleValue(true);
                            } else {
                                throw new ElasticsearchParseException("context must have either lat/lon or geohash");
                            }
                        } else {
                            throw new ElasticsearchParseException("lon must be a number");
                        }
                    } else if (CONTEXT_VALUE.equals(currentFieldName)) {
                        point = GeoUtils.parseGeoPoint(parser);
                    } else if (CONTEXT_BOOST.equals(currentFieldName)) {
                        final Number number;
                        if (token == Token.VALUE_STRING) {
                            try {
                                number = Long.parseLong(parser.text());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException("boost must be a string representing a numeric value, but was [" + parser.text() + "]");
                            }
                        } else if (token == Token.VALUE_NUMBER) {
                            XContentParser.NumberType numberType = parser.numberType();
                            number = parser.numberValue();
                            if (numberType != XContentParser.NumberType.INT) {
                                throw new ElasticsearchParseException("boost must be in the interval [0..2147483647], but was [" + number.longValue() + "]");
                            }
                        } else {
                            throw new ElasticsearchParseException("boost must be an int");
                        }
                        boost = number.intValue();
                    } else if (CONTEXT_NEIGHBOURS.equals(currentFieldName)) {
                        if (token == Token.VALUE_STRING) {
                            neighbours.add(GeoUtils.geoHashLevelsForPrecision(parser.text()));
                        } else if (token == Token.VALUE_NUMBER) {
                            XContentParser.NumberType numberType = parser.numberType();
                            if (numberType == XContentParser.NumberType.INT || numberType == XContentParser.NumberType.LONG) {
                                neighbours.add(parser.intValue());
                            } else {
                                neighbours.add(GeoUtils.geoHashLevelsForPrecision(parser.doubleValue()));
                            }
                        } else if (token == Token.START_ARRAY) {
                            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                if (token == Token.VALUE_STRING || token == Token.VALUE_NUMBER) {
                                    neighbours.add(parser.intValue(true));
                                } else {
                                    throw new ElasticsearchParseException("neighbours array must have only numbers");
                                }
                            }
                        } else {
                            throw new ElasticsearchParseException("neighbours must be a number or a list of numbers");
                        }
                    } else if (CONTEXT_PRECISION.equals(currentFieldName)) {
                        if (token == Token.VALUE_STRING) {
                            precision = GeoUtils.geoHashLevelsForPrecision(parser.text());
                        } else if (token == Token.VALUE_NUMBER) {
                            XContentParser.NumberType numberType = parser.numberType();
                            if (numberType == XContentParser.NumberType.INT || numberType == XContentParser.NumberType.LONG) {
                                precision = parser.intValue();
                            } else {
                                precision = GeoUtils.geoHashLevelsForPrecision(parser.doubleValue());
                            }
                        } else {
                            throw new ElasticsearchParseException("precision must be a number");
                        }
                    }
                }
            }
            if (point == null) {
                if (Double.isNaN(lat) == false && Double.isNaN(lon) == false) {
                    point = new GeoPoint(lat, lon);
                } else {
                    throw new ElasticsearchParseException("no context provided");
                }
            }

            String geoHash = GeoHashUtils.stringEncode(point.getLon(), point.getLat(), precision);
            if (neighbours.size() > 0) {
                final int[] neighbourValues = new int[neighbours.size()];
                for (int i = 0; i < neighbours.size(); i++) {
                    neighbourValues[i] = neighbours.get(i);
                }
                return new GeoQueryContext(geoHash, boost, precision, neighbourValues);
            } else {
                return new GeoQueryContext(geoHash, boost, precision, precision);
            }
        } else {
            throw new ElasticsearchParseException("contexts field expected string or object but was [" + token.name() + "]");
        }
    }

    @Override
    public List<CategoryQueryContext> getQueryContexts(List<CategoryQueryContext> queryContexts) {
        List<CategoryQueryContext> queryContextList = new ArrayList<>();
        for (CategoryQueryContext queryContext : queryContexts) {
            GeoQueryContext geoQueryContext = ((GeoQueryContext) queryContext);
            int precision = Math.min(this.precision, geoQueryContext.context.length());
            String truncatedGeohash = geoQueryContext.context.toString().substring(0, precision);
            queryContextList.add(new CategoryQueryContext(truncatedGeohash, geoQueryContext.boost, false));
            for (int neighboursPrecision : geoQueryContext.neighbours) {
                int neighbourPrecision = Math.min(neighboursPrecision, truncatedGeohash.length());
                String neighbourGeohash = truncatedGeohash.substring(0, neighbourPrecision);
                Collection<String> locations = new HashSet<>();
                GeoHashUtils.addNeighbors(neighbourGeohash, neighbourPrecision, locations);
                boolean isPrefix = neighbourPrecision < precision;
                for (String location : locations) {
                    queryContextList.add(new CategoryQueryContext(location, geoQueryContext.boost, isPrefix));
                }
            }
        }
        return queryContextList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        GeoContextMapping that = (GeoContextMapping) o;
        if (precision != that.precision) return false;
        return !(fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null);

    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, fieldName);
    }

    public static class Builder extends ContextBuilder<GeoContextMapping> {

        private int precision = DEFAULT_PRECISION;
        private String fieldName = null;
        
        protected Builder(String name) {
            super(name);
        }

        /**
         * Set the precision use o make suggestions
         * 
         * @param precision
         *            precision as distance with {@link DistanceUnit}. Default:
         *            meters
         * @return this
         */
        public Builder precision(String precision) {
            return precision(DistanceUnit.parse(precision, DistanceUnit.METERS, DistanceUnit.METERS));
        }

        /**
         * Set the precision use o make suggestions
         * 
         * @param precision
         *            precision value
         * @param unit
         *            {@link DistanceUnit} to use
         * @return this
         */
        public Builder precision(double precision, DistanceUnit unit) {
            return precision(unit.toMeters(precision));
        }

        /**
         * Set the precision use o make suggestions
         * 
         * @param meters
         *            precision as distance in meters
         * @return this
         */
        public Builder precision(double meters) {
            int level = GeoUtils.geoHashLevelsForPrecision(meters);
            // Ceiling precision: we might return more results 
            if (GeoUtils.geoHashCellSize(level) < meters) {
               level = Math.max(1, level - 1); 
            }
            return precision(level);
        }

        /**
         * Set the precision use o make suggestions
         * 
         * @param level
         *            maximum length of geohashes
         * @return this
         */
        public Builder precision(int level) {
            this.precision = level;
            return this;
        }

        /**
         * Set the name of the field containing a geolocation to use
         * @param fieldName name of the field
         * @return this
         */
        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        @Override
        public GeoContextMapping build() {
            return new GeoContextMapping(name, fieldName, precision);
        }
    }
}
