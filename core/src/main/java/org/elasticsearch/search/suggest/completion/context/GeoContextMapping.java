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
import org.apache.lucene.search.suggest.xdocument.CompletionQuery;
import org.apache.lucene.search.suggest.xdocument.ContextQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoHashUtils;
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
 * A {@link ContextMapping} that defines criterion type as a geo hash
 * The suggestions can be boosted and/or filtered depending on
 * whether it falls within an area, represented by a query geo hash
 *
 * {@link GeoQueryContext} defines the options for constructing
 * a unit of query context for this context type
 *
 * Internally, geo point values are converted to a geo hash and prepended
 * with the suggestion value at index time
 */
public class GeoContextMapping extends ContextMapping<GeoQueryContext> {

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
                        contexts.add(GeoHashUtils.encode(lat, lon, precision));
                    } else {
                        throw new ElasticsearchParseException("only two values expected");
                    }
                } else {
                    throw new ElasticsearchParseException("latitude must be a numeric value");
                }
            } else {
                while (token != Token.END_ARRAY) {
                    GeoPoint point = GeoUtils.parseGeoPoint(parser);
                    contexts.add(GeoHashUtils.encode(point.getLat(), point.getLon(), precision));
                    token = parser.nextToken();
                }
            }
        } else {
            // or a single location
            GeoPoint point = GeoUtils.parseGeoPoint(parser);
            contexts.add(GeoHashUtils.encode(point.getLat(), point.getLon(), precision));
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
                            geohashes.add(GeoHashUtils.encode(spare.getLat(), spare.getLon(), precision));
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

    @Override
    public QueryContexts<GeoQueryContext> parseQueryContext(String name, XContentParser parser) throws IOException, ElasticsearchParseException {
        QueryContexts<GeoQueryContext> queryContexts = new QueryContexts<>(name);
        Token token = parser.nextToken();
        if (token == Token.START_ARRAY) {
            while (parser.nextToken() != Token.END_ARRAY) {
                GeoQueryContext current = innerParseQueryContext(parser);
                if (current != null) {
                    queryContexts.add(current);
                }
            }
        } else if (token == Token.START_OBJECT || token == Token.VALUE_STRING) {
            GeoQueryContext current = innerParseQueryContext(parser);
            if (current != null) {
                queryContexts.add(current);
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
                } else if (token == Token.VALUE_STRING) {
                    if ("lat".equals(currentFieldName)) {
                        if (point == null) {
                            lat = parser.doubleValue(true);
                        } else {
                            throw new ElasticsearchParseException("only lat/lon is allowed");
                        }
                    } else if ("lon".equals(currentFieldName)) {
                        if (point == null) {
                            lon = parser.doubleValue(true);
                        } else {
                            throw new ElasticsearchParseException("only lat/lon is allowed");
                        }
                    } else if (CONTEXT_VALUE.equals(currentFieldName)) {
                        point = GeoUtils.parseGeoPoint(parser);
                    } else if (CONTEXT_BOOST.equals(currentFieldName)) {
                        Number number;
                        try {
                            number = Long.parseLong(parser.text());
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Boost must be a string representing a numeric value, but was [" + parser.text() + "]");
                        }
                        boost = number.intValue();
                    } else if (CONTEXT_NEIGHBOURS.equals(currentFieldName)) {
                        neighbours.add(GeoUtils.geoHashLevelsForPrecision(parser.text()));
                    } else if (CONTEXT_PRECISION.equals(currentFieldName)) {
                        precision = GeoUtils.geoHashLevelsForPrecision(parser.text());
                    } else {
                        throw new ElasticsearchParseException("unknown field [" + currentFieldName + "] for string value");
                    }
                } else if (token == Token.VALUE_NUMBER) {
                    if ("lat".equals(currentFieldName)) {
                        if (point == null) {
                            lat = parser.doubleValue(true);
                        } else {
                            throw new ElasticsearchParseException("only lat/lon is allowed");
                        }
                    } else if ("lon".equals(currentFieldName)) {
                        if (point == null) {
                            lon = parser.doubleValue(true);
                        } else {
                            throw new ElasticsearchParseException("only lat/lon is allowed");
                        }
                    } else if (CONTEXT_NEIGHBOURS.equals(currentFieldName)) {
                        XContentParser.NumberType numberType = parser.numberType();
                        if (numberType == XContentParser.NumberType.INT || numberType == XContentParser.NumberType.LONG) {
                            neighbours.add(parser.intValue());
                        } else {
                            neighbours.add(GeoUtils.geoHashLevelsForPrecision(parser.doubleValue()));
                        }
                    } else if (CONTEXT_PRECISION.equals(currentFieldName)) {
                        XContentParser.NumberType numberType = parser.numberType();
                        if (numberType == XContentParser.NumberType.INT || numberType == XContentParser.NumberType.LONG) {
                            precision = parser.intValue();
                        } else {
                            precision = GeoUtils.geoHashLevelsForPrecision(parser.doubleValue());
                        }
                    } else if (CONTEXT_BOOST.equals(currentFieldName)) {
                        XContentParser.NumberType numberType = parser.numberType();
                        Number number = parser.numberValue();
                        if (numberType == XContentParser.NumberType.INT) {
                            boost = number.intValue();
                        } else {
                            throw new ElasticsearchParseException("boost must be in the interval [0..2147483647], but was [" + number.longValue() + "]");
                        }
                    } else {
                        throw new ElasticsearchParseException("unknown field [" + currentFieldName + "] for numeric value");
                    }
                } else if (token == Token.START_OBJECT) {
                    if (CONTEXT_VALUE.equals(currentFieldName)) {
                        point = GeoUtils.parseGeoPoint(parser);
                    } else {
                        throw new ElasticsearchParseException("unknown field [" + currentFieldName + "] for object value");
                    }
                } else if (token == Token.START_ARRAY) {
                    if (CONTEXT_NEIGHBOURS.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            if (token == Token.VALUE_STRING || token == Token.VALUE_NUMBER) {
                                neighbours.add(parser.intValue(true));
                            } else {
                                throw new ElasticsearchParseException("neighbours accept string/numbers");
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException("unknown field [" + currentFieldName + "] for array value");
                    }
                }
            }
            if (point == null) {
                if (Double.isNaN(lat) == false && Double.isNaN(lon) == false) {
                    point = new GeoPoint(lat, lon);
                } else {
                    throw new ElasticsearchParseException("no context value");
                }
            }
            if (neighbours.size() > 0) {
                final int[] neighbourValues = new int[neighbours.size()];
                for (int i = 0; i < neighbours.size(); i++) {
                    neighbourValues[i] = neighbours.get(i);
                }
                return new GeoQueryContext(point, boost, precision, neighbourValues);
            } else {
                return new GeoQueryContext(point, boost, precision, precision);
            }
        } else {
            throw new ElasticsearchParseException("expected string or object");
        }
    }


    @Override
    public CompletionQuery toContextQuery(CompletionQuery query, @Nullable QueryContexts<GeoQueryContext> queryContexts) {
        final ContextQuery contextQuery = new ContextQuery(query);
        if (queryContexts != null) {
            for (GeoQueryContext queryContext : queryContexts) {
                int precision = Math.min(this.precision, queryContext.geoHash.length());
                String truncatedGeohash = queryContext.geoHash.toString().substring(0, precision);
                contextQuery.addContext(truncatedGeohash, queryContext.boost, false);
                for (int neighboursPrecision : queryContext.neighbours) {
                    int neighbourPrecision = Math.min(neighboursPrecision, truncatedGeohash.length());
                    String neighbourGeohash = truncatedGeohash.substring(0, neighbourPrecision);
                    Collection<String> locations = new HashSet<>();
                    GeoHashUtils.addNeighbors(neighbourGeohash, precision, locations);
                    for (String location : locations) {
                        contextQuery.addContext(location, queryContext.boost, false);
                    }
                }
            }
        }
        return contextQuery;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
        result = prime * result + precision;
        return result;
    }
   
    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            if (obj instanceof GeoContextMapping) {
                GeoContextMapping other = (GeoContextMapping) obj;
                if (fieldName == null) {
                    if (other.fieldName != null) {
                        return false;
                    }
                } else if (!fieldName.equals(other.fieldName)) {
                    return false;
                } else if (precision != other.precision) {
                    return false;
                }
                return true;
            }
        }
        return false;
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
