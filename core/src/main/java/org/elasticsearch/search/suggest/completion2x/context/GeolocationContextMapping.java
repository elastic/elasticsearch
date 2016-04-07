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

package org.elasticsearch.search.suggest.completion2x.context;

import com.carrotsearch.hppc.IntHashSet;
import org.apache.lucene.analysis.PrefixAnalyzer.PrefixTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.fst.FST;
import org.elasticsearch.ElasticsearchParseException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * The {@link GeolocationContextMapping} allows to take GeoInfomation into account
 * during building suggestions. The mapping itself works with geohashes
 * explicitly and is configured by three parameters:
 * <ul>
 * <li><code>precision</code>: length of the geohash indexed as prefix of the
 * completion field</li>
 * <li><code>neighbors</code>: Should the neighbor cells of the deepest geohash
 * level also be indexed as alternatives to the actual geohash</li>
 * <li><code>location</code>: (optional) location assumed if it is not provided</li>
 * </ul>
 * Internally this mapping wraps the suggestions into a form
 * <code>[geohash][suggestion]</code>. If the neighbor option is set the cells
 * next to the cell on the deepest geohash level ( <code>precision</code>) will
 * be indexed as well. The {@link TokenStream} used to build the {@link FST} for
 * suggestion will be wrapped into a {@link PrefixTokenFilter} managing these
 * geohases as prefixes.
 */
public class GeolocationContextMapping extends ContextMapping {

    public static final String TYPE = "geo";

    public static final String FIELD_PRECISION = "precision";
    public static final String FIELD_NEIGHBORS = "neighbors";
    public static final String FIELD_FIELDNAME = "path";

    private final Collection<String> defaultLocations;
    private final int[] precision;
    private final boolean neighbors;
    private final String fieldName;
    private final GeoConfig defaultConfig;

    /**
     * Create a new {@link GeolocationContextMapping} with a given precision
     *
     * @param precision
     *            length of the geohashes
     * @param neighbors
     *            should neighbors be indexed
     * @param defaultLocations
     *            location to use, if it is not provided by the document
     */
    protected GeolocationContextMapping(String name, int[] precision, boolean neighbors,
                                        Collection<String> defaultLocations, String fieldName) {
        super(TYPE, name);
        this.precision = precision;
        this.neighbors = neighbors;
        this.defaultLocations = defaultLocations;
        this.fieldName = fieldName;
        this.defaultConfig = new GeoConfig(this, defaultLocations);
    }

    /**
     * load a {@link GeolocationContextMapping} by configuration. Such a configuration
     * can set the parameters
     * <ul>
     * <li>precision [<code>String</code>, <code>Double</code>,
     * <code>Float</code> or <code>Integer</code>] defines the length of the
     * underlying geohash</li>
     * <li>defaultLocation [<code>String</code>] defines the location to use if
     * it is not provided by the document</li>
     * <li>neighbors [<code>Boolean</code>] defines if the last level of the
     * geohash should be extended by neighbor cells</li>
     * </ul>
     *
     * @param config
     *            Configuration for {@link GeolocationContextMapping}
     * @return new {@link GeolocationContextMapping} configured by the parameters of
     *         <code>config</code>
     */
    protected static GeolocationContextMapping load(String name, Map<String, Object> config) {
        if (!config.containsKey(FIELD_PRECISION)) {
            throw new ElasticsearchParseException("field [precision] is missing");
        }

        final GeolocationContextMapping.Builder builder = new GeolocationContextMapping.Builder(name);

        if (config != null) {
            final Object configPrecision = config.get(FIELD_PRECISION);
            if (configPrecision == null) {
                // ignore precision
            } else if (configPrecision instanceof Integer) {
                builder.precision((Integer) configPrecision);
                config.remove(FIELD_PRECISION);
            } else if (configPrecision instanceof Long) {
                builder.precision((Long) configPrecision);
                config.remove(FIELD_PRECISION);
            } else if (configPrecision instanceof Double) {
                builder.precision((Double) configPrecision);
                config.remove(FIELD_PRECISION);
            } else if (configPrecision instanceof Float) {
                builder.precision((Float) configPrecision);
                config.remove(FIELD_PRECISION);
            } else if (configPrecision instanceof Iterable) {
                for (Object precision : (Iterable)configPrecision) {
                    if (precision instanceof Integer) {
                        builder.precision((Integer) precision);
                    } else if (precision instanceof Long) {
                        builder.precision((Long) precision);
                    } else if (precision instanceof Double) {
                        builder.precision((Double) precision);
                    } else if (precision instanceof Float) {
                        builder.precision((Float) precision);
                    } else {
                        builder.precision(precision.toString());
                    }
                }
                config.remove(FIELD_PRECISION);
            } else {
                builder.precision(configPrecision.toString());
                config.remove(FIELD_PRECISION);
            }

            final Object configNeighbors = config.get(FIELD_NEIGHBORS);
            if (configNeighbors != null) {
                builder.neighbors((Boolean) configNeighbors);
                config.remove(FIELD_NEIGHBORS);
            }

            final Object def = config.get(FIELD_MISSING);
            if (def != null) {
                if (def instanceof Iterable) {
                    for (Object location : (Iterable)def) {
                        builder.addDefaultLocation(location.toString());
                    }
                } else if (def instanceof String) {
                    builder.addDefaultLocation(def.toString());
                } else if (def instanceof Map) {
                    Map<String, Object> latlonMap = (Map<String, Object>) def;
                    if (!latlonMap.containsKey("lat") || !(latlonMap.get("lat") instanceof Double)) {
                        throw new ElasticsearchParseException(
                            "field [{}] map must have field lat and a valid latitude", FIELD_MISSING);
                    }
                    if (!latlonMap.containsKey("lon") || !(latlonMap.get("lon") instanceof Double)) {
                        throw new ElasticsearchParseException(
                            "field [{}] map must have field lon and a valid longitude", FIELD_MISSING);
                    }
                    builder.addDefaultLocation(
                        Double.valueOf(latlonMap.get("lat").toString()), Double.valueOf(latlonMap.get("lon").toString()));
                } else {
                    throw new ElasticsearchParseException("field [{}] must be of type string or list", FIELD_MISSING);
                }
                config.remove(FIELD_MISSING);
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
        builder.field(FIELD_NEIGHBORS, neighbors);
        if (defaultLocations != null) {
            builder.startArray(FIELD_MISSING);
            for (String defaultLocation : defaultLocations) {
                builder.value(defaultLocation);
            }
            builder.endArray();
        }
        if (fieldName != null) {
            builder.field(FIELD_FIELDNAME, fieldName);
        }
        return builder;
    }

    protected static Collection<String> parseSinglePointOrList(XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        if(token == Token.START_ARRAY) {
            token = parser.nextToken();
            // Test if value is a single point in <code>[lon, lat]</code> format
            if(token == Token.VALUE_NUMBER) {
                double lon = parser.doubleValue();
                if(parser.nextToken() == Token.VALUE_NUMBER) {
                    double lat = parser.doubleValue();
                    if(parser.nextToken() == Token.END_ARRAY) {
                        return Collections.singleton(GeoHashUtils.stringEncode(lon, lat));
                    } else {
                        throw new ElasticsearchParseException("only two values expected");
                    }
                } else {
                    throw new ElasticsearchParseException("latitue must be a numeric value");
                }
            } else {
                // otherwise it's a list of locations
                ArrayList<String> result = new ArrayList<>();
                while (token != Token.END_ARRAY) {
                    result.add(GeoUtils.parseGeoPoint(parser).geohash());
                    token = parser.nextToken(); //infinite loop without this line
                }
                return result;
            }
        } else {
            // or a single location
            return Collections.singleton(GeoUtils.parseGeoPoint(parser).geohash());
        }
    }

    @Override
    public ContextConfig defaultConfig() {
        return defaultConfig;
    }

    @Override
    public ContextConfig parseContext(ParseContext parseContext, XContentParser parser)
        throws IOException, ElasticsearchParseException {

        if(fieldName != null) {
            FieldMapper mapper = parseContext.docMapper().mappers().getMapper(fieldName);
            if(!(mapper instanceof GeoPointFieldMapper)) {
                throw new ElasticsearchParseException("referenced field must be mapped to geo_point");
            }
        }

        Collection<String> locations;
        if(parser.currentToken() == Token.VALUE_NULL) {
            locations = null;
        } else {
            locations = parseSinglePointOrList(parser);
        }
        return new GeoConfig(this, locations);
    }

    /**
     * Create a new geolocation query from a given GeoPoint
     *
     * @param point
     *            query location
     * @return new geolocation query
     */
    public static GeoQuery query(String name, GeoPoint point) {
        return query(name, point.getGeohash());
    }

    /**
     * Create a new geolocation query from a given geocoordinate
     *
     * @param lat
     *            latitude of the location
     * @param lon
     *            longitude of the location
     * @return new geolocation query
     */
    public static GeoQuery query(String name, double lat, double lon, int ... precisions) {
        return query(name, GeoHashUtils.stringEncode(lon, lat), precisions);
    }

    public static GeoQuery query(String name, double lat, double lon, String ... precisions) {
        int precisionInts[] = new int[precisions.length];
        for (int i = 0 ; i < precisions.length; i++) {
            precisionInts[i] = GeoUtils.geoHashLevelsForPrecision(precisions[i]);
        }
        return query(name, GeoHashUtils.stringEncode(lon, lat), precisionInts);
    }

    /**
     * Create a new geolocation query from a given geohash
     *
     * @param geohash
     *            geohash of the location
     * @return new geolocation query
     */
    public static GeoQuery query(String name, String geohash, int ... precisions) {
        return new GeoQuery(name, geohash, precisions);
    }

    private static final int parsePrecision(XContentParser parser) throws IOException, ElasticsearchParseException {
        switch (parser.currentToken()) {
        case VALUE_STRING:
            return GeoUtils.geoHashLevelsForPrecision(parser.text());
        case VALUE_NUMBER:
            switch (parser.numberType()) {
            case INT:
            case LONG:
                return parser.intValue();
            default:
                return GeoUtils.geoHashLevelsForPrecision(parser.doubleValue());
            }
        default:
            throw new ElasticsearchParseException("invalid precision value");
        }
    }

    @Override
    public GeoQuery parseQuery(String name, XContentParser parser) throws IOException, ElasticsearchParseException {
        if (parser.currentToken() == Token.START_OBJECT) {
            double lat = Double.NaN;
            double lon = Double.NaN;
            GeoPoint point = null;
            int[] precision = null;

            while (parser.nextToken() != Token.END_OBJECT) {
                final String fieldName = parser.currentName();
                if("lat".equals(fieldName)) {
                    if(point == null) {
                        parser.nextToken();
                        switch (parser.currentToken()) {
                            case VALUE_NUMBER:
                            case VALUE_STRING:
                                lat = parser.doubleValue(true);
                                break;
                            default:
                                throw new ElasticsearchParseException("latitude must be a number");
                        }
                    } else {
                        throw new ElasticsearchParseException("only lat/lon or [{}] is allowed", FIELD_VALUE);
                    }
                } else if ("lon".equals(fieldName)) {
                    if(point == null) {
                        parser.nextToken();
                        switch (parser.currentToken()) {
                            case VALUE_NUMBER:
                            case VALUE_STRING:
                                lon = parser.doubleValue(true);
                                break;
                            default:
                                throw new ElasticsearchParseException("longitude must be a number");
                        }
                    } else {
                        throw new ElasticsearchParseException("only lat/lon or [{}] is allowed", FIELD_VALUE);
                    }
                } else if (FIELD_PRECISION.equals(fieldName)) {
                    if(parser.nextToken() == Token.START_ARRAY) {
                        IntHashSet precisions = new IntHashSet();
                        while(parser.nextToken() != Token.END_ARRAY) {
                            precisions.add(parsePrecision(parser));
                        }
                        precision = precisions.toArray();
                    } else {
                        precision = new int[] { parsePrecision(parser) };
                    }
                } else if (FIELD_VALUE.equals(fieldName)) {
                    if(Double.isNaN(lon) && Double.isNaN(lat)) {
                        parser.nextToken();
                        point = GeoUtils.parseGeoPoint(parser);
                    } else {
                        throw new ElasticsearchParseException("only lat/lon or [{}] is allowed", FIELD_VALUE);
                    }
                } else {
                    throw new ElasticsearchParseException("unexpected fieldname [{}]", fieldName);
                }
            }

            if (point == null) {
                if (Double.isNaN(lat) || Double.isNaN(lon)) {
                    throw new ElasticsearchParseException("location is missing");
                } else {
                    point = new GeoPoint(lat, lon);
                }
            }

            if (precision == null || precision.length == 0) {
                precision = this.precision;
            }

            return new GeoQuery(name, point.geohash(), precision);
        } else {
            return new GeoQuery(name, GeoUtils.parseGeoPoint(parser).getGeohash(), precision);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((defaultLocations == null) ? 0 : defaultLocations.hashCode());
        result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
        result = prime * result + (neighbors ? 1231 : 1237);
        result = prime * result + Arrays.hashCode(precision);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        GeolocationContextMapping other = (GeolocationContextMapping) obj;
        if (defaultLocations == null) {
            if (other.defaultLocations != null)
                return false;
        } else if (!defaultLocations.equals(other.defaultLocations))
            return false;
        if (fieldName == null) {
            if (other.fieldName != null)
                return false;
        } else if (!fieldName.equals(other.fieldName))
            return false;
        if (neighbors != other.neighbors)
            return false;
        if (!Arrays.equals(precision, other.precision))
            return false;
        return true;
    }




    public static class Builder extends ContextBuilder<GeolocationContextMapping> {

        private IntHashSet precisions = new IntHashSet();
        private boolean neighbors; // take neighbor cell on the lowest level into account
        private HashSet<String> defaultLocations = new HashSet<>();
        private String fieldName = null;

        protected Builder(String name) {
            this(name, true, null);
        }

        protected Builder(String name, boolean neighbors, int...levels) {
            super(name);
            neighbors(neighbors);
            if (levels != null) {
                for (int level : levels) {
                    precision(level);
                }
            }
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
            this.precisions.add(level);
            return this;
        }

        /**
         * Set neighborhood usage
         *
         * @param neighbors
         *            should neighbor cells also be valid
         * @return this
         */
        public Builder neighbors(boolean neighbors) {
            this.neighbors = neighbors;
            return this;
        }

        /**
         * Set a default location that should be used, if no location is
         * provided by the query
         *
         * @param geohash
         *            geohash of the default location
         * @return this
         */
        public Builder addDefaultLocation(String geohash) {
            this.defaultLocations.add(geohash);
            return this;
        }

        /**
         * Set a default location that should be used, if no location is
         * provided by the query
         *
         * @param geohashes
         *            geohash of the default location
         * @return this
         */
        public Builder addDefaultLocations(Collection<String> geohashes) {
            this.defaultLocations.addAll(geohashes);
            return this;
        }

        /**
         * Set a default location that should be used, if no location is
         * provided by the query
         *
         * @param lat
         *            latitude of the default location
         * @param lon
         *            longitude of the default location
         * @return this
         */
        public Builder addDefaultLocation(double lat, double lon) {
            this.defaultLocations.add(GeoHashUtils.stringEncode(lon, lat));
            return this;
        }

        /**
         * Set a default location that should be used, if no location is
         * provided by the query
         *
         * @param point
         *            location
         * @return this
         */
        public Builder defaultLocation(GeoPoint point) {
            this.defaultLocations.add(point.geohash());
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
        public GeolocationContextMapping build() {
            if(precisions.isEmpty()) {
                precisions.add(GeoHashUtils.PRECISION);
            }
            int[] precisionArray = precisions.toArray();
            Arrays.sort(precisionArray);
            return new GeolocationContextMapping(name, precisionArray, neighbors, defaultLocations, fieldName);
        }

    }

    private static class GeoConfig extends ContextConfig {

        private final GeolocationContextMapping mapping;
        private final Collection<String> locations;

        public GeoConfig(GeolocationContextMapping mapping, Collection<String> locations) {
            this.locations = locations;
            this.mapping = mapping;
        }

        @Override
        protected TokenStream wrapTokenStream(Document doc, TokenStream stream) {
            Collection<String> geohashes;

            if (locations == null || locations.size() == 0) {
                if(mapping.fieldName != null) {
                    IndexableField[] fields = doc.getFields(mapping.fieldName);
                    if(fields.length == 0) {
                        IndexableField[] lonFields = doc.getFields(mapping.fieldName + ".lon");
                        IndexableField[] latFields = doc.getFields(mapping.fieldName + ".lat");
                        if (lonFields.length > 0 && latFields.length > 0) {
                            geohashes = new ArrayList<>(fields.length);
                            GeoPoint spare = new GeoPoint();
                            for (int i = 0 ; i < lonFields.length ; i++) {
                                IndexableField lonField = lonFields[i];
                                IndexableField latField = latFields[i];
                                assert lonField.fieldType().docValuesType() == latField.fieldType().docValuesType();
                                // we write doc values fields differently: one field for all values,
                                // so we need to only care about indexed fields
                                if (lonField.fieldType().docValuesType() == DocValuesType.NONE) {
                                    spare.reset(latField.numericValue().doubleValue(), lonField.numericValue().doubleValue());
                                    geohashes.add(spare.geohash());
                                }
                            }
                        } else {
                            geohashes = mapping.defaultLocations;
                        }
                    } else {
                        geohashes = new ArrayList<>(fields.length);
                        GeoPoint spare = new GeoPoint();
                        for (IndexableField field : fields) {
                            if (field instanceof StringField) {
                                spare.resetFromString(field.stringValue());
                            } else if (field instanceof GeoPointField) {
                                GeoPointField geoPointField = (GeoPointField) field;
                                spare.reset(geoPointField.getLat(), geoPointField.getLon());
                            } else {
                                spare.resetFromString(field.stringValue());
                            }
                            geohashes.add(spare.geohash());
                        }
                    }
                } else {
                    geohashes = mapping.defaultLocations;
                }
            } else {
                geohashes = locations;
            }

            Collection<String> locations = new HashSet<>();
            for (String geohash : geohashes) {
                for (int p : mapping.precision) {
                    int precision = Math.min(p, geohash.length());
                    String truncatedGeohash = geohash.substring(0, precision);
                    if(mapping.neighbors) {
                        GeoHashUtils.addNeighbors(truncatedGeohash, precision, locations);
                    }
                    locations.add(truncatedGeohash);
                }
            }

            return new PrefixTokenFilter(stream, ContextMapping.SEPARATOR, locations);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("GeoConfig(location = [");
            Iterator<? extends CharSequence> location = this.locations.iterator();
            if (location.hasNext()) {
                sb.append(location.next());
                while (location.hasNext()) {
                    sb.append(", ").append(location.next());
                }
            }
            return sb.append("])").toString();
        }
    }

    private static class GeoQuery extends ContextQuery {
        private final String location;
        private final int[] precisions;

        public GeoQuery(String name, String location, int...precisions) {
            super(name);
            this.location = location;
            this.precisions = precisions;
        }

        @Override
        public Automaton toAutomaton() {
            Automaton automaton;
            if(precisions == null || precisions.length == 0) {
                 automaton = Automata.makeString(location);
            } else {
                automaton = Automata.makeString(
                    location.substring(0, Math.max(1, Math.min(location.length(), precisions[0]))));
                for (int i = 1; i < precisions.length; i++) {
                    final String cell = location.substring(0, Math.max(1, Math.min(location.length(), precisions[i])));
                    automaton = Operations.union(automaton, Automata.makeString(cell));
                }
            }
            return automaton;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if(precisions == null || precisions.length == 0) {
                builder.field(name, location);
            } else {
                builder.startObject(name);
                builder.field(FIELD_VALUE, location);
                builder.field(FIELD_PRECISION, precisions);
                builder.endObject();
            }
            return builder;
        }
    }
}
