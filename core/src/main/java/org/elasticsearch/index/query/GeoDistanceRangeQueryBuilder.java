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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.search.XGeoPointDistanceRangeQuery;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.BaseGeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapperLegacy;
import org.elasticsearch.index.search.geo.GeoDistanceRangeQuery;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public class GeoDistanceRangeQueryBuilder extends AbstractQueryBuilder<GeoDistanceRangeQueryBuilder> {
    public static final String NAME = "geo_distance_range";

    public static final boolean DEFAULT_INCLUDE_LOWER = true;
    public static final boolean DEFAULT_INCLUDE_UPPER = true;
    public static final GeoDistance DEFAULT_GEO_DISTANCE = GeoDistance.DEFAULT;
    public static final DistanceUnit DEFAULT_UNIT = DistanceUnit.DEFAULT;
    public static final String DEFAULT_OPTIMIZE_BBOX = "memory";

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField FROM_FIELD = new ParseField("from");
    private static final ParseField TO_FIELD = new ParseField("to");
    private static final ParseField INCLUDE_LOWER_FIELD = new ParseField("include_lower");
    private static final ParseField INCLUDE_UPPER_FIELD = new ParseField("include_upper");
    private static final ParseField GT_FIELD = new ParseField("gt");
    private static final ParseField GTE_FIELD = new ParseField("gte", "ge");
    private static final ParseField LT_FIELD = new ParseField("lt");
    private static final ParseField LTE_FIELD = new ParseField("lte", "le");
    private static final ParseField UNIT_FIELD = new ParseField("unit");
    private static final ParseField DISTANCE_TYPE_FIELD = new ParseField("distance_type");
    private static final ParseField NAME_FIELD = new ParseField("_name");
    private static final ParseField BOOST_FIELD = new ParseField("boost");
    private static final ParseField OPTIMIZE_BBOX_FIELD = new ParseField("optimize_bbox");
    private static final ParseField COERCE_FIELD = new ParseField("coerce", "normalize")
            .withAllDeprecated("use validation_method instead");
    private static final ParseField IGNORE_MALFORMED_FIELD = new ParseField("ignore_malformed")
            .withAllDeprecated("use validation_method instead");
    private static final ParseField VALIDATION_METHOD = new ParseField("validation_method");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String fieldName;

    private Object from;
    private Object to;
    private boolean includeLower = DEFAULT_INCLUDE_LOWER;
    private boolean includeUpper = DEFAULT_INCLUDE_UPPER;

    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    private final GeoPoint point;

    private GeoDistance geoDistance = DEFAULT_GEO_DISTANCE;

    private DistanceUnit unit = DEFAULT_UNIT;

    private String optimizeBbox = DEFAULT_OPTIMIZE_BBOX;

    private GeoValidationMethod validationMethod = GeoValidationMethod.DEFAULT;

    public GeoDistanceRangeQueryBuilder(String fieldName, GeoPoint point) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("fieldName must not be null");
        }
        if (point == null) {
            throw new IllegalArgumentException("point must not be null");
        }
        this.fieldName = fieldName;
        this.point = point;
    }

    public GeoDistanceRangeQueryBuilder(String fieldName, double lat, double lon) {
        this(fieldName, new GeoPoint(lat, lon));
    }

    public GeoDistanceRangeQueryBuilder(String fieldName, String geohash) {
        this(fieldName, geohash == null ? null : new GeoPoint().resetFromGeoHash(geohash));
    }

    /**
     * Read from a stream.
     */
    public GeoDistanceRangeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        point = in.readGeoPoint();
        from = in.readGenericValue();
        to = in.readGenericValue();
        includeLower = in.readBoolean();
        includeUpper = in.readBoolean();
        unit = DistanceUnit.valueOf(in.readString());
        geoDistance = GeoDistance.readFromStream(in);
        optimizeBbox = in.readString();
        validationMethod = GeoValidationMethod.readFromStream(in);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGeoPoint(point);
        out.writeGenericValue(from);
        out.writeGenericValue(to);
        out.writeBoolean(includeLower);
        out.writeBoolean(includeUpper);
        out.writeString(unit.name());
        geoDistance.writeTo(out);;
        out.writeString(optimizeBbox);
        validationMethod.writeTo(out);
        out.writeBoolean(ignoreUnmapped);
    }

    public String fieldName() {
        return fieldName;
    }

    public GeoPoint point() {
        return point;
    }

    public GeoDistanceRangeQueryBuilder from(String from) {
        if (from == null) {
            throw new IllegalArgumentException("[from] must not be null");
        }
        this.from = from;
        return this;
    }

    public GeoDistanceRangeQueryBuilder from(Number from) {
        if (from == null) {
            throw new IllegalArgumentException("[from] must not be null");
        }
        this.from = from;
        return this;
    }

    public Object from() {
        return from;
    }

    public GeoDistanceRangeQueryBuilder to(String to) {
        if (to == null) {
            throw new IllegalArgumentException("[to] must not be null");
        }
        this.to = to;
        return this;
    }

    public GeoDistanceRangeQueryBuilder to(Number to) {
        if (to == null) {
            throw new IllegalArgumentException("[to] must not be null");
        }
        this.to = to;
        return this;
    }

    public Object to() {
        return to;
    }

    public GeoDistanceRangeQueryBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    public boolean includeLower() {
        return includeLower;
    }

    public GeoDistanceRangeQueryBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    public boolean includeUpper() {
        return includeUpper;
    }

    public GeoDistanceRangeQueryBuilder geoDistance(GeoDistance geoDistance) {
        if (geoDistance == null) {
            throw new IllegalArgumentException("geoDistance calculation mode must not be null");
        }
        this.geoDistance = geoDistance;
        return this;
    }

    public GeoDistance geoDistance() {
        return geoDistance;
    }

    public GeoDistanceRangeQueryBuilder unit(DistanceUnit unit) {
        if (unit == null) {
            throw new IllegalArgumentException("distance unit must not be null");
        }
        this.unit = unit;
        return this;
    }

    public DistanceUnit unit() {
        return unit;
    }

    public GeoDistanceRangeQueryBuilder optimizeBbox(String optimizeBbox) {
        if (optimizeBbox == null) {
            throw new IllegalArgumentException("optimizeBbox must not be null");
        }
        switch (optimizeBbox) {
            case "none":
            case "memory":
            case "indexed":
                break;
            default:
                throw new IllegalArgumentException("optimizeBbox must be one of [none, memory, indexed]");
        }
        this.optimizeBbox = optimizeBbox;
        return this;
    }

    public String optimizeBbox() {
        return optimizeBbox;
    }

    /** Set validation method for coordinates. */
    public GeoDistanceRangeQueryBuilder setValidationMethod(GeoValidationMethod method) {
        this.validationMethod = method;
        return this;
    }

    /** Returns validation method for coordinates. */
    public GeoValidationMethod getValidationMethod() {
        return this.validationMethod;
    }

    /**
     * Sets whether the query builder should ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public GeoDistanceRangeQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
            }
        }
        if (!(fieldType instanceof BaseGeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryShardException(context, "field [" + fieldName + "] is not a geo_point field");
        }

        final boolean indexCreatedBeforeV2_0 = context.indexVersionCreated().before(Version.V_2_0_0);
        final boolean indexCreatedBeforeV2_2 = context.indexVersionCreated().before(Version.V_2_2_0);
        // validation was not available prior to 2.x, so to support bwc
        // percolation queries we only ignore_malformed on 2.x created indexes
        if (!indexCreatedBeforeV2_0 && !GeoValidationMethod.isIgnoreMalformed(validationMethod)) {
            if (!GeoUtils.isValidLatitude(point.lat())) {
                throw new QueryShardException(context, "illegal latitude value [{}] for [{}]", point.lat(), NAME);
            }
            if (!GeoUtils.isValidLongitude(point.lon())) {
                throw new QueryShardException(context, "illegal longitude value [{}] for [{}]", point.lon(), NAME);
            }
        }

        GeoPoint point = new GeoPoint(this.point);
        if (indexCreatedBeforeV2_2 == false || GeoValidationMethod.isCoerce(validationMethod)) {
            GeoUtils.normalizePoint(point, true, true);
        }

        Double fromValue;
        Double toValue;
        if (from != null) {
            if (from instanceof Number) {
                fromValue = unit.toMeters(((Number) from).doubleValue());
            } else {
                fromValue = DistanceUnit.parse((String) from, unit, DistanceUnit.DEFAULT);
            }
            if (indexCreatedBeforeV2_2 == true) {
                fromValue = geoDistance.normalize(fromValue, DistanceUnit.DEFAULT);
            }
        } else {
            fromValue = 0.0;
        }

        if (to != null) {
            if (to instanceof Number) {
                toValue = unit.toMeters(((Number) to).doubleValue());
            } else {
                toValue = DistanceUnit.parse((String) to, unit, DistanceUnit.DEFAULT);
            }
            if (indexCreatedBeforeV2_2 == true) {
                toValue = geoDistance.normalize(toValue, DistanceUnit.DEFAULT);
            }
        } else {
            toValue = GeoUtils.maxRadialDistanceMeters(point.lat(), point.lon());
        }

        final Version indexVersionCreated = context.indexVersionCreated();
        if (indexVersionCreated.before(Version.V_2_2_0)) {
            GeoPointFieldMapperLegacy.GeoPointFieldType geoFieldType = ((GeoPointFieldMapperLegacy.GeoPointFieldType) fieldType);
            IndexGeoPointFieldData indexFieldData = context.getForField(fieldType);
            return new GeoDistanceRangeQuery(point, fromValue, toValue, includeLower, includeUpper, geoDistance, geoFieldType,
                indexFieldData, optimizeBbox);
        }

        // if index created V_2_2 use (soon to be legacy) numeric encoding postings format
        // if index created V_2_3 > use prefix encoded postings format
        final GeoPointField.TermEncoding encoding = (indexVersionCreated.before(Version.V_2_3_0)) ?
            GeoPointField.TermEncoding.NUMERIC : GeoPointField.TermEncoding.PREFIX;

        return new XGeoPointDistanceRangeQuery(fieldType.name(), encoding, point.lat(), point.lon(),
            (includeLower) ? fromValue : fromValue + GeoUtils.TOLERANCE,
            (includeUpper) ? toValue : toValue - GeoUtils.TOLERANCE);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(fieldName).value(point.lon()).value(point.lat()).endArray();
        builder.field(FROM_FIELD.getPreferredName(), from);
        builder.field(TO_FIELD.getPreferredName(), to);
        builder.field(INCLUDE_LOWER_FIELD.getPreferredName(), includeLower);
        builder.field(INCLUDE_UPPER_FIELD.getPreferredName(), includeUpper);
        builder.field(UNIT_FIELD.getPreferredName(), unit);
        builder.field(DISTANCE_TYPE_FIELD.getPreferredName(), geoDistance.name().toLowerCase(Locale.ROOT));
        builder.field(OPTIMIZE_BBOX_FIELD.getPreferredName(), optimizeBbox);
        builder.field(VALIDATION_METHOD.getPreferredName(), validationMethod);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<GeoDistanceRangeQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token;

        Float boost = null;
        String queryName = null;
        String currentFieldName = null;
        GeoPoint point = null;
        String fieldName = null;
        Object vFrom = null;
        Object vTo = null;
        Boolean includeLower = null;
        Boolean includeUpper = null;
        DistanceUnit unit = null;
        GeoDistance geoDistance = null;
        String optimizeBbox = null;
        boolean coerce = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        boolean ignoreMalformed = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        GeoValidationMethod validationMethod = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (fieldName == null) {
                    if (point == null) {
                        point = new GeoPoint();
                    }
                    GeoUtils.parseGeoPoint(parser, point);
                    fieldName = currentFieldName;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + GeoDistanceRangeQueryBuilder.NAME +
                            "] field name already set to [" + fieldName + "] but found [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                // the json in the format of -> field : { lat : 30, lon : 12 }
                if (fieldName == null) {
                    fieldName = currentFieldName;
                    if (point == null) {
                        point = new GeoPoint();
                    }
                    GeoUtils.parseGeoPoint(parser, point);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + GeoDistanceRangeQueryBuilder.NAME +
                            "] field name already set to [" + fieldName + "] but found [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, FROM_FIELD)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vFrom = parser.text(); // a String
                    } else {
                        vFrom = parser.numberValue(); // a Number
                    }
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, TO_FIELD)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vTo = parser.text(); // a String
                    } else {
                        vTo = parser.numberValue(); // a Number
                    }
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INCLUDE_LOWER_FIELD)) {
                    includeLower = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INCLUDE_UPPER_FIELD)) {
                    includeUpper = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, IGNORE_UNMAPPED_FIELD)) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, GT_FIELD)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vFrom = parser.text(); // a String
                    } else {
                        vFrom = parser.numberValue(); // a Number
                    }
                    includeLower = false;
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, GTE_FIELD)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vFrom = parser.text(); // a String
                    } else {
                        vFrom = parser.numberValue(); // a Number
                    }
                    includeLower = true;
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, LT_FIELD)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vTo = parser.text(); // a String
                    } else {
                        vTo = parser.numberValue(); // a Number
                    }
                    includeUpper = false;
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, LTE_FIELD)) {
                    if (token == XContentParser.Token.VALUE_NULL) {
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        vTo = parser.text(); // a String
                    } else {
                        vTo = parser.numberValue(); // a Number
                    }
                    includeUpper = true;
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, UNIT_FIELD)) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, DISTANCE_TYPE_FIELD)) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if (currentFieldName.endsWith(GeoPointFieldMapper.Names.LAT_SUFFIX)) {
                    String maybeFieldName = currentFieldName.substring(0,
                            currentFieldName.length() - GeoPointFieldMapper.Names.LAT_SUFFIX.length());
                    if (fieldName == null || fieldName.equals(maybeFieldName)) {
                        fieldName = maybeFieldName;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + GeoDistanceRangeQueryBuilder.NAME +
                                "] field name already set to [" + fieldName + "] but found [" + currentFieldName + "]");
                    }
                    if (point == null) {
                        point = new GeoPoint();
                    }
                    point.resetLat(parser.doubleValue());
                } else if (currentFieldName.endsWith(GeoPointFieldMapper.Names.LON_SUFFIX)) {
                    String maybeFieldName = currentFieldName.substring(0,
                            currentFieldName.length() - GeoPointFieldMapper.Names.LON_SUFFIX.length());
                    if (fieldName == null || fieldName.equals(maybeFieldName)) {
                        fieldName = maybeFieldName;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + GeoDistanceRangeQueryBuilder.NAME +
                                "] field name already set to [" + fieldName + "] but found [" + currentFieldName + "]");
                    }
                    if (point == null) {
                        point = new GeoPoint();
                    }
                    point.resetLon(parser.doubleValue());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, OPTIMIZE_BBOX_FIELD)) {
                    optimizeBbox = parser.textOrNull();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, COERCE_FIELD)) {
                    coerce = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, IGNORE_MALFORMED_FIELD)) {
                    ignoreMalformed = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, VALIDATION_METHOD)) {
                    validationMethod = GeoValidationMethod.fromString(parser.text());
                } else {
                    if (fieldName == null) {
                        if (point == null) {
                            point = new GeoPoint();
                        }
                        point.resetFromString(parser.text());
                        fieldName = currentFieldName;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + GeoDistanceRangeQueryBuilder.NAME +
                                "] field name already set to [" + fieldName + "] but found [" + currentFieldName + "]");
                    }
                }
            }
        }

        GeoDistanceRangeQueryBuilder queryBuilder = new GeoDistanceRangeQueryBuilder(fieldName, point);
        if (boost != null) {
            queryBuilder.boost(boost);
        }

        if (queryName != null) {
            queryBuilder.queryName(queryName);
        }

        if (vFrom != null) {
            if (vFrom instanceof Number) {
                queryBuilder.from((Number) vFrom);
            } else {
                queryBuilder.from((String) vFrom);
            }
        }

        if (vTo != null) {
            if (vTo instanceof Number) {
                queryBuilder.to((Number) vTo);
            } else {
                queryBuilder.to((String) vTo);
            }
        }

        if (includeUpper != null) {
            queryBuilder.includeUpper(includeUpper);
        }

        if (includeLower != null) {
            queryBuilder.includeLower(includeLower);
        }

        if (unit != null) {
            queryBuilder.unit(unit);
        }

        if (geoDistance != null) {
            queryBuilder.geoDistance(geoDistance);
        }

        if (optimizeBbox != null) {
            queryBuilder.optimizeBbox(optimizeBbox);
        }

        if (validationMethod != null) {
            // if validation method is set explicitly ignore deprecated coerce/ignore malformed fields if any
            queryBuilder.setValidationMethod(validationMethod);
        } else {
            queryBuilder.setValidationMethod(GeoValidationMethod.infer(coerce, ignoreMalformed));
        }
        queryBuilder.ignoreUnmapped(ignoreUnmapped);
        return Optional.of(queryBuilder);
    }

    @Override
    protected boolean doEquals(GeoDistanceRangeQueryBuilder other) {
        return ((Objects.equals(fieldName, other.fieldName)) &&
                (Objects.equals(point, other.point)) &&
                (Objects.equals(from, other.from)) &&
                (Objects.equals(to, other.to)) &&
                (Objects.equals(includeUpper, other.includeUpper)) &&
                (Objects.equals(includeLower, other.includeLower)) &&
                (Objects.equals(geoDistance, other.geoDistance)) &&
                (Objects.equals(optimizeBbox, other.optimizeBbox)) &&
                (Objects.equals(validationMethod, other.validationMethod))) &&
                Objects.equals(ignoreUnmapped, other.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, point, from, to, includeUpper, includeLower, geoDistance, optimizeBbox, validationMethod,
                ignoreUnmapped);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
