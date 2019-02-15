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

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.unit.TimeValue;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query to boost scores based on their proximity to the given origin
 */
public class DistanceFeatureQueryBuilder extends AbstractQueryBuilder<DistanceFeatureQueryBuilder> {
    public static final String NAME = "distance_feature";

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField ORIGIN_FIELD = new ParseField("origin");
    private static final ParseField PIVOT_FIELD = new ParseField("pivot");

    private final String field;
    private final Object origin; //type of Long, String or GeoPoint
    private final Object pivot; //type of Long or String

    private static final ConstructingObjectParser<DistanceFeatureQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "distance_feature", false,
        args -> new DistanceFeatureQueryBuilder((String) args[0], args[1], args[2])
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        // origin: number for long fields; number or string for date fields; string, array, object for geo fields
        PARSER.declareField(constructorArg(), DistanceFeatureQueryBuilder::originFromXContent,
            ORIGIN_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_STRING_OR_NUMBER);
        //pivot: number for long fields; string for dates or geo fields
        PARSER.declareField(constructorArg(), DistanceFeatureQueryBuilder::pivotFromXContent,
            PIVOT_FIELD, ObjectParser.ValueType.LONG);
        declareStandardFields(PARSER);
    }

    public DistanceFeatureQueryBuilder(String field, Object origin, Object pivot) {
        this.field = field;
        this.origin = origin;
        this.pivot = pivot;
    }

    public static DistanceFeatureQueryBuilder fromXContent(XContentParser parser) {
        try {
            DistanceFeatureQueryBuilder builder = PARSER.apply(parser, null);
            return builder;
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    public static Object originFromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            return parser.longValue();
        } else if(parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return GeoUtils.parseGeoPoint(parser);
        } else { // if currentToken equals to START_ARRAY)
            return GeoUtils.parseGeoPoint(parser);
        }
    }

    public static Object pivotFromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            return parser.longValue();
        } else { // if currentToken equals to VALUE_STRING
            return parser.text();
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(ORIGIN_FIELD.getPreferredName(), origin);
        builder.field(PIVOT_FIELD.getPreferredName(), pivot);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public DistanceFeatureQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        origin = in.readGenericValue();
        pivot = in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeGenericValue(origin);
        out.writeGenericValue(pivot);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("Can't run [" + NAME + "] query on unmapped fields!");
        }
        if (fieldType instanceof DateFieldType) {
            long originLong = (origin instanceof Long) ? (Long) origin :
                ((DateFieldType) fieldType).parseToLong(origin, true, null, null, context);
            TimeValue val = TimeValue.parseTimeValue((String)pivot, TimeValue.timeValueHours(24),
                DistanceFeatureQueryBuilder.class.getSimpleName() + ".pivot");
            long pivotLong = val.getMillis();
            return LongPoint.newDistanceFeatureQuery(field, boost, originLong, pivotLong);
        } else if ((fieldType instanceof NumberFieldType) && (fieldType.typeName() == NumberType.LONG.typeName())) {
            long originLong = (Long) origin;
            long pivotLong = (Long) pivot;
            return LongPoint.newDistanceFeatureQuery(field, boost, originLong, pivotLong);
        } else if (fieldType instanceof GeoPointFieldType) {
            GeoPoint originGeoPoint = (origin instanceof GeoPoint)? (GeoPoint) origin : GeoUtils.parseFromString((String) origin);
            double pivotDouble = DistanceUnit.DEFAULT.parse((String) pivot, DistanceUnit.DEFAULT);
            return LatLonPoint.newDistanceFeatureQuery(field, boost, originGeoPoint.lat(), originGeoPoint.lon(), pivotDouble);
        }
        throw new IllegalArgumentException(
            "Illegal data type! ["+ NAME + "] query can only be run on a long, date or geo-point data type!");
    }

    public String fieldName() {
        return field;
    }

    public Object origin() {
        return origin;
    }

    public Object pivot() {
        return pivot;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, origin, pivot);
    }

    @Override
    protected boolean doEquals(DistanceFeatureQueryBuilder other) {
        return this.field.equals(other.field) && Objects.equals(this.origin, other.origin) && Objects.equals(this.pivot, other.pivot);
    }
}
