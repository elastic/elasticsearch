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
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Objects;

/**
 * A Query that matches documents within an range of terms.
 */
public class RangeQueryBuilder extends AbstractQueryBuilder<RangeQueryBuilder> implements MultiTermQueryBuilder {
    public static final String NAME = "range";

    public static final boolean DEFAULT_INCLUDE_UPPER = true;
    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    public static final ParseField LTE_FIELD = new ParseField("lte");
    public static final ParseField GTE_FIELD = new ParseField("gte");
    public static final ParseField FROM_FIELD = new ParseField("from");
    public static final ParseField TO_FIELD = new ParseField("to");
    private static final ParseField INCLUDE_LOWER_FIELD = new ParseField("include_lower");
    private static final ParseField INCLUDE_UPPER_FIELD = new ParseField("include_upper");
    public static final ParseField GT_FIELD = new ParseField("gt");
    public static final ParseField LT_FIELD = new ParseField("lt");
    private static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");
    private static final ParseField FORMAT_FIELD = new ParseField("format");
    private static final ParseField RELATION_FIELD = new ParseField("relation");

    private final String fieldName;
    private Object from;
    private Object to;
    private ZoneId timeZone;
    private boolean includeLower = DEFAULT_INCLUDE_LOWER;
    private boolean includeUpper = DEFAULT_INCLUDE_UPPER;
    private String format;
    private ShapeRelation relation;

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param fieldName The field name
     */
    public RangeQueryBuilder(String fieldName) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        this.fieldName = fieldName;
    }

    /**
     * Read from a stream.
     */
    public RangeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        from = in.readGenericValue();
        to = in.readGenericValue();
        includeLower = in.readBoolean();
        includeUpper = in.readBoolean();
        timeZone = in.readOptionalZoneId();
        format = in.readOptionalString();
        String relationString = in.readOptionalString();
        if (relationString != null) {
            relation = ShapeRelation.getRelationByName(relationString);
            if (relation != null && !isRelationAllowed(relation)) {
                throw new IllegalArgumentException(
                    "[range] query does not support relation [" + relationString + "]");
            }
        }
    }

    private boolean isRelationAllowed(ShapeRelation relation) {
        return relation == ShapeRelation.INTERSECTS
            || relation == ShapeRelation.CONTAINS
            || relation == ShapeRelation.WITHIN;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeGenericValue(this.from);
        out.writeGenericValue(this.to);
        out.writeBoolean(this.includeLower);
        out.writeBoolean(this.includeUpper);
        out.writeOptionalZoneId(timeZone);
        out.writeOptionalString(format);
        String relationString = null;
        if (this.relation != null) {
            relationString = this.relation.getRelationName();
        }
        out.writeOptionalString(relationString);
    }

    /**
     * Get the field name for this query.
     */
    @Override
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     * In case lower bound is assigned to a string, we internally convert it to a {@link BytesRef} because
     * in {@link RangeQueryBuilder} field are later parsed as {@link BytesRef} and we need internal representation
     * of query to be equal regardless of whether it was created from XContent or via Java API.
     */
    public RangeQueryBuilder from(Object from, boolean includeLower) {
        this.from = maybeConvertToBytesRef(from);
        this.includeLower = includeLower;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(Object from) {
        return from(from, this.includeLower);
    }

    /**
     * Gets the lower range value for this query.
     */
    public Object from() {
        return maybeConvertToString(this.from);
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gt(Object from) {
        return from(from, false);
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gte(Object from) {
        return from(from, true);
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(Object to, boolean includeUpper) {
        this.to = maybeConvertToBytesRef(to);
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(Object to) {
        return to(to, this.includeUpper);
    }

    /**
     * Gets the upper range value for this query.
     * In case upper bound is assigned to a string, we internally convert it to a {@link BytesRef} because
     * in {@link RangeQueryBuilder} field are later parsed as {@link BytesRef} and we need internal representation
     * of query to be equal regardless of whether it was created from XContent or via Java API.
     */
    public Object to() {
        return maybeConvertToString(this.to);
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lt(Object to) {
        return to(to, false);
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lte(Object to) {
        return to(to, true);
    }

    /**
     * Should the lower bound be included or not. Defaults to {@code true}.
     */
    public RangeQueryBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Gets the includeLower flag for this query.
     */
    public boolean includeLower() {
        return this.includeLower;
    }

    /**
     * Should the upper bound be included or not. Defaults to {@code true}.
     */
    public RangeQueryBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * Gets the includeUpper flag for this query.
     */
    public boolean includeUpper() {
        return this.includeUpper;
    }

    /**
     * In case of date field, we can adjust the from/to fields using a timezone
     */
    public RangeQueryBuilder timeZone(String timeZone) {
        if (timeZone == null) {
            throw new IllegalArgumentException("timezone cannot be null");
        }
        try {
            this.timeZone = ZoneId.of(timeZone);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    /**
     * In case of date field, gets the from/to fields timezone adjustment
     */
    public String timeZone() {
        return this.timeZone == null ? null : this.timeZone.getId();
    }

    ZoneId getDateTimeZone() { // for testing
        return timeZone;
    }

    /**
     * In case of format field, we can parse the from/to fields using this time format
     */
    public RangeQueryBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("format cannot be null");
        }
        // this just ensure that the pattern is actually valid, no need to keep it here
        DateFormatter.forPattern(format);
        this.format = format;
        return this;
    }

    /**
     * Gets the format field to parse the from/to fields
     */
    public String format() {
        return format;
    }

    DateMathParser getForceDateParser() { // pkg private for testing
        if (Strings.hasText(format)) {
            return DateFormatter.forPattern(this.format).toDateMathParser();
        }
        return null;
    }

    public ShapeRelation relation() {
        return this.relation;
    }

    public RangeQueryBuilder relation(String relation) {
        if (relation == null) {
            throw new IllegalArgumentException("relation cannot be null");
        }
        this.relation = ShapeRelation.getRelationByName(relation);
        if (this.relation == null) {
            throw new IllegalArgumentException(relation + " is not a valid relation");
        }
        if (!isRelationAllowed(this.relation)) {
            throw new IllegalArgumentException("[range] query does not support relation [" + relation + "]");
        }
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(FROM_FIELD.getPreferredName(), maybeConvertToString(this.from));
        builder.field(TO_FIELD.getPreferredName(), maybeConvertToString(this.to));
        builder.field(INCLUDE_LOWER_FIELD.getPreferredName(), includeLower);
        builder.field(INCLUDE_UPPER_FIELD.getPreferredName(), includeUpper);
        if (timeZone != null) {
            builder.field(TIME_ZONE_FIELD.getPreferredName(), timeZone.getId());
        }
        if (Strings.hasText(format)) {
            builder.field(FORMAT_FIELD.getPreferredName(), format);
        }
        if (relation != null) {
            builder.field(RELATION_FIELD.getPreferredName(), relation.getRelationName());
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static RangeQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        Object from = null;
        Object to = null;
        boolean includeLower = RangeQueryBuilder.DEFAULT_INCLUDE_LOWER;
        boolean includeUpper = RangeQueryBuilder.DEFAULT_INCLUDE_UPPER;
        String timeZone = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String format = null;
        String relation = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            from = maybeConvertToBytesRef(parser.objectBytes());
                        } else if (TO_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            to = maybeConvertToBytesRef(parser.objectBytes());
                        } else if (INCLUDE_LOWER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            includeLower = parser.booleanValue();
                        } else if (INCLUDE_UPPER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            includeUpper = parser.booleanValue();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (GT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            from = maybeConvertToBytesRef(parser.objectBytes());
                            includeLower = false;
                        } else if (GTE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            from = maybeConvertToBytesRef(parser.objectBytes());
                            includeLower = true;
                        } else if (LT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            to = maybeConvertToBytesRef(parser.objectBytes());
                            includeUpper = false;
                        } else if (LTE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            to = maybeConvertToBytesRef(parser.objectBytes());
                            includeUpper = true;
                        } else if (TIME_ZONE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            timeZone = parser.text();
                        } else if (FORMAT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            format = parser.text();
                        } else if (RELATION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            relation = parser.text();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[range] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                    throw new ParsingException(parser.getTokenLocation(), "[range] query does not support [" + currentFieldName + "]");
            }
        }

        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(fieldName);
        rangeQuery.from(from);
        rangeQuery.to(to);
        rangeQuery.includeLower(includeLower);
        rangeQuery.includeUpper(includeUpper);
        if (timeZone != null) {
            rangeQuery.timeZone(timeZone);
        }
        rangeQuery.boost(boost);
        rangeQuery.queryName(queryName);
        if (format != null) {
            rangeQuery.format(format);
        }
        if (relation != null) {
            rangeQuery.relation(relation);
        }
        return rangeQuery;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    // Overridable for testing only
    protected MappedFieldType.Relation getRelation(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryShardContext shardContext = queryRewriteContext.convertToShardContext();
        // If the context is null we are not on the shard and cannot
        // rewrite so just pretend there is an intersection so that the rewrite is a noop
        if (shardContext == null || shardContext.getIndexReader() == null) {
            return MappedFieldType.Relation.INTERSECTS;
        }
        final MapperService mapperService = shardContext.getMapperService();
        final MappedFieldType fieldType = mapperService.fieldType(fieldName);
        if (fieldType == null) {
            // no field means we have no values
            return MappedFieldType.Relation.DISJOINT;
        } else {
            DateMathParser dateMathParser = getForceDateParser();
            return fieldType.isFieldWithinQuery(shardContext.getIndexReader(), from, to, includeLower,
                    includeUpper, timeZone, dateMathParser, queryRewriteContext);
        }
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        // Percolator queries get rewritten and pre-processed at index time.
        // If a range query has a date range using 'now' and 'now' gets resolved at index time then
        // the pre-processing uses that to pre-process. This can then lead to mismatches at query time.
        if (queryRewriteContext.convertNowRangeToMatchAll()) {
            if ((from() != null && from().toString().contains("now")) ||
                (to() != null && to().toString().contains("now"))) {
                return new MatchAllQueryBuilder();
            }
        }

        final MappedFieldType.Relation relation = getRelation(queryRewriteContext);
        switch (relation) {
        case DISJOINT:
            return new MatchNoneQueryBuilder();
        case WITHIN:
            if (from != null || to != null || format != null || timeZone != null) {
                RangeQueryBuilder newRangeQuery = new RangeQueryBuilder(fieldName);
                newRangeQuery.from(null);
                newRangeQuery.to(null);
                newRangeQuery.format = null;
                newRangeQuery.timeZone = null;
                return newRangeQuery;
            } else {
                return this;
            }
        case INTERSECTS:
            return this;
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (from == null && to == null) {
            /**
             * Open bounds on both side, we can rewrite to an exists query
             * if the {@link FieldNamesFieldMapper} is enabled.
             */
            final FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType =
                (FieldNamesFieldMapper.FieldNamesFieldType) context.getMapperService().fieldType(FieldNamesFieldMapper.NAME);
            if (fieldNamesFieldType == null) {
                return new MatchNoDocsQuery("No mappings yet");
            }
            // Exists query would fail if the fieldNames field is disabled.
            if (fieldNamesFieldType.isEnabled()) {
                return ExistsQueryBuilder.newFilter(context, fieldName);
            }
        }
        Query query = null;
        MappedFieldType mapper = context.fieldMapper(this.fieldName);
        if (mapper != null) {
            DateMathParser forcedDateParser = getForceDateParser();
            query = mapper.rangeQuery(
                    from, to, includeLower, includeUpper,
                    relation, timeZone, forcedDateParser, context);
        } else {
            if (timeZone != null) {
                throw new QueryShardException(context, "[range] time_zone can not be applied to non unmapped field ["
                        + fieldName + "]");
            }
        }

        if (query == null) {
            query = new TermRangeQuery(this.fieldName,
                    BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to),
                    includeLower, includeUpper);
        }
        return query;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, from, to, timeZone, includeLower, includeUpper, format);
    }

    @Override
    protected boolean doEquals(RangeQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
               Objects.equals(from, other.from) &&
               Objects.equals(to, other.to) &&
               Objects.equals(timeZone, other.timeZone) &&
               Objects.equals(includeLower, other.includeLower) &&
               Objects.equals(includeUpper, other.includeUpper) &&
               Objects.equals(format, other.format);
    }
}
