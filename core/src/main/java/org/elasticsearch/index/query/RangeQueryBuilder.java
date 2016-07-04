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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.LegacyDateFieldMapper;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * A Query that matches documents within an range of terms.
 */
public class RangeQueryBuilder extends AbstractQueryBuilder<RangeQueryBuilder> implements MultiTermQueryBuilder {
    public static final String NAME = "range";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);

    public static final boolean DEFAULT_INCLUDE_UPPER = true;
    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    private static final ParseField FIELDDATA_FIELD = new ParseField("fielddata").withAllDeprecated("[no replacement]");
    private static final ParseField NAME_FIELD = new ParseField("_name")
            .withAllDeprecated("query name is not supported in short version of range query");
    private static final ParseField LTE_FIELD = new ParseField("lte", "le");
    private static final ParseField GTE_FIELD = new ParseField("gte", "ge");
    private static final ParseField FROM_FIELD = new ParseField("from");
    private static final ParseField TO_FIELD = new ParseField("to");
    private static final ParseField INCLUDE_LOWER_FIELD = new ParseField("include_lower");
    private static final ParseField INCLUDE_UPPER_FIELD = new ParseField("include_upper");
    private static final ParseField GT_FIELD = new ParseField("gt");
    private static final ParseField LT_FIELD = new ParseField("lt");
    private static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");
    private static final ParseField FORMAT_FIELD = new ParseField("format");

    private final String fieldName;

    private Object from;

    private Object to;

    private DateTimeZone timeZone;

    private boolean includeLower = DEFAULT_INCLUDE_LOWER;

    private boolean includeUpper = DEFAULT_INCLUDE_UPPER;

    private FormatDateTimeFormatter format;

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
        timeZone = in.readOptionalTimeZone();
        String formatString = in.readOptionalString();
        if (formatString != null) {
            format = Joda.forPattern(formatString);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeGenericValue(this.from);
        out.writeGenericValue(this.to);
        out.writeBoolean(this.includeLower);
        out.writeBoolean(this.includeUpper);
        out.writeOptionalTimeZone(timeZone);
        String formatString = null;
        if (this.format != null) {
            formatString = this.format.format();
        }
        out.writeOptionalString(formatString);
    }

    /**
     * Get the field name for this query.
     */
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
        this.from = convertToBytesRefIfString(from);
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
        return convertToStringIfBytesRef(this.from);
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
        this.to = convertToBytesRefIfString(to);
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
        return convertToStringIfBytesRef(this.to);
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
     * Should the lower bound be included or not. Defaults to <tt>true</tt>.
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
     * Should the upper bound be included or not. Defaults to <tt>true</tt>.
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
        this.timeZone = DateTimeZone.forID(timeZone);
        return this;
    }

    /**
     * In case of date field, gets the from/to fields timezone adjustment
     */
    public String timeZone() {
        return this.timeZone == null ? null : this.timeZone.getID();
    }

    /**
     * In case of format field, we can parse the from/to fields using this time format
     */
    public RangeQueryBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("format cannot be null");
        }
        this.format = Joda.forPattern(format);
        return this;
    }

    /**
     * Gets the format field to parse the from/to fields
     */
    public String format() {
        return this.format == null ? null : this.format.format();
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(FROM_FIELD.getPreferredName(), convertToStringIfBytesRef(this.from));
        builder.field(TO_FIELD.getPreferredName(), convertToStringIfBytesRef(this.to));
        builder.field(INCLUDE_LOWER_FIELD.getPreferredName(), includeLower);
        builder.field(INCLUDE_UPPER_FIELD.getPreferredName(), includeUpper);
        if (timeZone != null) {
            builder.field(TIME_ZONE_FIELD.getPreferredName(), timeZone.getID());
        }
        if (format != null) {
            builder.field(FORMAT_FIELD.getPreferredName(), format.format());
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static Optional<RangeQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        Object from = null;
        Object to = null;
        boolean includeLower = RangeQueryBuilder.DEFAULT_INCLUDE_LOWER;
        boolean includeUpper = RangeQueryBuilder.DEFAULT_INCLUDE_UPPER;
        String timeZone = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String format = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (parseContext.getParseFieldMatcher().match(currentFieldName, FROM_FIELD)) {
                            from = parser.objectBytes();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, TO_FIELD)) {
                            to = parser.objectBytes();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INCLUDE_LOWER_FIELD)) {
                            includeLower = parser.booleanValue();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INCLUDE_UPPER_FIELD)) {
                            includeUpper = parser.booleanValue();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                            boost = parser.floatValue();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, GT_FIELD)) {
                            from = parser.objectBytes();
                            includeLower = false;
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, GTE_FIELD)) {
                            from = parser.objectBytes();
                            includeLower = true;
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, LT_FIELD)) {
                            to = parser.objectBytes();
                            includeUpper = false;
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, LTE_FIELD)) {
                            to = parser.objectBytes();
                            includeUpper = true;
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, TIME_ZONE_FIELD)) {
                            timeZone = parser.text();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, FORMAT_FIELD)) {
                            format = parser.text();
                        } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[range] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, FIELDDATA_FIELD)) {
                    // ignore
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[range] query does not support [" + currentFieldName + "]");
                }
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
        return Optional.of(rangeQuery);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    // Overridable for testing only
    protected MappedFieldType.Relation getRelation(QueryRewriteContext queryRewriteContext) throws IOException {
        IndexReader reader = queryRewriteContext.getIndexReader();
        // If the reader is null we are not on the shard and cannot
        // rewrite so just pretend there is an intersection so that the rewrite is a noop
        if (reader == null) {
            return MappedFieldType.Relation.INTERSECTS;
        }
        final MapperService mapperService = queryRewriteContext.getMapperService();
        final MappedFieldType fieldType = mapperService.fullName(fieldName);
        if (fieldType == null) {
            // no field means we have no values
            return MappedFieldType.Relation.DISJOINT;
        } else {
            DateMathParser dateMathParser = format == null ? null : new DateMathParser(format);
            return fieldType.isFieldWithinQuery(queryRewriteContext.getIndexReader(), from, to, includeLower,
                    includeUpper, timeZone, dateMathParser);
        }
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        final MappedFieldType.Relation relation = getRelation(queryRewriteContext);
        switch (relation) {
        case DISJOINT:
            return new MatchNoneQueryBuilder();
        case WITHIN:
            if (from != null || to != null) {
                RangeQueryBuilder newRangeQuery = new RangeQueryBuilder(fieldName);
                newRangeQuery.from(null);
                newRangeQuery.to(null);
                newRangeQuery.format = format;
                newRangeQuery.timeZone = timeZone;
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
        Query query = null;
        MappedFieldType mapper = context.fieldMapper(this.fieldName);
        if (mapper != null) {
            if (mapper instanceof LegacyDateFieldMapper.DateFieldType) {
                DateMathParser forcedDateParser = null;
                if (this.format  != null) {
                    forcedDateParser = new DateMathParser(this.format);
                }
                query = ((LegacyDateFieldMapper.DateFieldType) mapper).rangeQuery(from, to, includeLower, includeUpper,
                        timeZone, forcedDateParser);
            } else if (mapper instanceof DateFieldMapper.DateFieldType) {
                DateMathParser forcedDateParser = null;
                if (this.format  != null) {
                    forcedDateParser = new DateMathParser(this.format);
                }
                query = ((DateFieldMapper.DateFieldType) mapper).rangeQuery(from, to, includeLower, includeUpper,
                        timeZone, forcedDateParser);
            } else  {
                if (timeZone != null) {
                    throw new QueryShardException(context, "[range] time_zone can not be applied to non date field ["
                            + fieldName + "]");
                }
                //LUCENE 4 UPGRADE Mapper#rangeQuery should use bytesref as well?
                query = mapper.rangeQuery(from, to, includeLower, includeUpper);
            }
        } else {
            if (timeZone != null) {
                throw new QueryShardException(context, "[range] time_zone can not be applied to non unmapped field ["
                        + fieldName + "]");
            }
        }

        if (query == null) {
            query = new TermRangeQuery(this.fieldName, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to), includeLower, includeUpper);
        }
        return query;
    }

    @Override
    protected int doHashCode() {
        String timeZoneId = timeZone == null ? null : timeZone.getID();
        String formatString = format == null ? null : format.format();
        return Objects.hash(fieldName, from, to, timeZoneId, includeLower, includeUpper, formatString);
    }

    @Override
    protected boolean doEquals(RangeQueryBuilder other) {
        String timeZoneId = timeZone == null ? null : timeZone.getID();
        String formatString = format == null ? null : format.format();
        return Objects.equals(fieldName, other.fieldName) &&
               Objects.equals(from, other.from) &&
               Objects.equals(to, other.to) &&
               Objects.equals(timeZoneId, other.timeZone()) &&
               Objects.equals(includeLower, other.includeLower) &&
               Objects.equals(includeUpper, other.includeUpper) &&
               Objects.equals(formatString, other.format());
    }
}
