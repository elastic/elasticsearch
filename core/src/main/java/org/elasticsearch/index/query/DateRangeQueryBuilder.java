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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class DateRangeQueryBuilder extends AbstractQueryBuilder<DateRangeQueryBuilder> {
    public static final String NAME = "daterange";

    public static final boolean DEFAULT_INCLUDE_UPPER = true;
    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    private static final ParseField FIELDDATA_FIELD = new ParseField("fielddata").withAllDeprecated("[no replacement]");
    private static final ParseField NAME_FIELD = new ParseField("_name")
        .withAllDeprecated("query name is not supported in short version of range query");
    public static final ParseField LTE_FIELD = new ParseField("lte");
    public static final ParseField LT_FIELD = new ParseField("lt");
    public static final ParseField GTE_FIELD = new ParseField("gte");
    public static final ParseField GT_FIELD = new ParseField("gt");
    private static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");
    private static final ParseField FORMAT_FIELD = new ParseField("format");
    private static final ParseField RELATION_FIELD = new ParseField("relation");

    private final String fieldName;

    private DateTime lt;

    private DateTime lte;

    private DateTime gt;

    private DateTime gte;

    private DateTimeZone timeZone;

    private boolean includeLower = DEFAULT_INCLUDE_LOWER;

    private boolean includeUpper = DEFAULT_INCLUDE_UPPER;

    private FormatDateTimeFormatter format = Joda.forPattern(
        "strict_date_optional_time||epoch_millis", Locale.ROOT);

    private ShapeRelation relation;

    /**
     * A Query that matches documents within an range of dates.
     *
     * @param fieldName The field name
     */
    public DateRangeQueryBuilder(String fieldName) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        this.fieldName = fieldName;
    }

    /**
     * Read from a stream.
     */
    public DateRangeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        lt = convertToDateIfString(in.readString());
        lte = convertToDateIfString(in.readString());
        gt = convertToDateIfString(in.readString());
        gte = convertToDateIfString(in.readString());
        timeZone = in.readOptionalTimeZone();
        String formatString = in.readOptionalString();
        if (formatString != null) {
            format = Joda.forPattern(formatString);
        }
        if (in.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            String relationString = in.readOptionalString();
            if (relationString != null) {
                relation = ShapeRelation.getRelationByName(relationString);
            }
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeString(this.lt.toString());
        out.writeString(this.lte.toString());
        out.writeString(this.gt.toString());
        out.writeString(this.gte.toString());
        out.writeOptionalTimeZone(timeZone);
        String formatString = null;
        if (this.format != null) {
            formatString = this.format.format();
        }
        out.writeOptionalString(formatString);
        if (out.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            String relationString = null;
            if (this.relation != null) {
                relationString = this.relation.getRelationName();
            }
            out.writeOptionalString(relationString);
        }
    }

    /**
     * Get the field name for this query.
     */
    public String fieldName() {
        return this.fieldName;
    }

    public DateTime gt() {
        return this.gt;
    }

    public DateTime gte() {
        return this.gte;
    }


    public DateRangeQueryBuilder gt(DateTime gt) {
        this.gt = gt;
        this.includeLower = false;
        return this;
    }


    public DateRangeQueryBuilder gt(String gt) {
        this.gt = convertToDateIfString(gt);
        this.includeLower = false;
        return this;
    }


    public DateRangeQueryBuilder gte(DateTime gte) {
        this.gte = gte;
        this.includeLower = true;
        return this;
    }


    public DateRangeQueryBuilder gte(String gte) {
        this.gte = convertToDateIfString(gte);
        this.includeLower = true;
        return this;
    }

    public DateTime lt() {
        return this.lt;
    }

    public DateTime lte() {
        return this.lte;
    }


    public DateRangeQueryBuilder lt(DateTime lt) {
        this.lt = lt;
        this.includeUpper = false;
        return this;
    }


    public DateRangeQueryBuilder lt(String lt) {
        this.lt = convertToDateIfString(lt);
        this.includeUpper = false;
        return this;
    }


    public DateRangeQueryBuilder lte(DateTime lte) {
        this.lte = lte;
        this.includeUpper = true;
        return this;
    }


    public DateRangeQueryBuilder lte(String lte) {
        this.lte = convertToDateIfString(lte);
        this.includeUpper = true;
        return this;
    }

    public DateRangeQueryBuilder timeZone(String timeZone) {
        if (timeZone == null) {
            throw new IllegalArgumentException("timezone cannot be null");
        }
        this.timeZone = DateTimeZone.forID(timeZone);
        return this;
    }

    public String timeZone() {
        return this.timeZone == null ? null : this.timeZone.getID();
    }

    DateTimeZone getDateTimeZone() { // for testing
        return timeZone;
    }


    public DateRangeQueryBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("format cannot be null");
        }
        this.format = Joda.forPattern(format);
        return this;
    }


    public String format() {
        return this.format == null ? null : this.format.format();
    }

    DateMathParser getForceDateParser() { // pkg private for testing
        if (this.format != null) {
            return new DateMathParser(this.format);
        } else {
            return new DateMathParser(Joda.forPattern(
                "strict_date_optional_time||epoch_millis", Locale.ROOT));
        }
    }

    public ShapeRelation relation() {
        return this.relation;
    }

    public DateRangeQueryBuilder relation(String relation) {
        if (relation == null) {
            throw new IllegalArgumentException("relation cannot be null");
        }
        this.relation = ShapeRelation.getRelationByName(relation);
        if (this.relation == null) {
            throw new IllegalArgumentException(relation + " is not a valid relation");
        }
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(LT_FIELD.getPreferredName(), this.lt);
        builder.field(LTE_FIELD.getPreferredName(), this.lte);
        builder.field(GT_FIELD.getPreferredName(), this.gt);
        builder.field(GTE_FIELD.getPreferredName(), this.gte);
        if (timeZone != null) {
            builder.field(TIME_ZONE_FIELD.getPreferredName(), timeZone.getID());
        }
        if (format != null) {
            builder.field(FORMAT_FIELD.getPreferredName(), format.format());
        }
        if (relation != null) {
            builder.field(RELATION_FIELD.getPreferredName(), relation.getRelationName());
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static DateRangeQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        String gt = null;
        String gte = null;
        String lt = null;
        String lte = null;
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
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                            boost = parser.floatValue();
                        } else if (GT_FIELD.match(currentFieldName)) {
                            gt = parser.text();
                        } else if (GTE_FIELD.match(currentFieldName)) {
                            gte = parser.text();
                        } else if (LT_FIELD.match(currentFieldName)) {
                            lt = parser.text();
                        } else if (LTE_FIELD.match(currentFieldName)) {
                            lte = parser.text();
                        } else if (TIME_ZONE_FIELD.match(currentFieldName)) {
                            timeZone = parser.text();
                        } else if (FORMAT_FIELD.match(currentFieldName)) {
                            format = parser.text();
                        } else if (RELATION_FIELD.match(currentFieldName)) {
                            relation = parser.text();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                "[daterange] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (NAME_FIELD.match(currentFieldName)) {
                    queryName = parser.text();
                } else if (FIELDDATA_FIELD.match(currentFieldName)) {
                    // ignore
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[daterange] query does not support [" + currentFieldName + "]");
                }
            }
        }

        DateRangeQueryBuilder rangeQuery = new DateRangeQueryBuilder(fieldName);
        rangeQuery.gt(gt);
        rangeQuery.gte(gte);
        rangeQuery.lt(lt);
        rangeQuery.lte(lte);
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
            return fieldType.isFieldWithinQuery(queryRewriteContext.getIndexReader(), gte != null ? gte : gt, lte != null ? lte : lt, includeLower,
                includeUpper, timeZone, dateMathParser, queryRewriteContext);
        }
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        final MappedFieldType.Relation relation = getRelation(queryRewriteContext);
        switch (relation) {
            case DISJOINT:
                return new MatchNoneQueryBuilder();
            case WITHIN:
                if (lt != null || lte != null || gt != null || gte != null || format != null || timeZone != null) {
                    DateRangeQueryBuilder newDateRangeQuery = new DateRangeQueryBuilder(fieldName);
                    newDateRangeQuery.gte = null;
                    newDateRangeQuery.gt = null;
                    newDateRangeQuery.lt = null;
                    newDateRangeQuery.lte = null;
                    newDateRangeQuery.format = null;
                    newDateRangeQuery.timeZone = null;
                    return newDateRangeQuery;
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
        DateMathParser dateParser = getForceDateParser();

        long l, u;
        if (lte != null) {
            l = dateParser.parse(lte.toString(format()), context::nowInMillis, !includeLower, timeZone);
            if (includeLower == false) {
                ++l;
            }
        } else if (lt != null) {
            l = dateParser.parse(lt.toString(format()), context::nowInMillis, !includeLower, timeZone);
            if (includeLower == false) {
                ++l;
            }
        } else {
            l = Long.MIN_VALUE;
        }

        if (gte != null) {
            u = dateParser.parse(gte.toString(format()), context::nowInMillis, includeUpper, timeZone);
            if (includeUpper == false) {
                --u;
            }
        } else if (gt != null) {
            u = dateParser.parse(gt.toString(format()), context::nowInMillis, includeUpper, timeZone);
            if (includeUpper == false) {
                --u;
            }
        } else {
            u = Long.MAX_VALUE;
        }

        query = LongPoint.newRangeQuery(fieldName, l, u);

        return query;
    }

    @Override
    protected int doHashCode() {
        String timeZoneId = timeZone == null ? null : timeZone.getID();
        String formatString = format == null ? null : format.format();
        return Objects.hash(fieldName, lt, lte, gt, gte, timeZoneId, formatString);
    }

    @Override
    protected boolean doEquals(DateRangeQueryBuilder other) {
        String timeZoneId = timeZone == null ? null : timeZone.getID();
        String formatString = format == null ? null : format.format();
        return Objects.equals(fieldName, other.fieldName) &&
            Objects.equals(lte, other.lte) &&
            Objects.equals(lt, other.lt) &&
            Objects.equals(gte, other.gte) &&
            Objects.equals(gt, other.gt) &&
            Objects.equals(timeZoneId, other.timeZone()) &&
            Objects.equals(formatString, other.format());
    }

    private DateTime convertToDateIfString(String dateTime) {
        if (format == null)
            throw new IllegalArgumentException("Cannot convert from String if format is null");

        DateTime date = format.parser().parseDateTime(dateTime);
        return date;
    }
}
