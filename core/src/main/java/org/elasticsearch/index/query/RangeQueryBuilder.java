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
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;

/**
 * A Query that matches documents within an range of terms.
 */
public class RangeQueryBuilder extends AbstractQueryBuilder<RangeQueryBuilder> implements MultiTermQueryBuilder<RangeQueryBuilder> {

    public static final boolean DEFAULT_INCLUDE_UPPER = true;

    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    public static final String NAME = "range";

    private final String fieldName;

    private Object from;

    private Object to;

    private DateTimeZone timeZone;

    private boolean includeLower = DEFAULT_INCLUDE_LOWER;

    private boolean includeUpper = DEFAULT_INCLUDE_UPPER;

    private FormatDateTimeFormatter format;

    static final RangeQueryBuilder PROTOTYPE = new RangeQueryBuilder("field");

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
     * Get the field name for this query.
     */
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     * In case lower bound is assigned to a string, we internally convert it to a {@link BytesRef} because
     * in {@link RangeQueryParser} field are later parsed as {@link BytesRef} and we need internal representation
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
     * in {@link RangeQueryParser} field are later parsed as {@link BytesRef} and we need internal representation
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
        builder.field(RangeQueryParser.FROM_FIELD.getPreferredName(), convertToStringIfBytesRef(this.from));
        builder.field(RangeQueryParser.TO_FIELD.getPreferredName(), convertToStringIfBytesRef(this.to));
        builder.field(RangeQueryParser.INCLUDE_LOWER_FIELD.getPreferredName(), includeLower);
        builder.field(RangeQueryParser.INCLUDE_UPPER_FIELD.getPreferredName(), includeUpper);
        if (timeZone != null) {
            builder.field(RangeQueryParser.TIME_ZONE_FIELD.getPreferredName(), timeZone.getID());
        }
        if (format != null) {
            builder.field(RangeQueryParser.FORMAT_FIELD.getPreferredName(), format.format());
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query query = null;
        MappedFieldType mapper = context.fieldMapper(this.fieldName);
        if (mapper != null) {
            if (mapper instanceof DateFieldMapper.DateFieldType) {
                DateMathParser forcedDateParser = null;
                if (this.format  != null) {
                    forcedDateParser = new DateMathParser(this.format);
                }
                query = ((DateFieldMapper.DateFieldType) mapper).rangeQuery(from, to, includeLower, includeUpper, timeZone, forcedDateParser);
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
    protected RangeQueryBuilder doReadFrom(StreamInput in) throws IOException {
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(in.readString());
        rangeQueryBuilder.from = in.readGenericValue();
        rangeQueryBuilder.to = in.readGenericValue();
        rangeQueryBuilder.includeLower = in.readBoolean();
        rangeQueryBuilder.includeUpper = in.readBoolean();
        String timeZoneId = in.readOptionalString();
        if (timeZoneId != null) {
            rangeQueryBuilder.timeZone = DateTimeZone.forID(timeZoneId);
        }
        String formatString = in.readOptionalString();
        if (formatString != null) {
            rangeQueryBuilder.format = Joda.forPattern(formatString);
        }
        return rangeQueryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeGenericValue(this.from);
        out.writeGenericValue(this.to);
        out.writeBoolean(this.includeLower);
        out.writeBoolean(this.includeUpper);
        String timeZoneId = null;
        if (this.timeZone != null) {
            timeZoneId = this.timeZone.getID();
        }
        out.writeOptionalString(timeZoneId);
        String formatString = null;
        if (this.format != null) {
            formatString = this.format.format();
        }
        out.writeOptionalString(formatString);
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
