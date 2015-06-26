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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.DateMathParser;
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
public class RangeQueryBuilder extends MultiTermQueryBuilder<RangeQueryBuilder> implements BoostableQueryBuilder<RangeQueryBuilder> {

    public static final boolean DEFAULT_INCLUDE_UPPER = true;

    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    public static final String NAME = "range";

    private final String fieldName;

    private Object from;

    private Object to;

    private String timeZone;

    private boolean includeLower = DEFAULT_INCLUDE_LOWER;

    private boolean includeUpper = DEFAULT_INCLUDE_UPPER;

    private float boost = 1.0f;

    private String queryName;

    private String format;

    static final RangeQueryBuilder PROTOTYPE = new RangeQueryBuilder(null);

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param fieldName The field name
     */
    public RangeQueryBuilder(String fieldName) {
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
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public RangeQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Gets the boost factor for the query.
     */
    public float boost() {
        return this.boost;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public RangeQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Gets the query name for the query.
     */
    public String queryName() {
        return this.queryName;
    }

    /**
     * In case of date field, we can adjust the from/to fields using a timezone
     */
    public RangeQueryBuilder timeZone(String timezone) {
        this.timeZone = timezone;
        return this;
    }

    /**
     * In case of date field, gets the from/to fields timezone adjustment
     */
    public String timeZone() {
        return this.timeZone;
    }

    /**
     * In case of format field, we can parse the from/to fields using this time format
     */
    public RangeQueryBuilder format(String format) {
        this.format = format;
        return this;
    }

    /**
     * Gets the format field to parse the from/to fields
     */
    public String format() {
        return this.format;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field("from", convertToStringIfBytesRef(this.from));
        builder.field("to", convertToStringIfBytesRef(this.to));
        builder.field("include_lower", includeLower);
        builder.field("include_upper", includeUpper);
        builder.field("boost", boost);
        if (timeZone != null) {
            builder.field("time_zone", timeZone);
        }
        if (format != null) {
            builder.field("format", format);
        }
        builder.endObject();
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        Query query = null;
        MappedFieldType mapper = parseContext.fieldMapper(this.fieldName);
        if (mapper != null) {
            if (mapper instanceof DateFieldMapper.DateFieldType) {
                DateMathParser forcedDateParser = null;
                if (this.format  != null) {
                    forcedDateParser = new DateMathParser(Joda.forPattern(this.format));
                }
                DateTimeZone dateTimeZone = null;
                if (this.timeZone != null) {
                    dateTimeZone = DateTimeZone.forID(this.timeZone);
                }
                query = ((DateFieldMapper.DateFieldType) mapper).rangeQuery(from, to, includeLower, includeUpper, dateTimeZone, forcedDateParser, parseContext);
            } else  {
                if (timeZone != null) {
                    throw new QueryParsingException(parseContext, "[range] time_zone can not be applied to non date field ["
                            + fieldName + "]");
                }
                //LUCENE 4 UPGRADE Mapper#rangeQuery should use bytesref as well?
                query = mapper.rangeQuery(from, to, includeLower, includeUpper, parseContext);
            }
        } else {
            if (timeZone != null) {
                throw new QueryParsingException(parseContext, "[range] time_zone can not be applied to non unmapped field ["
                        + fieldName + "]");
            }
        }

        if (query == null) {
            query = new TermRangeQuery(this.fieldName, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to), includeLower, includeUpper);
        }

        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (this.fieldName == null || this.fieldName.isEmpty()) {
            validationException = addValidationError("field name cannot be null or empty.", validationException);
        }
        if (this.timeZone != null) {
            try {
                DateTimeZone.forID(this.timeZone);
            } catch (Exception e) {
                validationException = addValidationError("error parsing timezone." + e.getMessage(),
                        validationException);
            }
        }
        if (this.format != null) {
            try {
                Joda.forPattern(this.format);
            } catch (Exception e) {
                validationException = addValidationError("error parsing format." + e.getMessage(),
                        validationException);
            }
        }
        return validationException;
    }

    @Override
    public RangeQueryBuilder readFrom(StreamInput in) throws IOException {
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(in.readString());
        rangeQueryBuilder.from = in.readGenericValue();
        rangeQueryBuilder.to = in.readGenericValue();
        rangeQueryBuilder.includeLower = in.readBoolean();
        rangeQueryBuilder.includeUpper = in.readBoolean();
        rangeQueryBuilder.timeZone = in.readOptionalString();
        rangeQueryBuilder.format = in.readOptionalString();
        rangeQueryBuilder.boost = in.readFloat();
        rangeQueryBuilder.queryName = in.readOptionalString();
        return rangeQueryBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeGenericValue(this.from);
        out.writeGenericValue(this.to);
        out.writeBoolean(this.includeLower);
        out.writeBoolean(this.includeUpper);
        out.writeOptionalString(this.timeZone);
        out.writeOptionalString(this.format);
        out.writeFloat(this.boost);
        out.writeOptionalString(this.queryName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, from, to, timeZone, includeLower, includeUpper,
                boost, queryName, format);
    }

    @Override
    public boolean doEquals(RangeQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
               Objects.equals(from, other.from) &&
               Objects.equals(to, other.to) &&
               Objects.equals(timeZone, other.timeZone) &&
               Objects.equals(includeLower, other.includeLower) &&
               Objects.equals(includeUpper, other.includeUpper) &&
               Objects.equals(boost, other.boost) &&
               Objects.equals(queryName, other.queryName) &&
               Objects.equals(format, other.format);
    }
}
