/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.json;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.*;
import org.apache.lucene.util.NumericUtils;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericDateAnalyzer;
import org.elasticsearch.util.Numbers;
import org.elasticsearch.util.joda.FormatDateTimeFormatter;
import org.elasticsearch.util.joda.Joda;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class JsonDateFieldMapper extends JsonNumberFieldMapper<Long> {

    public static final String JSON_TYPE = "date";

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern("dateOptionalTime");

        public static final String NULL_VALUE = null;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonDateFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        protected FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        @Override public JsonDateFieldMapper build(BuilderContext context) {
            JsonDateFieldMapper fieldMapper = new JsonDateFieldMapper(buildNames(context), dateTimeFormatter,
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }


    private final FormatDateTimeFormatter dateTimeFormatter;

    private final String nullValue;

    protected JsonDateFieldMapper(Names names, FormatDateTimeFormatter dateTimeFormatter, int precisionStep,
                                  Field.Index index, Field.Store store,
                                  float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                  String nullValue) {
        super(names, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new NamedAnalyzer("_date/" + precisionStep, new NumericDateAnalyzer(precisionStep, dateTimeFormatter.parser())),
                new NamedAnalyzer("_date/max", new NumericDateAnalyzer(Integer.MAX_VALUE, dateTimeFormatter.parser())));
        this.dateTimeFormatter = dateTimeFormatter;
        this.nullValue = nullValue;
    }

    @Override protected int maxPrecisionStep() {
        return 64;
    }

    @Override public Long value(Fieldable field) {
        byte[] value = field.getBinaryValue();
        if (value == null) {
            return Long.MIN_VALUE;
        }
        return Numbers.bytesToLong(value);
    }

    @Override public String valueAsString(Fieldable field) {
        return dateTimeFormatter.printer().print(value(field));
    }

    @Override public String indexedValue(String value) {
        return NumericUtils.longToPrefixCoded(dateTimeFormatter.parser().parseMillis(value));
    }

    @Override public String indexedValue(Long value) {
        return NumericUtils.longToPrefixCoded(value);
    }

    @Override public String valueAsString(String text) {
        final int shift = text.charAt(0) - NumericUtils.SHIFT_START_LONG;
        if (shift > 0 && shift <= 63) {
            return null;
        }
        return dateTimeFormatter.printer().print(NumericUtils.prefixCodedToLong(text));
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : dateTimeFormatter.parser().parseMillis(lowerTerm),
                upperTerm == null ? null : dateTimeFormatter.parser().parseMillis(upperTerm),
                includeLower, includeUpper);
    }

    @Override public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeFilter.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : dateTimeFormatter.parser().parseMillis(lowerTerm),
                upperTerm == null ? null : dateTimeFormatter.parser().parseMillis(upperTerm),
                includeLower, includeUpper);
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        String dateAsString;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            dateAsString = nullValue;
        } else {
            dateAsString = jsonContext.jp().getText();
        }
        if (dateAsString == null) {
            return null;
        }
        if (includeInAll == null || includeInAll) {
            jsonContext.allEntries().addText(names.fullName(), dateAsString, boost);
        }
        long value = dateTimeFormatter.parser().parseMillis(dateAsString);
        Field field = null;
        if (stored()) {
            field = new Field(names.indexName(), Numbers.longToBytes(value), store);
            if (indexed()) {
                field.setTokenStream(popCachedStream(precisionStep).setLongValue(value));
            }
        } else if (indexed()) {
            field = new Field(names.indexName(), popCachedStream(precisionStep).setLongValue(value));
        }
        return field;
    }

    @Override public int sortType() {
        return SortField.LONG;
    }

    @Override protected String jsonType() {
        return JSON_TYPE;
    }

    @Override protected void doJsonBody(JsonBuilder builder) throws IOException {
        super.doJsonBody(builder);
        builder.field("format", dateTimeFormatter.format());
        if (nullValue != null) {
            builder.field("nullValue", nullValue);
        }
        if (includeInAll != null) {
            builder.field("includeInAll", includeInAll);
        }
    }
}