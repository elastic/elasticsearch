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

package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.*;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericDateAnalyzer;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.Numbers;
import org.elasticsearch.util.Strings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class XContentDateFieldMapper extends XContentNumberFieldMapper<Long> {

    public static final String CONTENT_TYPE = "date";

    public static class Defaults extends XContentNumberFieldMapper.Defaults {
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern("dateOptionalTime");

        public static final String NULL_VALUE = null;
    }

    public static class Builder extends XContentNumberFieldMapper.Builder<Builder, XContentDateFieldMapper> {

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

        @Override public XContentDateFieldMapper build(BuilderContext context) {
            XContentDateFieldMapper fieldMapper = new XContentDateFieldMapper(buildNames(context), dateTimeFormatter,
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements XContentTypeParser {
        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            XContentDateFieldMapper.Builder builder = dateField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(propNode.toString());
                } else if (propName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(propName, propNode));
                }
            }
            return builder;
        }
    }

    private final FormatDateTimeFormatter dateTimeFormatter;

    private final String nullValue;

    protected XContentDateFieldMapper(Names names, FormatDateTimeFormatter dateTimeFormatter, int precisionStep,
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
            return null;
        }
        return Numbers.bytesToLong(value);
    }

    /**
     * Dates should return as a string, delegates to {@link #valueAsString(org.apache.lucene.document.Fieldable)}.
     */
    @Override public Object valueForSearch(Fieldable field) {
        return valueAsString(field);
    }

    @Override public Object valueForSearch(Object value) {
        return dateTimeFormatter.printer().print((Long) value);
    }

    @Override public String valueAsString(Fieldable field) {
        Long value = value(field);
        if (value == null) {
            return null;
        }
        return dateTimeFormatter.printer().print(value);
    }

    @Override public String indexedValue(String value) {
        return NumericUtils.longToPrefixCoded(dateTimeFormatter.parser().parseMillis(value));
    }

    @Override public String indexedValue(Long value) {
        return NumericUtils.longToPrefixCoded(value);
    }

    @Override public Object valueFromTerm(String term) {
        final int shift = term.charAt(0) - NumericUtils.SHIFT_START_LONG;
        if (shift > 0 && shift <= 63) {
            return null;
        }
        return NumericUtils.prefixCodedToLong(term);
    }

    @Override public Object valueFromString(String text) {
        return dateTimeFormatter.parser().parseMillis(text);
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

    @Override protected Field parseCreateField(ParseContext context) throws IOException {
        String dateAsString;
        if (context.externalValueSet()) {
            dateAsString = (String) context.externalValue();
            if (dateAsString == null) {
                dateAsString = nullValue;
            }
        } else {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                dateAsString = nullValue;
            } else {
                dateAsString = context.parser().text();
            }
        }

        if (dateAsString == null) {
            return null;
        }
        if (includeInAll == null || includeInAll) {
            context.allEntries().addText(names.fullName(), dateAsString, boost);
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

    @Override public FieldData.Type fieldDataType() {
        return FieldData.Type.LONG;
    }

    @Override protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override protected void doXContentBody(XContentBuilder builder) throws IOException {
        super.doXContentBody(builder);
        builder.field("format", dateTimeFormatter.format());
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }
    }
}