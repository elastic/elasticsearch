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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericDateAnalyzer;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.Numbers;
import org.elasticsearch.util.joda.FormatDateTimeFormatter;
import org.elasticsearch.util.joda.Joda;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;
import static org.elasticsearch.index.mapper.json.JsonTypeParsers.*;

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

    public static class TypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode dateNode = (ObjectNode) node;
            JsonDateFieldMapper.Builder builder = dateField(name);
            parseNumberField(builder, name, dateNode, parserContext);
            for (Iterator<Map.Entry<String, JsonNode>> propsIt = dateNode.getFields(); propsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = propsIt.next();
                String propName = entry.getKey();
                JsonNode propNode = entry.getValue();
                if (propName.equals("nullValue")) {
                    builder.nullValue(propNode.getValueAsText());
                } else if (propName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(propName, propNode));
                }
            }
            return builder;
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

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        String dateAsString;
        if (jsonContext.externalValueSet()) {
            dateAsString = (String) jsonContext.externalValue();
            if (dateAsString == null) {
                dateAsString = nullValue;
            }
        } else {
            if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
                dateAsString = nullValue;
            } else {
                dateAsString = jsonContext.jp().getText();
            }
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