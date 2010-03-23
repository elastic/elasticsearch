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
import org.elasticsearch.index.analysis.NumericIntegerAnalyzer;
import org.elasticsearch.util.Numbers;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class JsonShortFieldMapper extends JsonNumberFieldMapper<Short> {

    public static final String JSON_TYPE = "short";

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final Short NULL_VALUE = null;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonShortFieldMapper> {

        protected Short nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(short nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public JsonShortFieldMapper build(BuilderContext context) {
            JsonShortFieldMapper fieldMapper = new JsonShortFieldMapper(buildNames(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    private final Short nullValue;

    private final String nullValueAsString;

    protected JsonShortFieldMapper(Names names, int precisionStep, Field.Index index, Field.Store store,
                                   float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                   Short nullValue) {
        super(names, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new NamedAnalyzer("_short/" + precisionStep, new NumericIntegerAnalyzer(precisionStep)),
                new NamedAnalyzer("_short/max", new NumericIntegerAnalyzer(Integer.MAX_VALUE)));
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
    }

    @Override protected int maxPrecisionStep() {
        return 32;
    }

    @Override public Short value(Fieldable field) {
        byte[] value = field.getBinaryValue();
        if (value == null) {
            return Short.MIN_VALUE;
        }
        return Numbers.bytesToShort(value);
    }

    @Override public String indexedValue(String value) {
        return indexedValue(Short.parseShort(value));
    }

    @Override public String indexedValue(Short value) {
        return NumericUtils.intToPrefixCoded(value);
    }

    @Override public Object valueFromTerm(String term) {
        final int shift = term.charAt(0) - NumericUtils.SHIFT_START_INT;
        if (shift > 0 && shift <= 31) {
            return null;
        }
        return NumericUtils.prefixCodedToInt(term);
    }

    @Override public Object valueFromString(String text) {
        return Short.parseShort(text);
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newIntRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : Integer.parseInt(lowerTerm),
                upperTerm == null ? null : Integer.parseInt(upperTerm),
                includeLower, includeUpper);
    }

    @Override public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeFilter.newIntRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : Integer.parseInt(lowerTerm),
                upperTerm == null ? null : Integer.parseInt(upperTerm),
                includeLower, includeUpper);
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        int value;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            if (nullValue == null) {
                return null;
            }
            value = nullValue;
            if (includeInAll == null || includeInAll) {
                jsonContext.allEntries().addText(names.fullName(), nullValueAsString, boost);
            }
        } else {
            if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_STRING) {
                value = Integer.parseInt(jsonContext.jp().getText());
            } else {
                value = jsonContext.jp().getIntValue();
            }
            if (includeInAll == null || includeInAll) {
                jsonContext.allEntries().addText(names.fullName(), jsonContext.jp().getText(), boost);
            }
        }
        Field field = null;
        if (stored()) {
            field = new Field(names.indexName(), Numbers.shortToBytes(value), store);
            if (indexed()) {
                field.setTokenStream(popCachedStream(precisionStep).setIntValue(value));
            }
        } else if (indexed()) {
            field = new Field(names.indexName(), popCachedStream(precisionStep).setIntValue(value));
        }
        return field;
    }

    @Override public int sortType() {
        return SortField.SHORT;
    }

    @Override protected String jsonType() {
        return JSON_TYPE;
    }

    @Override protected void doJsonBody(JsonBuilder builder) throws IOException {
        super.doJsonBody(builder);
        if (nullValue != null) {
            builder.field("nullValue", nullValue);
        }
        if (includeInAll != null) {
            builder.field("includeInAll", includeInAll);
        }
    }
}