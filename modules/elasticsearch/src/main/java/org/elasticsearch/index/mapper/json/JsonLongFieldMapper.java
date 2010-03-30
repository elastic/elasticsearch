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
import org.elasticsearch.index.analysis.NumericLongAnalyzer;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.Numbers;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;
import static org.elasticsearch.index.mapper.json.JsonTypeParsers.*;
import static org.elasticsearch.util.json.JacksonNodes.*;

/**
 * @author kimchy (shay.banon)
 */
public class JsonLongFieldMapper extends JsonNumberFieldMapper<Long> {

    public static final String JSON_TYPE = "long";

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final Long NULL_VALUE = null;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonLongFieldMapper> {

        protected Long nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(long nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public JsonLongFieldMapper build(BuilderContext context) {
            JsonLongFieldMapper fieldMapper = new JsonLongFieldMapper(buildNames(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode longNode = (ObjectNode) node;
            JsonLongFieldMapper.Builder builder = longField(name);
            parseNumberField(builder, name, longNode, parserContext);
            for (Iterator<Map.Entry<String, JsonNode>> propsIt = longNode.getFields(); propsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = propsIt.next();
                String propName = entry.getKey();
                JsonNode propNode = entry.getValue();
                if (propName.equals("nullValue")) {
                    builder.nullValue(nodeLongValue(propNode));
                }
            }
            return builder;
        }
    }

    private final Long nullValue;

    private final String nullValueAsString;

    protected JsonLongFieldMapper(Names names, int precisionStep, Field.Index index, Field.Store store,
                                  float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                  Long nullValue) {
        super(names, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new NamedAnalyzer("_long/" + precisionStep, new NumericLongAnalyzer(precisionStep)),
                new NamedAnalyzer("_long/max", new NumericLongAnalyzer(Integer.MAX_VALUE)));
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
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

    @Override public String indexedValue(String value) {
        return indexedValue(Long.parseLong(value));
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
        return Long.parseLong(text);
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : Long.parseLong(lowerTerm),
                upperTerm == null ? null : Long.parseLong(upperTerm),
                includeLower, includeUpper);
    }

    @Override public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return NumericRangeFilter.newLongRange(names.indexName(), precisionStep,
                lowerTerm == null ? null : Long.parseLong(lowerTerm),
                upperTerm == null ? null : Long.parseLong(upperTerm),
                includeLower, includeUpper);
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        long value;
        if (jsonContext.externalValueSet()) {
            Object externalValue = jsonContext.externalValue();
            if (externalValue == null) {
                if (nullValue == null) {
                    return null;
                }
                value = nullValue;
            } else {
                value = ((Number) externalValue).longValue();
            }
            if (includeInAll == null || includeInAll) {
                jsonContext.allEntries().addText(names.fullName(), Long.toString(value), boost);
            }
        } else {
            if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
                if (nullValue == null) {
                    return null;
                }
                value = nullValue;
                if (nullValueAsString != null && (includeInAll == null || includeInAll)) {
                    jsonContext.allEntries().addText(names.fullName(), nullValueAsString, boost);
                }
            } else {
                if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_STRING) {
                    value = Long.parseLong(jsonContext.jp().getText());
                } else {
                    value = jsonContext.jp().getLongValue();
                }
                if (includeInAll == null || includeInAll) {
                    jsonContext.allEntries().addText(names.fullName(), jsonContext.jp().getText(), boost);
                }
            }
        }

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
        if (nullValue != null) {
            builder.field("nullValue", nullValue);
        }
        if (includeInAll != null) {
            builder.field("includeInAll", includeInAll);
        }
    }
}