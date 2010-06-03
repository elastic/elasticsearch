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
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericIntegerAnalyzer;
import org.elasticsearch.index.field.FieldData;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.Numbers;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.xcontent.XContentParser;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;
import static org.elasticsearch.util.xcontent.support.XContentMapValues.*;

/**
 * @author kimchy (shay.banon)
 */
public class XContentIntegerFieldMapper extends XContentNumberFieldMapper<Integer> {

    public static final String CONTENT_TYPE = "integer";

    public static class Defaults extends XContentNumberFieldMapper.Defaults {
        public static final Integer NULL_VALUE = null;
    }

    public static class Builder extends XContentNumberFieldMapper.Builder<Builder, XContentIntegerFieldMapper> {

        protected Integer nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(int nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public XContentIntegerFieldMapper build(BuilderContext context) {
            XContentIntegerFieldMapper fieldMapper = new XContentIntegerFieldMapper(buildNames(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements XContentTypeParser {
        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            XContentIntegerFieldMapper.Builder builder = integerField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(nodeIntegerValue(propNode));
                }
            }
            return builder;
        }
    }

    private final Integer nullValue;

    private final String nullValueAsString;

    protected XContentIntegerFieldMapper(Names names, int precisionStep, Field.Index index, Field.Store store,
                                         float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                         Integer nullValue) {
        super(names, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new NamedAnalyzer("_int/" + precisionStep, new NumericIntegerAnalyzer(precisionStep)),
                new NamedAnalyzer("_int/max", new NumericIntegerAnalyzer(Integer.MAX_VALUE)));
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
    }

    @Override protected int maxPrecisionStep() {
        return 32;
    }

    @Override public Integer value(Fieldable field) {
        byte[] value = field.getBinaryValue();
        if (value == null) {
            return null;
        }
        return Numbers.bytesToInt(value);
    }

    @Override public String indexedValue(String value) {
        return indexedValue(Integer.parseInt(value));
    }

    @Override public String indexedValue(Integer value) {
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
        return Integer.parseInt(text);
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

    @Override protected Field parseCreateField(ParseContext context) throws IOException {
        int value;
        if (context.externalValueSet()) {
            Object externalValue = context.externalValue();
            if (externalValue == null) {
                if (nullValue == null) {
                    return null;
                }
                value = nullValue;
            } else {
                value = ((Number) externalValue).intValue();
            }
            if (includeInAll == null || includeInAll) {
                context.allEntries().addText(names.fullName(), Integer.toString(value), boost);
            }
        } else {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                if (nullValue == null) {
                    return null;
                }
                value = nullValue;
                if (nullValueAsString != null && (includeInAll == null || includeInAll)) {
                    context.allEntries().addText(names.fullName(), nullValueAsString, boost);
                }
            } else {
                value = context.parser().intValue();
                if (includeInAll == null || includeInAll) {
                    context.allEntries().addText(names.fullName(), context.parser().text(), boost);
                }
            }
        }

        Field field = null;
        if (stored()) {
            field = new Field(names.indexName(), Numbers.intToBytes(value), store);
            if (indexed()) {
                field.setTokenStream(popCachedStream(precisionStep).setIntValue(value));
            }
        } else if (indexed()) {
            field = new Field(names.indexName(), popCachedStream(precisionStep).setIntValue(value));
        }
        return field;
    }

    @Override public int sortType() {
        return SortField.INT;
    }

    @Override public FieldData.Type fieldDataType() {
        return FieldData.Type.INT;
    }

    @Override protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override protected void doXContentBody(XContentBuilder builder) throws IOException {
        super.doXContentBody(builder);
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }
    }
}
