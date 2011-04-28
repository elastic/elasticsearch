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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class StringFieldMapper extends AbstractFieldMapper<String> implements IncludeInAllMapper {

    public static final String CONTENT_TYPE = "string";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        // NOTE, when adding defaults here, make sure you add them in the builder
        public static final String NULL_VALUE = null;
    }

    public static class Builder extends AbstractFieldMapper.OpenBuilder<Builder, StringFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public Builder includeInAll(Boolean includeInAll) {
            this.includeInAll = includeInAll;
            return this;
        }

        @Override public StringFieldMapper build(BuilderContext context) {
            StringFieldMapper fieldMapper = new StringFieldMapper(buildNames(context),
                    index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, nullValue,
                    indexAnalyzer, searchAnalyzer);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements XContentMapper.TypeParser {
        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            StringFieldMapper.Builder builder = stringField(name);
            parseField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(propNode.toString());
                }
            }
            return builder;
        }
    }

    static class FieldWrapper {
        public Field field;
    }

    private String nullValue;

    private Boolean includeInAll;

    protected StringFieldMapper(Names names, Field.Index index, Field.Store store, Field.TermVector termVector,
                                float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                String nullValue, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
        super(names, index, store, termVector, boost, omitNorms, omitTermFreqAndPositions, indexAnalyzer, searchAnalyzer);
        this.nullValue = nullValue;
    }

    @Override public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            this.includeInAll = includeInAll;
        }
    }

    @Override public void includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            this.includeInAll = includeInAll;
        }
    }

    @Override public String value(Fieldable field) {
        return field.stringValue();
    }

    @Override public String valueFromString(String value) {
        return value;
    }

    @Override public String valueAsString(Fieldable field) {
        return value(field);
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override protected Field parseCreateField(ParseContext context) throws IOException {
        String value;
        if (context.externalValueSet()) {
            value = (String) context.externalValue();
            if (value == null) {
                value = nullValue;
            }
        } else {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                value = nullValue;
            } else {
                value = context.parser().text();
            }
        }
        if (value == null) {
            return null;
        }
        if (context.includeInAll(includeInAll)) {
            context.allEntries().addText(names.fullName(), value, boost);
        }
        if (!indexed() && !stored()) {
            context.ignoredValue(names.indexName(), value);
            return null;
        }
        return new Field(names.indexName(), false, value, store, index, termVector);
    }

    @Override protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            this.includeInAll = ((StringFieldMapper) mergeWith).includeInAll;
            this.nullValue = ((StringFieldMapper) mergeWith).nullValue;
        }
    }

    @Override protected void doXContentBody(XContentBuilder builder) throws IOException {
        super.doXContentBody(builder);
        if (index != Defaults.INDEX) {
            builder.field("index", index.name().toLowerCase());
        }
        if (store != Defaults.STORE) {
            builder.field("store", store.name().toLowerCase());
        }
        if (termVector != Defaults.TERM_VECTOR) {
            builder.field("term_vector", termVector.name().toLowerCase());
        }
        if (omitNorms != Defaults.OMIT_NORMS) {
            builder.field("omit_norms", omitNorms);
        }
        if (omitTermFreqAndPositions != Defaults.OMIT_TERM_FREQ_AND_POSITIONS) {
            builder.field("omit_term_freq_and_positions", omitTermFreqAndPositions);
        }
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }
    }
}
