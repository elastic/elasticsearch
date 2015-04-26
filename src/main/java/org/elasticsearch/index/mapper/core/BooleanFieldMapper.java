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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.booleanField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 * A field mapper for boolean fields.
 */
public class BooleanFieldMapper extends AbstractFieldMapper<Boolean> {

    public static final String CONTENT_TYPE = "boolean";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
        }

        public static final Boolean NULL_VALUE = null;
    }

    public static class Values {
        public final static BytesRef TRUE = new BytesRef("T");
        public final static BytesRef FALSE = new BytesRef("F");
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, BooleanFieldMapper> {

        private Boolean nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            this.builder = this;
        }

        public Builder nullValue(boolean nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public Builder tokenized(boolean tokenized) {
            if (tokenized) {
                throw new ElasticsearchIllegalArgumentException("bool field can't be tokenized");
            }
            return super.tokenized(tokenized);
        }

        @Override
        public BooleanFieldMapper build(BuilderContext context) {
            return new BooleanFieldMapper(buildNames(context), boost, fieldType, docValues, nullValue,
                    similarity, normsLoading, fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            BooleanFieldMapper.Builder builder = booleanField(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(nodeBooleanValue(propNode));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    private Boolean nullValue;

    protected BooleanFieldMapper(Names names, float boost, FieldType fieldType, Boolean docValues, Boolean nullValue,
                                 SimilarityProvider similarity, Loading normsLoading,
                                 @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, boost, fieldType, docValues, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, similarity, normsLoading, fieldDataSettings, indexSettings, multiFields, copyTo);
        this.nullValue = nullValue;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        // TODO have a special boolean type?
        return new FieldDataType(CONTENT_TYPE);
    }

    @Override
    public boolean useTermQueryWithQueryString() {
        return true;
    }

    @Override
    public Boolean value(Object value) {
        if (value == null) {
            return Boolean.FALSE;
        }
        String sValue = value.toString();
        if (sValue.length() == 0) {
            return Boolean.FALSE;
        }
        if (sValue.length() == 1 && sValue.charAt(0) == 'F') {
            return Boolean.FALSE;
        }
        if (Booleans.parseBoolean(sValue, false)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    @Override
    public Object valueForSearch(Object value) {
        return value(value);
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        if (value == null) {
            return Values.FALSE;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? Values.TRUE : Values.FALSE;
        }
        String sValue;
        if (value instanceof BytesRef) {
            sValue = ((BytesRef) value).utf8ToString();
        } else {
            sValue = value.toString();
        }
        if (sValue.length() == 0) {
            return Values.FALSE;
        }
        if (sValue.length() == 1 && sValue.charAt(0) == 'F') {
            return Values.FALSE;
        }
        if (Booleans.parseBoolean(sValue, false)) {
            return Values.TRUE;
        }
        return Values.FALSE;
    }

    @Override
    public Filter nullValueFilter() {
        if (nullValue == null) {
            return null;
        }
        return Queries.wrap(new TermQuery(names().createIndexNameTerm(nullValue ? Values.TRUE : Values.FALSE)));
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (fieldType().indexOptions() == IndexOptions.NONE && !fieldType().stored() && !hasDocValues()) {
            return;
        }

        Boolean value = context.parseExternalValue(Boolean.class);
        if (value == null) {
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                if (nullValue != null) {
                    value = nullValue;
                }
            } else {
                value = context.parser().booleanValue();
            }
        }

        if (value == null) {
            return;
        }
        fields.add(new Field(names.indexName(), value ? "T" : "F", fieldType));
        if (hasDocValues()) {
            fields.add(new SortedNumericDocValuesField(names.indexName(), value ? 1 : 0));
        }
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }

        if (!mergeResult.simulate()) {
            this.nullValue = ((BooleanFieldMapper) mergeWith).nullValue;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || nullValue != null) {
            builder.field("null_value", nullValue);
        }
    }
}
