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

package org.elasticsearch.index.mapper.externalvalues;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.core.StringFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseTextField;

// Like a String mapper but with very few options. We just use it to test if highlighting on a custom string mapped field works as expected.
public class FakeStringFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "fake_string";

    public static class Defaults {

        public static final MappedFieldType FIELD_TYPE = new FakeStringFieldType();

        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, FakeStringFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public FakeStringFieldType fieldType() {
            return (FakeStringFieldType) super.fieldType();
        }

        @Override
        public FakeStringFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new FakeStringFieldMapper(
                name, fieldType(), defaultFieldType,
                context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {
        }

        @Override
        public Mapper.Builder parse(String fieldName, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            FakeStringFieldMapper.Builder builder = new FakeStringFieldMapper.Builder(fieldName);
            parseTextField(builder, fieldName, node, parserContext);
            return builder;
        }
    }

    public static final class FakeStringFieldType extends StringFieldType {


        public FakeStringFieldType() {
        }

        protected FakeStringFieldType(FakeStringFieldType ref) {
            super(ref);
        }

        public FakeStringFieldType clone() {
            return new FakeStringFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query nullValueQuery() {
            if (nullValue() == null) {
                return null;
            }
            return termQuery(nullValue(), null);
        }
    }

    protected FakeStringFieldMapper(String simpleName, FakeStringFieldType fieldType, MappedFieldType defaultFieldType,
                                    Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    protected StringFieldMapper clone() {
        return (StringFieldMapper) super.clone();
    }

    @Override
    protected boolean customBoost() {
        return true;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        StringFieldMapper.ValueAndBoost valueAndBoost = parseCreateFieldForString(context, fieldType().boost());
        if (valueAndBoost.value() == null) {
            return;
        }
        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().name(), valueAndBoost.value(), fieldType());
            fields.add(field);
        }
        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(valueAndBoost.value())));
        }
    }

    public static StringFieldMapper.ValueAndBoost parseCreateFieldForString(ParseContext context, float defaultBoost) throws IOException {
        if (context.externalValueSet()) {
            return new StringFieldMapper.ValueAndBoost(context.externalValue().toString(), defaultBoost);
        }
        XContentParser parser = context.parser();
        return new StringFieldMapper.ValueAndBoost(parser.textOrNull(), defaultBoost);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
    }

    @Override
    public FakeStringFieldType fieldType() {
        return (FakeStringFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        doXContentAnalyzers(builder, includeDefaults);
    }

}
