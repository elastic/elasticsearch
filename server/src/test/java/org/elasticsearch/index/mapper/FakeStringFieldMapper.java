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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseTextField;

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
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }
    }

    protected FakeStringFieldMapper(String simpleName, FakeStringFieldType fieldType, MappedFieldType defaultFieldType,
                                    Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            value = context.parser().textOrNull();
        }

        if (value == null) {
            return;
        }

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().name(), value, fieldType());
            fields.add(field);
        }
        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(value)));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
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
