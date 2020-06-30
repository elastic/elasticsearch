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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseTextField;

// Like a String mapper but with very few options. We just use it to test if highlighting on a custom string mapped field works as expected.
public class FakeStringFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "fake_string";

    public static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        public Builder(String name) {
            super(name, FIELD_TYPE);
            builder = this;
        }

        @Override
        public Builder index(boolean index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FakeStringFieldMapper build(BuilderContext context) {
            return new FakeStringFieldMapper(
                fieldType, new FakeStringFieldType(name),
                multiFieldsBuilder.build(this, context), copyTo);
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


        public FakeStringFieldType(String name) {
            super(name, true, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            setIndexAnalyzer(Lucene.STANDARD_ANALYZER);
            setSearchAnalyzer(Lucene.STANDARD_ANALYZER);
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

    protected FakeStringFieldMapper(FieldType fieldType, MappedFieldType mappedFieldType,
                                    MultiFields multiFields, CopyTo copyTo) {
        super(mappedFieldType.name(), fieldType, mappedFieldType, multiFields, copyTo);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            value = context.parser().textOrNull();
        }

        if (value == null) {
            return;
        }

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(fieldType().name(), value, fieldType);
            context.doc().add(field);
        }
        if (fieldType().hasDocValues()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(value)));
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FakeStringFieldType fieldType() {
        return (FakeStringFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
    }

}
