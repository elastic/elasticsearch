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
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

// Like a String mapper but with very few options. We just use it to test if highlighting on a custom string mapped field works as expected.
public class FakeStringFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "fake_string";

    public static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    }

    public static class Builder extends FieldMapper.Builder {

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.emptyList();
        }

        @Override
        public FakeStringFieldMapper build(ContentPath contentPath) {
            return new FakeStringFieldMapper(
                new FakeStringFieldType(name, true,
                    new TextSearchInfo(FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER)),
                multiFieldsBuilder.build(this, contentPath), copyTo.build());
        }
    }

    public static TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class FakeStringFieldType extends StringFieldType {

        private FakeStringFieldType(String name, boolean stored, TextSearchInfo textSearchInfo) {
            super(name, true, stored, true, textSearchInfo, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }
    }

    protected FakeStringFieldMapper(MappedFieldType mappedFieldType,
                                    MultiFields multiFields, CopyTo copyTo) {
        super(mappedFieldType.name(), mappedFieldType, Lucene.STANDARD_ANALYZER,
            new IndexableValueParser() {
                @Override
                public void parseAndIndex(XContentParser parser, Consumer<IndexableValue> indexer) throws IOException {
                    indexer.accept(IndexableValue.wrapString(parser.text()));
                }
            }, multiFields, copyTo);
    }

    @Override
    protected void buildIndexableFields(ParseContext context, IndexableValue value) {
        Field field = new Field(fieldType().name(), value.stringValue(), FIELD_TYPE);
        context.doc().add(field);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
