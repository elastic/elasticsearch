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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;


public class IndexFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_index";

    public static final String CONTENT_TYPE = "_index";

    public static class Defaults {
        public static final String NAME = IndexFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder> {

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
        }

        @Override
        public IndexFieldMapper build(BuilderContext context) {
            return new IndexFieldMapper(fieldType);
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?> parse(String name, Map<String, Object> node,
                                                      ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(ParserContext context) {
            return new IndexFieldMapper(Defaults.FIELD_TYPE);
        }
    }

    static final class IndexFieldType extends ConstantFieldType {

        static final IndexFieldType INSTANCE = new IndexFieldType();

        private IndexFieldType() {
            super(NAME, Collections.emptyMap());
        }

        protected IndexFieldType(IndexFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new IndexFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected boolean matches(String pattern, QueryShardContext context) {
            return context.indexMatches(pattern);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return new ConstantIndexFieldData.Builder(mapperService -> fullyQualifiedIndexName, CoreValuesSourceType.BYTES);
        }

    }

    private IndexFieldMapper(FieldType fieldType) {
        super(fieldType, IndexFieldType.INSTANCE);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {}

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {}

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
