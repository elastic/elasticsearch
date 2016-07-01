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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.IndexIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 *
 */
public class IndexFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_index";

    public static final String CONTENT_TYPE = "_index";

    public static class Defaults {
        public static final String NAME = IndexFieldMapper.NAME;

        public static final MappedFieldType FIELD_TYPE = new IndexFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, IndexFieldMapper> {

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing, Defaults.FIELD_TYPE);
        }

        @Override
        public IndexFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new IndexFieldMapper(fieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().onOrAfter(Version.V_5_0_0_alpha3)) {
                throw new MapperParsingException(NAME + " is not configurable");
            }
            return new Builder(parserContext.mapperService().fullName(NAME));
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new IndexFieldMapper(indexSettings, fieldType);
        }
    }

    static final class IndexFieldType extends MappedFieldType {

        public IndexFieldType() {}

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
        public boolean isSearchable() {
            // The _index field is always searchable.
            return true;
        }

        /**
         * This termQuery impl looks at the context to determine the index that
         * is being queried and then returns a MATCH_ALL_QUERY or MATCH_NO_QUERY
         * if the value matches this index. This can be useful if aliases or
         * wildcards are used but the aim is to restrict the query to specific
         * indices
         */
        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            if (isSameIndex(value, context.index().getName())) {
                return Queries.newMatchAllQuery();
            } else {
                return Queries.newMatchNoDocsQuery("Index didn't match. Index queried: " + context.index().getName() + " vs. " + value);
            }
        }

        @Override
        public Query termsQuery(List values, QueryShardContext context) {
            if (context == null) {
                return super.termsQuery(values, context);
            }
            for (Object value : values) {
                if (isSameIndex(value, context.index().getName())) {
                    // No need to OR these clauses - we can only logically be
                    // running in the context of just one of these index names.
                    return Queries.newMatchAllQuery();
                }
            }
            // None of the listed index names are this one
            return Queries.newMatchNoDocsQuery("Index didn't match. Index queried: " + context.index().getName() + " vs. " + values);
        }

        private boolean isSameIndex(Object value, String indexName) {
            if (value instanceof BytesRef) {
                BytesRef indexNameRef = new BytesRef(indexName);
                return (indexNameRef.bytesEquals((BytesRef) value));
            } else {
                return indexName.equals(value.toString());
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            return new IndexIndexFieldData.Builder();
        }
    }

    private IndexFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing == null ? Defaults.FIELD_TYPE.clone() : existing, indexSettings);
    }

    private IndexFieldMapper(MappedFieldType fieldType, Settings indexSettings) {
        super(NAME, fieldType, Defaults.FIELD_TYPE, indexSettings);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {}

    @Override
    public void postParse(ParseContext context) throws IOException {}

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {}

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        // nothing to do
    }

}
