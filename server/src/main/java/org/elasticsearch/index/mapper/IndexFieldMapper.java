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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


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
        public MetadataFieldMapper.Builder<?,?> parse(String name, Map<String, Object> node,
                                                      ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            return new IndexFieldMapper(indexSettings, fieldType);
        }
    }

    static final class IndexFieldType extends MappedFieldType {

        IndexFieldType() {}

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

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new MatchAllDocsQuery();
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
            if (isSameIndex(value, context.getFullyQualifiedIndex().getName())) {
                return Queries.newMatchAllQuery();
            } else {
                return Queries.newMatchNoDocsQuery("Index didn't match. Index queried: " + context.index().getName()
                    + " vs. " + value);
            }
        }

        @Override
        public Query termsQuery(List values, QueryShardContext context) {
            if (context == null) {
                return super.termsQuery(values, context);
            }
            for (Object value : values) {
                if (isSameIndex(value, context.getFullyQualifiedIndex().getName())) {
                    // No need to OR these clauses - we can only logically be
                    // running in the context of just one of these index names.
                    return Queries.newMatchAllQuery();
                }
            }
            // None of the listed index names are this one
            return Queries.newMatchNoDocsQuery("Index didn't match. Index queried: " + context.getFullyQualifiedIndex().getName()
                + " vs. " + values);
        }

        @Override
        public Query prefixQuery(String value,
                                 @Nullable MultiTermQuery.RewriteMethod method,
                                 QueryShardContext context) {
            String indexName = context.getFullyQualifiedIndex().getName();
            if (indexName.startsWith(value)) {
                return Queries.newMatchAllQuery();
            } else {
                return Queries.newMatchNoDocsQuery("The index [" + indexName +
                    "] doesn't match the provided prefix [" + value + "].");
            }
        }

        @Override
        public Query regexpQuery(String value, int flags, int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            String indexName = context.getFullyQualifiedIndex().getName();
            Pattern pattern = Regex.compile(value, Regex.flagsToString(flags));

            if (pattern.matcher(indexName).matches()) {
                return Queries.newMatchAllQuery();
            } else {
                return Queries.newMatchNoDocsQuery("The index [" + indexName +
                    "] doesn't match the provided pattern [" + value + "].");
            }
        }

        @Override
        public Query wildcardQuery(String value,
                                   @Nullable MultiTermQuery.RewriteMethod method,
                                   QueryShardContext context) {
            String indexName = context.getFullyQualifiedIndex().getName();
            if (isSameIndex(value, indexName)) {
                return Queries.newMatchAllQuery();
            } else {
                return Queries.newMatchNoDocsQuery("The index [" + indexName +
                    "] doesn't match the provided pattern [" + value + "].");
            }
        }

        private boolean isSameIndex(Object value, String indexName) {
            String pattern = value instanceof BytesRef ? ((BytesRef) value).utf8ToString() : value.toString();
            return Regex.simpleMatch(pattern, indexName);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return new ConstantIndexFieldData.Builder(mapperService -> fullyQualifiedIndexName);
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
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {}

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        // nothing to do
    }
}
