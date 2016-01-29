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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class IdFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_id";

    public static final String CONTENT_TYPE = "_id";

    public static class Defaults {
        public static final String NAME = IdFieldMapper.NAME;

        public static final MappedFieldType FIELD_TYPE = new IdFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, IdFieldMapper> {

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing, Defaults.FIELD_TYPE);
            indexName = Defaults.NAME;
        }

        // if we are indexed we use DOCS
        @Override
        protected IndexOptions getDefaultIndexOption() {
            return IndexOptions.DOCS;
        }

        @Override
        public IdFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new IdFieldMapper(fieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(NAME + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new IdFieldMapper(indexSettings, fieldType);
        }
    }

    static final class IdFieldType extends MappedFieldType {

        public IdFieldType() {
            setFieldDataType(new FieldDataType("string"));
        }

        protected IdFieldType(IdFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new IdFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String value(Object value) {
            if (value == null) {
                return null;
            }
            return value.toString();
        }

        @Override
        public boolean useTermQueryWithQueryString() {
            return true;
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            if (indexOptions() != IndexOptions.NONE || context == null) {
                return super.termQuery(value, context);
            }
            final BytesRef[] uids = Uid.createUidsForTypesAndId(context.queryTypes(), value);
            return new TermsQuery(UidFieldMapper.NAME, uids);
        }

        @Override
        public Query termsQuery(List values, @Nullable QueryShardContext context) {
            if (indexOptions() != IndexOptions.NONE || context == null) {
                return super.termsQuery(values, context);
            }
            return new TermsQuery(UidFieldMapper.NAME, Uid.createUidsForTypesAndIds(context.queryTypes(), values));
        }

        @Override
        public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryShardContext context) {
            if (indexOptions() != IndexOptions.NONE || context == null) {
                return super.prefixQuery(value, method, context);
            }
            Collection<String> queryTypes = context.queryTypes();
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            for (String queryType : queryTypes) {
                PrefixQuery prefixQuery = new PrefixQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(queryType, BytesRefs.toBytesRef(value))));
                if (method != null) {
                    prefixQuery.setRewriteMethod(method);
                }
                query.add(prefixQuery, BooleanClause.Occur.SHOULD);
            }
            return query.build();
        }

        @Override
        public Query regexpQuery(String value, int flags, int maxDeterminizedStates, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryShardContext context) {
            if (indexOptions() != IndexOptions.NONE || context == null) {
                return super.regexpQuery(value, flags, maxDeterminizedStates, method, context);
            }
            Collection<String> queryTypes = context.queryTypes();
            if (queryTypes.size() == 1) {
                RegexpQuery regexpQuery = new RegexpQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(Iterables.getFirst(queryTypes, null), BytesRefs.toBytesRef(value))),
                    flags, maxDeterminizedStates);
                if (method != null) {
                    regexpQuery.setRewriteMethod(method);
                }
                return regexpQuery;
            }
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            for (String queryType : queryTypes) {
                RegexpQuery regexpQuery = new RegexpQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(queryType, BytesRefs.toBytesRef(value))), flags, maxDeterminizedStates);
                if (method != null) {
                    regexpQuery.setRewriteMethod(method);
                }
                query.add(regexpQuery, BooleanClause.Occur.SHOULD);
            }
            return query.build();
        }
    }

    private IdFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing != null ? existing : Defaults.FIELD_TYPE, indexSettings);
    }

    private IdFieldMapper(MappedFieldType fieldType, Settings indexSettings) {
        super(NAME, fieldType, Defaults.FIELD_TYPE, indexSettings);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        if (context.sourceToParse().id() != null) {
            context.id(context.sourceToParse().id());
            super.parse(context);
        }
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        if (context.id() == null) {
            throw new MapperParsingException("No id found while parsing the content source");
        }
        // it either get built in the preParse phase, or get parsed...
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentName() != null && parser.currentName().equals(Defaults.NAME) && parser.currentToken().isValue()) {
            // we are in the parse Phase
            String id = parser.text();
            if (context.id() != null && !context.id().equals(id)) {
                throw new MapperParsingException("Provided id [" + context.id() + "] does not match the content one [" + id + "]");
            }
            context.id(id);
        } // else we are in the pre/post parse phase

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            fields.add(new Field(fieldType().name(), context.id(), fieldType()));
        }
        if (fieldType().hasDocValues()) {
            fields.add(new BinaryDocValuesField(fieldType().name(), new BytesRef(context.id())));
        }
    }

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
        // do nothing here, no merging, but also no exception
    }
}
