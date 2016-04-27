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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
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
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }

        public static final EnabledAttributeMapper ENABLED_STATE = EnabledAttributeMapper.UNSET_DISABLED;
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, IndexFieldMapper> {

        private EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing, Defaults.FIELD_TYPE);
            indexName = Defaults.NAME;
        }

        public Builder enabled(EnabledAttributeMapper enabledState) {
            this.enabledState = enabledState;
            return this;
        }

        @Override
        public IndexFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            fieldType.setHasDocValues(false);
            return new IndexFieldMapper(fieldType, enabledState, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
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

        /**
         * This termQuery impl looks at the context to determine the index that
         * is being queried and then returns a MATCH_ALL_QUERY or MATCH_NO_QUERY
         * if the value matches this index. This can be useful if aliases or
         * wildcards are used but the aim is to restrict the query to specific
         * indices
         */
        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            if (context == null) {
                return super.termQuery(value, context);
            }
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

    private EnabledAttributeMapper enabledState;

    private IndexFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing == null ? Defaults.FIELD_TYPE.clone() : existing, Defaults.ENABLED_STATE, indexSettings);
    }

    private IndexFieldMapper(MappedFieldType fieldType, EnabledAttributeMapper enabledState, Settings indexSettings) {
        super(NAME, fieldType, Defaults.FIELD_TYPE, indexSettings);
        this.enabledState = enabledState;
    }

    public boolean enabled() {
        return this.enabledState.enabled;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        // we pre parse it and not in parse, since its not part of the root object
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!enabledState.enabled) {
            return;
        }
        fields.add(new Field(fieldType().name(), context.index(), fieldType()));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // if all defaults, no need to write it at all
        if (includeDefaults == false && enabledState == Defaults.ENABLED_STATE) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (includeDefaults || enabledState != Defaults.ENABLED_STATE) {
            builder.field("enabled", enabledState.enabled);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        IndexFieldMapper indexFieldMapperMergeWith = (IndexFieldMapper) mergeWith;
        if (indexFieldMapperMergeWith.enabledState != enabledState && !indexFieldMapperMergeWith.enabledState.unset()) {
            this.enabledState = indexFieldMapperMergeWith.enabledState;
        }
    }

}
