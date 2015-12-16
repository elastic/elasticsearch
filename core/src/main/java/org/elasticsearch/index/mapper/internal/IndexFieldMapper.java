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
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

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
            FIELD_TYPE.setNames(new MappedFieldType.Names(NAME));
            FIELD_TYPE.freeze();
        }

        public static final EnabledAttributeMapper ENABLED_STATE = EnabledAttributeMapper.UNSET_DISABLED;
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, IndexFieldMapper> {

        private EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing);
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
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME));
            if (parserContext.indexVersionCreated().onOrAfter(Version.V_2_0_0_beta1)) {
                return builder;
            }

            parseField(builder, builder.name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    EnabledAttributeMapper mapper = nodeBooleanValue(fieldNode) ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED;
                    builder.enabled(mapper);
                    iterator.remove();
                }
            }
            return builder;
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
        public boolean useTermQueryWithQueryString() {
            // As we spoof the presence of an indexed field we have to override
            // the default of returning false which otherwise leads MatchQuery
            // et al to run an analyzer over the query string and then try to
            // hit the search index. We need them to use our termQuery(..)
            // method which checks index names
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
            if (context == null) {
                return super.termQuery(value, context);
            }
            if (isSameIndex(value, context.index().getName())) {
                return Queries.newMatchAllQuery();
            } else {
                return Queries.newMatchNoDocsQuery();
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
            return Queries.newMatchNoDocsQuery();
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
        public String value(Object value) {
            if (value == null) {
                return null;
            }
            return value.toString();
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

    public String value(Document document) {
        Field field = (Field) document.getField(fieldType().names().indexName());
        return field == null ? null : (String)fieldType().value(field);
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
        fields.add(new Field(fieldType().names().indexName(), context.index(), fieldType()));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // if all defaults, no need to write it at all
        if (!includeDefaults && fieldType().stored() == Defaults.FIELD_TYPE.stored() && enabledState == Defaults.ENABLED_STATE && hasCustomFieldDataSettings() == false) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (indexCreatedBefore2x && (includeDefaults || fieldType().stored() != Defaults.FIELD_TYPE.stored())) {
            builder.field("store", fieldType().stored());
        }
        if (includeDefaults || enabledState != Defaults.ENABLED_STATE) {
            builder.field("enabled", enabledState.enabled);
        }
        if (indexCreatedBefore2x && (includeDefaults || hasCustomFieldDataSettings())) {
            builder.field("fielddata", (Map) fieldType().fieldDataType().getSettings().getAsMap());
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
