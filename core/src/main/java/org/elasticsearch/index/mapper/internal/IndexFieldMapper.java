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
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.search.highlight.HighlightBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class IndexFieldMapper extends AbstractFieldMapper implements RootMapper {

    public static final String NAME = "_index";

    public static final String CONTENT_TYPE = "_index";

    public static class Defaults extends AbstractFieldMapper.Defaults {
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

    public static class Builder extends AbstractFieldMapper.Builder<Builder, IndexFieldMapper> {

        private EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
            indexName = Defaults.NAME;
        }

        public Builder enabled(EnabledAttributeMapper enabledState) {
            this.enabledState = enabledState;
            return this;
        }

        @Override
        public IndexFieldMapper build(BuilderContext context) {
            fieldType.setNames(new MappedFieldType.Names(name, indexName, indexName, name));
            return new IndexFieldMapper(fieldType, enabledState, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            IndexFieldMapper.Builder builder = MapperBuilders.index();
            if (parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                parseField(builder, builder.name, node, parserContext);
            }

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
    }

    static final class IndexFieldType extends MappedFieldType {

        public IndexFieldType() {
            super(AbstractFieldMapper.Defaults.FIELD_TYPE);
        }

        protected IndexFieldType(IndexFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new IndexFieldType(this);
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

    public IndexFieldMapper(Settings indexSettings) {
        this(Defaults.FIELD_TYPE.clone(), Defaults.ENABLED_STATE, null, indexSettings);
    }

    public IndexFieldMapper(MappedFieldType fieldType, EnabledAttributeMapper enabledState,
                            @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(fieldType, false, fieldDataSettings, indexSettings);
        this.enabledState = enabledState;
    }

    public boolean enabled() {
        return this.enabledState.enabled;
    }

    @Override
    public MappedFieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType(IndexFieldMapper.NAME);
    }

    public String value(Document document) {
        Field field = (Field) document.getField(fieldType.names().indexName());
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
        fields.add(new Field(fieldType.names().indexName(), context.index(), fieldType));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // if all defaults, no need to write it at all
        if (!includeDefaults && fieldType().stored() == Defaults.FIELD_TYPE.stored() && enabledState == Defaults.ENABLED_STATE && customFieldDataSettings == null) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (indexCreatedBefore2x && (includeDefaults || fieldType().stored() != Defaults.FIELD_TYPE.stored())) {
            builder.field("store", fieldType().stored());
        }
        if (includeDefaults || enabledState != Defaults.ENABLED_STATE) {
            builder.field("enabled", enabledState.enabled);
        }

        if (indexCreatedBefore2x) {
            if (customFieldDataSettings != null) {
                builder.field("fielddata", (Map) customFieldDataSettings.getAsMap());
            } else if (includeDefaults) {
                builder.field("fielddata", (Map) fieldType.fieldDataType().getSettings().getAsMap());
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        IndexFieldMapper indexFieldMapperMergeWith = (IndexFieldMapper) mergeWith;
        if (!mergeResult.simulate()) {
            if (indexFieldMapperMergeWith.enabledState != enabledState && !indexFieldMapperMergeWith.enabledState.unset()) {
                this.enabledState = indexFieldMapperMergeWith.enabledState;
            }
        }
    }

}
