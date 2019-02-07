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

package org.elasticsearch.index.mapper.size;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.EnabledAttributeMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SizeFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_size";

    public static class Defaults  {
        public static final EnabledAttributeMapper ENABLED_STATE = EnabledAttributeMapper.UNSET_DISABLED;

        public static final MappedFieldType SIZE_FIELD_TYPE =
            new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);

        static {
            SIZE_FIELD_TYPE.setStored(true);
            SIZE_FIELD_TYPE.setName(NAME);
            SIZE_FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            SIZE_FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            SIZE_FIELD_TYPE.setHasDocValues(true);
            SIZE_FIELD_TYPE.freeze();
        }
    }

    private static MappedFieldType defaultFieldType(Version indexCreated) {
        MappedFieldType defaultFieldType = Defaults.SIZE_FIELD_TYPE.clone();
        defaultFieldType.setHasDocValues(true);
        return defaultFieldType;
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, SizeFieldMapper> {

        protected EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;

        private Builder(MappedFieldType existing, Version indexCreated) {
            super(NAME, existing == null ? defaultFieldType(indexCreated) : existing.clone(),
                defaultFieldType(indexCreated));
            builder = this;
        }

        public Builder enabled(EnabledAttributeMapper enabled) {
            this.enabledState = enabled;
            return builder;
        }

        @Override
        public SizeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new SizeFieldMapper(enabledState, fieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node,
                                                       ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME),
                parserContext.indexVersionCreated());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    boolean enabled = XContentMapValues.nodeBooleanValue(fieldNode, name + ".enabled");
                    builder.enabled(enabled ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED);
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            return new SizeFieldMapper(indexSettings, fieldType);
        }
    }

    private EnabledAttributeMapper enabledState;

    private SizeFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(Defaults.ENABLED_STATE,
            existing == null ? defaultFieldType(Version.indexCreated(indexSettings)) : existing.clone(),
            indexSettings);
    }

    private SizeFieldMapper(EnabledAttributeMapper enabled, MappedFieldType fieldType, Settings indexSettings) {
        super(NAME, fieldType, defaultFieldType(Version.indexCreated(indexSettings)), indexSettings);
        this.enabledState = enabled;
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    public boolean enabled() {
        return this.enabledState.enabled;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        // we post parse it so we get the size stored, possibly compressed (source will be preParse)
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // nothing to do here, we call the parent in postParse
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (!enabledState.enabled) {
            return;
        }
        final int value = context.sourceToParse().source().length();
        boolean indexed = fieldType().indexOptions() != IndexOptions.NONE;
        boolean docValued = fieldType().hasDocValues();
        boolean stored = fieldType().stored();
        fields.addAll(NumberFieldMapper.NumberType.INTEGER.createFields(name(), value, indexed, docValued, stored));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // all are defaults, no need to write it at all
        if (!includeDefaults && enabledState == Defaults.ENABLED_STATE) {
            return builder;
        }
        builder.startObject(contentType());
        if (includeDefaults || enabledState != Defaults.ENABLED_STATE) {
            builder.field("enabled", enabledState.enabled);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        SizeFieldMapper sizeFieldMapperMergeWith = (SizeFieldMapper) mergeWith;
        if (sizeFieldMapperMergeWith.enabledState != enabledState && !sizeFieldMapperMergeWith.enabledState.unset()) {
            this.enabledState = sizeFieldMapperMergeWith.enabledState;
        }
    }
}
