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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LegacyIntegerFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.internal.EnabledAttributeMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.lenientNodeBooleanValue;

public class SizeFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_size";
    public static final String CONTENT_TYPE = "_size";

    public static class Defaults  {
        public static final EnabledAttributeMapper ENABLED_STATE = EnabledAttributeMapper.UNSET_DISABLED;

        public static final MappedFieldType SIZE_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        public static final MappedFieldType LEGACY_SIZE_FIELD_TYPE = LegacyIntegerFieldMapper.Defaults.FIELD_TYPE.clone();

        static {
            SIZE_FIELD_TYPE.setStored(true);
            SIZE_FIELD_TYPE.setName(NAME);
            SIZE_FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            SIZE_FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            SIZE_FIELD_TYPE.freeze();

            LEGACY_SIZE_FIELD_TYPE.setStored(true);
            LEGACY_SIZE_FIELD_TYPE.setNumericPrecisionStep(LegacyIntegerFieldMapper.Defaults.PRECISION_STEP_32_BIT);
            LEGACY_SIZE_FIELD_TYPE.setName(NAME);
            LEGACY_SIZE_FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            LEGACY_SIZE_FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            LEGACY_SIZE_FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, SizeFieldMapper> {

        protected EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;

        private Builder(MappedFieldType existing, Version indexCreated) {
            super(NAME, existing == null
                    ? indexCreated.before(Version.V_5_0_0_alpha2) ? Defaults.LEGACY_SIZE_FIELD_TYPE : Defaults.SIZE_FIELD_TYPE
                    : existing, Defaults.LEGACY_SIZE_FIELD_TYPE);
            builder = this;
        }

        public Builder enabled(EnabledAttributeMapper enabled) {
            this.enabledState = enabled;
            return builder;
        }

        @Override
        public SizeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            fieldType.setHasDocValues(false);
            return new SizeFieldMapper(enabledState, fieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME), parserContext.indexVersionCreated());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(lenientNodeBooleanValue(fieldNode) ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED);
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new SizeFieldMapper(indexSettings, fieldType);
        }
    }

    private EnabledAttributeMapper enabledState;

    private SizeFieldMapper(Settings indexSettings, MappedFieldType mappedFieldType) {
        this(Defaults.ENABLED_STATE, mappedFieldType == null ? Defaults.LEGACY_SIZE_FIELD_TYPE : mappedFieldType, indexSettings);
    }

    private SizeFieldMapper(EnabledAttributeMapper enabled, MappedFieldType fieldType, Settings indexSettings) {
        super(NAME, fieldType, Defaults.LEGACY_SIZE_FIELD_TYPE, indexSettings);
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
    public Mapper parse(ParseContext context) throws IOException {
        // nothing to do here, we call the parent in postParse
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!enabledState.enabled) {
            return;
        }
        final int value = context.sourceToParse().source().length();
        if (Version.indexCreated(context.indexSettings()).before(Version.V_5_0_0_alpha2)) {
            fields.add(new LegacyIntegerFieldMapper.CustomIntegerNumericField(value, fieldType()));
        } else {
            boolean indexed = fieldType().indexOptions() != IndexOptions.NONE;
            boolean docValued = fieldType().hasDocValues();
            boolean stored = fieldType().stored();
            fields.addAll(NumberFieldMapper.NumberType.INTEGER.createFields(name(), value, indexed, docValued, stored));
        }
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
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        SizeFieldMapper sizeFieldMapperMergeWith = (SizeFieldMapper) mergeWith;
        if (sizeFieldMapperMergeWith.enabledState != enabledState && !sizeFieldMapperMergeWith.enabledState.unset()) {
            this.enabledState = sizeFieldMapperMergeWith.enabledState;
        }
    }
}
