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
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NumericIntegerAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.internal.EnabledAttributeMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseStore;

public class SizeFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_size";
    public static final String CONTENT_TYPE = "_size";

    public static class Defaults extends IntegerFieldMapper.Defaults {
        public static final EnabledAttributeMapper ENABLED_STATE = EnabledAttributeMapper.UNSET_DISABLED;

        public static final MappedFieldType SIZE_FIELD_TYPE = IntegerFieldMapper.Defaults.FIELD_TYPE.clone();

        static {
            SIZE_FIELD_TYPE.setStored(true);
            SIZE_FIELD_TYPE.setNumericPrecisionStep(Defaults.PRECISION_STEP_32_BIT);
            SIZE_FIELD_TYPE.setNames(new MappedFieldType.Names(NAME));
            SIZE_FIELD_TYPE.setIndexAnalyzer(NumericIntegerAnalyzer.buildNamedAnalyzer(Defaults.PRECISION_STEP_32_BIT));
            SIZE_FIELD_TYPE.setSearchAnalyzer(NumericIntegerAnalyzer.buildNamedAnalyzer(Integer.MAX_VALUE));
            SIZE_FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, SizeFieldMapper> {

        protected EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;

        public Builder(MappedFieldType existing) {
            super(NAME, existing == null ? Defaults.SIZE_FIELD_TYPE : existing);
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

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME));
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode) ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED);
                    iterator.remove();
                } else if (fieldName.equals("store") && parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {
                    builder.store(parseStore(fieldName, fieldNode.toString()));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    private EnabledAttributeMapper enabledState;

    private SizeFieldMapper(EnabledAttributeMapper enabled, MappedFieldType fieldType, Settings indexSettings) {
        super(NAME, fieldType, Defaults.SIZE_FIELD_TYPE, indexSettings);
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
        if (context.flyweight()) {
            return;
        }
        fields.add(new IntegerFieldMapper.CustomIntegerNumericField(context.source().length(), fieldType()));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // all are defaults, no need to write it at all
        if (!includeDefaults && enabledState == Defaults.ENABLED_STATE && (indexCreatedBefore2x == false || fieldType().stored() == false)) {
            return builder;
        }
        builder.startObject(contentType());
        if (includeDefaults || enabledState != Defaults.ENABLED_STATE) {
            builder.field("enabled", enabledState.enabled);
        }
        if (indexCreatedBefore2x && (includeDefaults || fieldType().stored() == true)) {
            builder.field("store", fieldType().stored());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        SizeFieldMapper sizeFieldMapperMergeWith = (SizeFieldMapper) mergeWith;
        if (!mergeResult.simulate()) {
            if (sizeFieldMapperMergeWith.enabledState != enabledState && !sizeFieldMapperMergeWith.enabledState.unset()) {
                this.enabledState = sizeFieldMapperMergeWith.enabledState;
            }
        }
    }
}
