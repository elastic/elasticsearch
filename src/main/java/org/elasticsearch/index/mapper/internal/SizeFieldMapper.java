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
import org.apache.lucene.document.FieldType;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.size;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseStore;

public class SizeFieldMapper extends IntegerFieldMapper implements RootMapper {

    public static final String NAME = "_size";
    public static final String CONTENT_TYPE = "_size";

    public static class Defaults extends IntegerFieldMapper.Defaults {
        public static final String NAME = CONTENT_TYPE;
        public static final EnabledAttributeMapper ENABLED_STATE = EnabledAttributeMapper.UNSET_DISABLED;

        public static final FieldType SIZE_FIELD_TYPE = new FieldType(IntegerFieldMapper.Defaults.FIELD_TYPE);

        static {
            SIZE_FIELD_TYPE.setStored(true);
            SIZE_FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, IntegerFieldMapper> {

        protected EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;

        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.SIZE_FIELD_TYPE), Defaults.PRECISION_STEP_32_BIT);
            builder = this;
        }

        public Builder enabled(EnabledAttributeMapper enabled) {
            this.enabledState = enabled;
            return builder;
        }

        @Override
        public SizeFieldMapper build(BuilderContext context) {
            return new SizeFieldMapper(enabledState, fieldType, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            SizeFieldMapper.Builder builder = size();
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode) ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED);
                    iterator.remove();
                } else if (fieldName.equals("store") && parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                    builder.store(parseStore(fieldName, fieldNode.toString()));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    private EnabledAttributeMapper enabledState;

    public SizeFieldMapper(Settings indexSettings) {
        this(Defaults.ENABLED_STATE, new FieldType(Defaults.SIZE_FIELD_TYPE), null, indexSettings);
    }

    public SizeFieldMapper(EnabledAttributeMapper enabled, FieldType fieldType, @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(Defaults.NAME), Defaults.PRECISION_STEP_32_BIT, Defaults.BOOST, fieldType, false, Defaults.NULL_VALUE,
                Defaults.IGNORE_MALFORMED,  Defaults.COERCE, null, null, fieldDataSettings, 
                indexSettings, MultiFields.empty(), null);
        this.enabledState = enabled;
    }

    @Override
    protected String contentType() {
        return Defaults.NAME;
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
    public boolean includeInObject() {
        return false;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!enabledState.enabled) {
            return;
        }
        if (context.flyweight()) {
            return;
        }
        fields.add(new CustomIntegerNumericField(this, context.source().length(), fieldType));
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
