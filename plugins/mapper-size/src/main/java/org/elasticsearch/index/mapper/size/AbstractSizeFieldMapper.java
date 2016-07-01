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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.internal.EnabledAttributeMapper;

import java.io.IOException;

abstract class AbstractSizeFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_size";
    public static final String CONTENT_TYPE = "_size";

    public static class Defaults {
        public static final EnabledAttributeMapper ENABLED_STATE = EnabledAttributeMapper.UNSET_DISABLED;
    }

    public static abstract class Builder extends MetadataFieldMapper.Builder<Builder, AbstractSizeFieldMapper> {
        protected EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;

        Builder(MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(NAME, fieldType, defaultFieldType);
        }

        public Builder enabled(EnabledAttributeMapper enabled) {
            this.enabledState = enabled;
            return builder;
        }
    }

    protected EnabledAttributeMapper enabledState;

    public AbstractSizeFieldMapper(EnabledAttributeMapper enabled, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                   Settings indexSettings) {
        super(NAME, fieldType, defaultFieldType, indexSettings);
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
        AbstractSizeFieldMapper sizeFieldMapperMergeWith = (AbstractSizeFieldMapper) mergeWith;
        if (sizeFieldMapperMergeWith.enabledState != enabledState && !sizeFieldMapperMergeWith.enabledState.unset()) {
            this.enabledState = sizeFieldMapperMergeWith.enabledState;
        }
    }
}
