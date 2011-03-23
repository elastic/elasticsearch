/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;

public class SizeFieldMapper extends IntegerFieldMapper {

    public static final String CONTENT_TYPE = "_size";

    public static class Defaults extends IntegerFieldMapper.Defaults {
        public static final String NAME = CONTENT_TYPE;
        public static final boolean ENABLED = false;
    }

    public static class Builder extends XContentMapper.Builder<Builder, IntegerFieldMapper> {

        protected boolean enabled = Defaults.ENABLED;

        protected Field.Store store = Defaults.STORE;

        public Builder() {
            super(Defaults.NAME);
            builder = this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return builder;
        }

        public Builder store(Field.Store store) {
            this.store = store;
            return builder;
        }

        @Override public SizeFieldMapper build(BuilderContext context) {
            return new SizeFieldMapper(enabled, store);
        }
    }

    private final boolean enabled;

    public SizeFieldMapper() {
        this(Defaults.ENABLED, Defaults.STORE);
    }

    public SizeFieldMapper(boolean enabled, Field.Store store) {
        super(new Names(Defaults.NAME), Defaults.PRECISION_STEP, Defaults.INDEX, store, Defaults.BOOST, Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Defaults.NULL_VALUE);
        this.enabled = enabled;
    }

    @Override protected String contentType() {
        return Defaults.NAME;
    }

    public boolean enabled() {
        return this.enabled;
    }

    @Override protected Fieldable parseCreateField(ParseContext context) throws IOException {
        if (!enabled) {
            return null;
        }
        return new CustomIntegerNumericField(this, ((Number) context.externalValue()).intValue());
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // all are defaults, no need to write it at all
        if (enabled == Defaults.ENABLED && store == Defaults.STORE) {
            return builder;
        }
        builder.startObject(contentType());
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (store != Defaults.STORE) {
            builder.field("store", store.name().toLowerCase());
        }
        builder.endObject();
        return builder;
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // maybe allow to change enabled? But then we need to figure out null for default value
    }
}