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

import org.apache.lucene.document.*;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class XContentSourceFieldMapper extends XContentFieldMapper<byte[]> implements SourceFieldMapper {

    public static final String CONTENT_TYPE = "_source";

    public static class Defaults extends XContentFieldMapper.Defaults {
        public static final String NAME = SourceFieldMapper.NAME;
        public static final boolean ENABLED = true;
        public static final Field.Index INDEX = Field.Index.NO;
        public static final Field.Store STORE = Field.Store.YES;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends XContentMapper.Builder<Builder, XContentSourceFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        public Builder() {
            super(Defaults.NAME);
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override public XContentSourceFieldMapper build(BuilderContext context) {
            return new XContentSourceFieldMapper(name, enabled);
        }
    }

    private final boolean enabled;

    private final SourceFieldSelector fieldSelector;

    protected XContentSourceFieldMapper() {
        this(Defaults.NAME, Defaults.ENABLED);
    }

    protected XContentSourceFieldMapper(String name, boolean enabled) {
        super(new Names(name, name, name, name), Defaults.INDEX, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.enabled = enabled;
        this.fieldSelector = new SourceFieldSelector(names.indexName());
    }

    public boolean enabled() {
        return this.enabled;
    }

    public FieldSelector fieldSelector() {
        return this.fieldSelector;
    }

    @Override protected Field parseCreateField(ParseContext context) throws IOException {
        if (!enabled) {
            return null;
        }
        return new Field(names.indexName(), context.source(), store);
    }

    @Override public byte[] value(Document document) {
        Fieldable field = document.getFieldable(names.indexName());
        return field == null ? null : value(field);
    }

    @Override public byte[] value(Fieldable field) {
        return field.getBinaryValue();
    }

    @Override public String valueAsString(Fieldable field) {
        throw new UnsupportedOperationException();
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    private static class SourceFieldSelector implements FieldSelector {

        private final String name;

        private SourceFieldSelector(String name) {
            this.name = name;
        }

        @Override public FieldSelectorResult accept(String fieldName) {
            if (fieldName.equals(name)) {
                return FieldSelectorResult.LOAD_AND_BREAK;
            }
            return FieldSelectorResult.NO_LOAD;
        }
    }

    @Override protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(contentType());
        builder.field("name", name());
        builder.field("enabled", enabled);
        builder.endObject();
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
