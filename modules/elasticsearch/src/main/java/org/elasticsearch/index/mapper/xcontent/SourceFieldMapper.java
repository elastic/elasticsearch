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
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.compress.lzf.LZF;
import org.elasticsearch.common.compress.lzf.LZFDecoder;
import org.elasticsearch.common.compress.lzf.LZFEncoder;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class SourceFieldMapper extends AbstractFieldMapper<byte[]> implements org.elasticsearch.index.mapper.SourceFieldMapper {

    public static final String CONTENT_TYPE = "_source";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = org.elasticsearch.index.mapper.SourceFieldMapper.NAME;
        public static final boolean ENABLED = true;
        public static final long COMPRESS_THRESHOLD = -1;
        public static final Field.Index INDEX = Field.Index.NO;
        public static final Field.Store STORE = Field.Store.YES;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends XContentMapper.Builder<Builder, SourceFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        private long compressThreshold = Defaults.COMPRESS_THRESHOLD;

        private Boolean compress = null;

        public Builder() {
            super(Defaults.NAME);
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder compress(boolean compress) {
            this.compress = compress;
            return this;
        }

        public Builder compressThreshold(long compressThreshold) {
            this.compressThreshold = compressThreshold;
            return this;
        }

        @Override public SourceFieldMapper build(BuilderContext context) {
            return new SourceFieldMapper(name, enabled, compress, compressThreshold);
        }
    }

    private final boolean enabled;

    private Boolean compress;

    private long compressThreshold;

    private final SourceFieldSelector fieldSelector;

    protected SourceFieldMapper() {
        this(Defaults.NAME, Defaults.ENABLED, null, -1);
    }

    protected SourceFieldMapper(String name, boolean enabled, Boolean compress, long compressThreshold) {
        super(new Names(name, name, name, name), Defaults.INDEX, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.enabled = enabled;
        this.compress = compress;
        this.compressThreshold = compressThreshold;
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
        if (store == Field.Store.NO) {
            return null;
        }
        if (context.flyweight()) {
            return null;
        }
        byte[] data = context.source();
        if (compress != null && compress && !LZF.isCompressed(data)) {
            if (compressThreshold == -1 || data.length > compressThreshold) {
                data = LZFEncoder.encode(data, data.length);
                context.source(data);
            }
        }
        return new Field(names().indexName(), data);
    }

    @Override public byte[] value(Document document) {
        Fieldable field = document.getFieldable(names.indexName());
        return field == null ? null : value(field);
    }

    @Override public byte[] nativeValue(Fieldable field) {
        return field.getBinaryValue();
    }

    @Override public byte[] value(Fieldable field) {
        byte[] value = field.getBinaryValue();
        if (value == null) {
            return value;
        }
        if (LZF.isCompressed(value)) {
            try {
                return LZFDecoder.decode(value);
            } catch (IOException e) {
                throw new ElasticSearchParseException("failed to decompress source", e);
            }
        }
        return value;
    }

    @Override public byte[] valueFromString(String value) {
        return null;
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

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // all are defaults, no need to write it at all
        if (enabled == Defaults.ENABLED && compress == null && compressThreshold == -1) {
            return builder;
        }
        builder.startObject(contentType());
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (compress != null) {
            builder.field("compress", compress);
        }
        if (compressThreshold != -1) {
            builder.field("compress_threshold", new ByteSizeValue(compressThreshold).toString());
        }
        builder.endObject();
        return builder;
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        SourceFieldMapper sourceMergeWith = (SourceFieldMapper) mergeWith;
        if (!mergeContext.mergeFlags().simulate()) {
            if (sourceMergeWith.compress != null) {
                this.compress = sourceMergeWith.compress;
            }
            if (sourceMergeWith.compressThreshold != -1) {
                this.compressThreshold = sourceMergeWith.compressThreshold;
            }
        }
    }
}
