/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.mapper.internal;

import com.google.common.base.Objects;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.lzf.LZF;
import org.elasticsearch.common.compress.lzf.LZFDecoder;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.document.ResetFieldSelector;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.source.SourceProvider;
import org.elasticsearch.index.source.SourceProviderParser;
import org.elasticsearch.index.source.SourceProviderService;
import org.elasticsearch.index.source.field.DefaultSourceProvider;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;
import static org.elasticsearch.index.mapper.MapperBuilders.source;

/**
 *
 */
public class SourceFieldMapper extends AbstractFieldMapper<byte[]> implements InternalMapper, RootMapper {

    public static final String NAME = "_source";

    public static final String CONTENT_TYPE = "_source";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = SourceFieldMapper.NAME;
        public static final boolean ENABLED = true;
        public static final String PROVIDER_NAME = "default";
        public static final SourceProvider PROVIDER = new DefaultSourceProvider();
        public static final Field.Index INDEX = Field.Index.NO;
        public static final Field.Store STORE = Field.Store.YES;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends Mapper.Builder<Builder, SourceFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        private SourceProvider sourceProvider = null;

        private String sourceProviderName = Defaults.PROVIDER_NAME;

        public Builder() {
            super(Defaults.NAME);
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder sourceProvider(String sourceProviderName, SourceProvider sourceProvider) {
            this.sourceProviderName = sourceProviderName;
            this.sourceProvider = sourceProvider;
            return this;
        }

        @Override
        public SourceFieldMapper build(BuilderContext context) {
            return new SourceFieldMapper(name, enabled, sourceProviderName, sourceProvider);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            SourceProviderService sourceProviderService = parserContext.sourceProviderService();

            SourceFieldMapper.Builder builder = source();
            String sourceProviderName = Defaults.PROVIDER_NAME;
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("provider") && fieldNode != null) {
                    sourceProviderName = nodeStringValue(fieldNode, null);
                }
            }
            
            SourceProviderParser parser = sourceProviderService.sourceProviderParser(sourceProviderName);
            if (parser == null) {
                throw new ElasticSearchParseException("failed to find source provider " + sourceProviderName);
            }
            builder.sourceProvider(sourceProviderName, parser.parse(node));
            return builder;
        }
    }


    private final boolean enabled;

    private final SourceProvider sourceProvider;

    private final String sourceProviderName;

    public SourceFieldMapper() {
        this(Defaults.NAME, Defaults.ENABLED, Defaults.PROVIDER_NAME, Defaults.PROVIDER);
    }

    protected SourceFieldMapper(String name, boolean enabled, String sourceProviderName, SourceProvider sourceProvider) {
        super(new Names(name, name, name, name), Defaults.INDEX, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.enabled = enabled;
        this.sourceProvider = sourceProvider;
        this.sourceProviderName = sourceProviderName;
    }

    public boolean enabled() {
        return this.enabled;
    }

    public ResetFieldSelector fieldSelector() {
        return SourceFieldSelector.INSTANCE;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // nothing to do here, we will call it in pre parse
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public boolean includeInObject() {
        return false;
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        if (!enabled) {
            return null;
        }
        if (store == Field.Store.NO) {
            return null;
        }
        if (context.flyweight()) {
            return null;
        }
        BytesHolder bytesHolder = sourceProvider.dehydrateSource(context);
        if(bytesHolder != null) {
            return new Field(names().indexName(), bytesHolder.bytes(), bytesHolder.offset(), bytesHolder.length());
        }
        return null;
    }

    public byte[] value(Document document) {
        Fieldable field = document.getFieldable(names.indexName());
        return field == null ? null : value(field);
    }

    public BytesHolder extractSource(String type, String id, Fieldable fieldable) {
        return sourceProvider.rehydrateSource(id, type, fieldable.getBinaryValue(),
                fieldable.getBinaryOffset(), fieldable.getBinaryLength());
    }

    @Override
    public byte[] value(Fieldable field) {
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

    @Override
    public byte[] valueFromString(String value) {
        return null;
    }

    @Override
    public String valueAsString(Fieldable field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String indexedValue(String value) {
        return value;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // all are defaults, no need to write it at all
        if (enabled == Defaults.ENABLED && Objects.equal(sourceProvider, Defaults.PROVIDER)) {
            return builder;
        }
        builder.startObject(contentType());
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }

        // check if provider type changed
        if (!Objects.equal(sourceProviderName, Defaults.PROVIDER_NAME)) {
            builder.field("provider", sourceProviderName);
        }

        // check if provider settings changed
        if (!Objects.equal(sourceProvider, Defaults.PROVIDER)) {
            sourceProvider.toXContent(builder, params);
        }
        
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        SourceFieldMapper sourceMergeWith = (SourceFieldMapper) mergeWith;
        if (!mergeContext.mergeFlags().simulate()) {
            if (sourceMergeWith.sourceProvider != null) {
                this.sourceProvider.merge(sourceMergeWith.sourceProvider, mergeContext);
            }
        }
    }
}
