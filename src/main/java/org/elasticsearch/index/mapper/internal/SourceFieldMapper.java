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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.document.ResetFieldSelector;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.List;
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
        public static final long COMPRESS_THRESHOLD = -1;
        public static final String FORMAT = null; // default format is to use the one provided
        public static final Field.Index INDEX = Field.Index.NO;
        public static final Field.Store STORE = Field.Store.YES;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
        public static final String[] INCLUDES = Strings.EMPTY_ARRAY;
        public static final String[] EXCLUDES = Strings.EMPTY_ARRAY;
    }

    public static class Builder extends Mapper.Builder<Builder, SourceFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        private long compressThreshold = Defaults.COMPRESS_THRESHOLD;

        private Boolean compress = null;

        private String format = Defaults.FORMAT;

        private String[] includes = Defaults.INCLUDES;
        private String[] excludes = Defaults.EXCLUDES;

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

        public Builder format(String format) {
            this.format = format;
            return this;
        }

        public Builder includes(String[] includes) {
            this.includes = includes;
            return this;
        }

        public Builder excludes(String[] excludes) {
            this.excludes = excludes;
            return this;
        }

        @Override
        public SourceFieldMapper build(BuilderContext context) {
            return new SourceFieldMapper(name, enabled, format, compress, compressThreshold, includes, excludes);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            SourceFieldMapper.Builder builder = source();

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("compress") && fieldNode != null) {
                    builder.compress(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("compress_threshold") && fieldNode != null) {
                    if (fieldNode instanceof Number) {
                        builder.compressThreshold(((Number) fieldNode).longValue());
                        builder.compress(true);
                    } else {
                        builder.compressThreshold(ByteSizeValue.parseBytesSizeValue(fieldNode.toString()).bytes());
                        builder.compress(true);
                    }
                } else if ("format".equals(fieldName)) {
                    builder.format(nodeStringValue(fieldNode, null));
                } else if (fieldName.equals("includes")) {
                    List<Object> values = (List<Object>) fieldNode;
                    String[] includes = new String[values.size()];
                    for (int i = 0; i < includes.length; i++) {
                        includes[i] = values.get(i).toString();
                    }
                    builder.includes(includes);
                } else if (fieldName.equals("excludes")) {
                    List<Object> values = (List<Object>) fieldNode;
                    String[] excludes = new String[values.size()];
                    for (int i = 0; i < excludes.length; i++) {
                        excludes[i] = values.get(i).toString();
                    }
                    builder.excludes(excludes);
                }
            }
            return builder;
        }
    }


    private final boolean enabled;

    private Boolean compress;

    private long compressThreshold;

    private String[] includes;

    private String[] excludes;

    private String format;

    private XContentType formatContentType;

    public SourceFieldMapper() {
        this(Defaults.NAME, Defaults.ENABLED, Defaults.FORMAT, null, -1, Defaults.INCLUDES, Defaults.EXCLUDES);
    }

    protected SourceFieldMapper(String name, boolean enabled, String format, Boolean compress, long compressThreshold, String[] includes, String[] excludes) {
        super(new Names(name, name, name, name), Defaults.INDEX, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.enabled = enabled;
        this.compress = compress;
        this.compressThreshold = compressThreshold;
        this.includes = includes;
        this.excludes = excludes;
        this.format = format;
        this.formatContentType = format == null ? null : XContentType.fromRestContentType(format);
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
        BytesReference source = context.source();

        boolean filtered = includes.length > 0 || excludes.length > 0;
        if (filtered) {
            // we don't update the context source if we filter, we want to keep it as is...

            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(source, true);
            Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), includes, excludes);
            CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
            StreamOutput streamOutput;
            if (compress != null && compress && (compressThreshold == -1 || source.length() > compressThreshold)) {
                streamOutput = cachedEntry.bytes(CompressorFactory.defaultCompressor());
            } else {
                streamOutput = cachedEntry.bytes();
            }
            XContentType contentType = formatContentType;
            if (contentType == null) {
                contentType = mapTuple.v1();
            }
            XContentBuilder builder = XContentFactory.contentBuilder(contentType, streamOutput).map(filteredSource);
            builder.close();

            source = cachedEntry.bytes().bytes().copyBytesArray();

            CachedStreamOutput.pushEntry(cachedEntry);
        } else if (compress != null && compress && !CompressorFactory.isCompressed(source)) {
            if (compressThreshold == -1 || source.length() > compressThreshold) {
                CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
                try {
                    XContentType contentType = XContentFactory.xContentType(source);
                    if (formatContentType != null && formatContentType != contentType) {
                        XContentBuilder builder = XContentFactory.contentBuilder(formatContentType, cachedEntry.bytes(CompressorFactory.defaultCompressor()));
                        builder.copyCurrentStructure(XContentFactory.xContent(contentType).createParser(source));
                        builder.close();
                    } else {
                        StreamOutput streamOutput = cachedEntry.bytes(CompressorFactory.defaultCompressor());
                        source.writeTo(streamOutput);
                        streamOutput.close();
                    }
                    // we copy over the byte array, since we need to push back the cached entry
                    // TODO, we we had a handle into when we are done with parsing, then we push back then and not copy over bytes
                    source = cachedEntry.bytes().bytes().copyBytesArray();
                    // update the data in the context, so it can be compressed and stored compressed outside...
                    context.source(source);
                } finally {
                    CachedStreamOutput.pushEntry(cachedEntry);
                }
            }
        } else if (formatContentType != null) {
            // see if we need to convert the content type
            Compressor compressor = CompressorFactory.compressor(source);
            if (compressor != null) {
                CompressedStreamInput compressedStreamInput = compressor.streamInput(source.streamInput());
                XContentType contentType = XContentFactory.xContentType(compressedStreamInput);
                compressedStreamInput.resetToBufferStart();
                if (contentType != formatContentType) {
                    // we need to reread and store back, compressed....
                    CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
                    try {
                        StreamOutput streamOutput = cachedEntry.bytes(CompressorFactory.defaultCompressor());
                        XContentBuilder builder = XContentFactory.contentBuilder(formatContentType, streamOutput);
                        builder.copyCurrentStructure(XContentFactory.xContent(contentType).createParser(compressedStreamInput));
                        builder.close();
                        source = cachedEntry.bytes().bytes().copyBytesArray();
                        // update the data in the context, so we store it in the translog in this format
                        context.source(source);
                    } finally {
                        CachedStreamOutput.pushEntry(cachedEntry);
                    }
                } else {
                    compressedStreamInput.close();
                }
            } else {
                XContentType contentType = XContentFactory.xContentType(source);
                if (contentType != formatContentType) {
                    // we need to reread and store back
                    // we need to reread and store back, compressed....
                    CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
                    try {
                        XContentBuilder builder = XContentFactory.contentBuilder(formatContentType, cachedEntry.bytes());
                        builder.copyCurrentStructure(XContentFactory.xContent(contentType).createParser(source));
                        builder.close();
                        source = cachedEntry.bytes().bytes().copyBytesArray();
                        // update the data in the context, so we store it in the translog in this format
                        context.source(source);
                    } finally {
                        CachedStreamOutput.pushEntry(cachedEntry);
                    }
                }
            }
        }
        assert source.hasArray();
        return new Field(names().indexName(), source.array(), source.arrayOffset(), source.length());
    }

    public byte[] value(Document document) {
        Fieldable field = document.getFieldable(names.indexName());
        return field == null ? null : value(field);
    }

    public byte[] nativeValue(Fieldable field) {
        return field.getBinaryValue();
    }

    @Override
    public byte[] value(Fieldable field) {
        byte[] value = field.getBinaryValue();
        if (value == null) {
            return value;
        }
        try {
            return CompressorFactory.uncompressIfNeeded(new BytesArray(value)).toBytes();
        } catch (IOException e) {
            throw new ElasticSearchParseException("failed to decompress source", e);
        }
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
        if (enabled == Defaults.ENABLED && compress == null && compressThreshold == -1 && includes.length == 0 && excludes.length == 0) {
            return builder;
        }
        builder.startObject(contentType());
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (!Objects.equal(format, Defaults.FORMAT)) {
            builder.field("format", format);
        }
        if (compress != null) {
            builder.field("compress", compress);
        }
        if (compressThreshold != -1) {
            builder.field("compress_threshold", new ByteSizeValue(compressThreshold).toString());
        }
        if (includes.length > 0) {
            builder.field("includes", includes);
        }
        if (excludes.length > 0) {
            builder.field("excludes", excludes);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
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
