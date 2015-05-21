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

import com.google.common.base.Objects;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;
import static org.elasticsearch.index.mapper.MapperBuilders.source;

/**
 *
 */
public class SourceFieldMapper extends AbstractFieldMapper implements RootMapper {

    public static final String NAME = "_source";

    public static final String CONTENT_TYPE = "_source";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = SourceFieldMapper.NAME;
        public static final boolean ENABLED = true;
        public static final long COMPRESS_THRESHOLD = -1;
        public static final String FORMAT = null; // default format is to use the one provided

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE); // not indexed
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

    }

    public static class Builder extends Mapper.Builder<Builder, SourceFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        private long compressThreshold = Defaults.COMPRESS_THRESHOLD;

        private Boolean compress = null;

        private String format = Defaults.FORMAT;

        private String[] includes = null;
        private String[] excludes = null;

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
            return new SourceFieldMapper(name, enabled, format, compress, compressThreshold, includes, excludes, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            SourceFieldMapper.Builder builder = source();

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("compress") && parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                    if (fieldNode != null) {
                        builder.compress(nodeBooleanValue(fieldNode));
                    }
                    iterator.remove();
                } else if (fieldName.equals("compress_threshold") && parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                    if (fieldNode != null) {
                        if (fieldNode instanceof Number) {
                            builder.compressThreshold(((Number) fieldNode).longValue());
                            builder.compress(true);
                        } else {
                            builder.compressThreshold(ByteSizeValue.parseBytesSizeValue(fieldNode.toString()).bytes());
                            builder.compress(true);
                        }
                    }
                    iterator.remove();
                } else if ("format".equals(fieldName)) {
                    builder.format(nodeStringValue(fieldNode, null));
                    iterator.remove();
                } else if (fieldName.equals("includes")) {
                    List<Object> values = (List<Object>) fieldNode;
                    String[] includes = new String[values.size()];
                    for (int i = 0; i < includes.length; i++) {
                        includes[i] = values.get(i).toString();
                    }
                    builder.includes(includes);
                    iterator.remove();
                } else if (fieldName.equals("excludes")) {
                    List<Object> values = (List<Object>) fieldNode;
                    String[] excludes = new String[values.size()];
                    for (int i = 0; i < excludes.length; i++) {
                        excludes[i] = values.get(i).toString();
                    }
                    builder.excludes(excludes);
                    iterator.remove();
                }
            }
            return builder;
        }
    }


    private final boolean enabled;

    /** indicates whether the source will always exist and be complete, for use by features like the update API */
    private final boolean complete;

    private Boolean compress;
    private long compressThreshold;

    private final String[] includes;
    private final String[] excludes;

    private String format;

    private XContentType formatContentType;

    public SourceFieldMapper(Settings indexSettings) {
        this(Defaults.NAME, Defaults.ENABLED, Defaults.FORMAT, null, -1, null, null, indexSettings);
    }

    protected SourceFieldMapper(String name, boolean enabled, String format, Boolean compress, long compressThreshold,
                                String[] includes, String[] excludes, Settings indexSettings) {
        super(new Names(name, name, name, name), Defaults.BOOST, new FieldType(Defaults.FIELD_TYPE), false,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, null, null, null, indexSettings); // Only stored.
        this.enabled = enabled;
        this.compress = compress;
        this.compressThreshold = compressThreshold;
        this.includes = includes;
        this.excludes = excludes;
        this.format = format;
        this.formatContentType = format == null ? null : XContentType.fromRestContentType(format);
        this.complete = enabled && includes == null && excludes == null;
    }

    public boolean enabled() {
        return enabled;
    }

    public String[] excludes() {
        return this.excludes != null ? this.excludes : Strings.EMPTY_ARRAY;

    }

    public String[] includes() {
        return this.includes != null ? this.includes : Strings.EMPTY_ARRAY;
    }

    public boolean isComplete() {
        return complete;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return null;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // nothing to do here, we will call it in pre parse
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!enabled) {
            return;
        }
        if (!fieldType.stored()) {
            return;
        }
        if (context.flyweight()) {
            return;
        }
        BytesReference source = context.source();

        boolean filtered = (includes != null && includes.length > 0) || (excludes != null && excludes.length > 0);
        if (filtered) {
            // we don't update the context source if we filter, we want to keep it as is...

            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(source, true);
            Map<String, Object> filteredSource = XContentMapValues.filter(mapTuple.v2(), includes, excludes);
            BytesStreamOutput bStream = new BytesStreamOutput();
            StreamOutput streamOutput = bStream;
            if (compress != null && compress && (compressThreshold == -1 || source.length() > compressThreshold)) {
                streamOutput = CompressorFactory.defaultCompressor().streamOutput(bStream);
            }
            XContentType contentType = formatContentType;
            if (contentType == null) {
                contentType = mapTuple.v1();
            }
            XContentBuilder builder = XContentFactory.contentBuilder(contentType, streamOutput).map(filteredSource);
            builder.close();

            source = bStream.bytes();
        } else if (compress != null && compress && !CompressorFactory.isCompressed(source)) {
            if (compressThreshold == -1 || source.length() > compressThreshold) {
                BytesStreamOutput bStream = new BytesStreamOutput();
                XContentType contentType = XContentFactory.xContentType(source);
                if (formatContentType != null && formatContentType != contentType) {
                    XContentBuilder builder = XContentFactory.contentBuilder(formatContentType, CompressorFactory.defaultCompressor().streamOutput(bStream));
                    builder.copyCurrentStructure(XContentFactory.xContent(contentType).createParser(source));
                    builder.close();
                } else {
                    StreamOutput streamOutput = CompressorFactory.defaultCompressor().streamOutput(bStream);
                    source.writeTo(streamOutput);
                    streamOutput.close();
                }
                source = bStream.bytes();
                // update the data in the context, so it can be compressed and stored compressed outside...
                context.source(source);
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
                    BytesStreamOutput bStream = new BytesStreamOutput();
                    StreamOutput streamOutput = CompressorFactory.defaultCompressor().streamOutput(bStream);
                    XContentBuilder builder = XContentFactory.contentBuilder(formatContentType, streamOutput);
                    builder.copyCurrentStructure(XContentFactory.xContent(contentType).createParser(compressedStreamInput));
                    builder.close();
                    source = bStream.bytes();
                    // update the data in the context, so we store it in the translog in this format
                    context.source(source);
                } else {
                    compressedStreamInput.close();
                }
            } else {
                XContentType contentType = XContentFactory.xContentType(source);
                if (contentType != formatContentType) {
                    // we need to reread and store back
                    // we need to reread and store back, compressed....
                    BytesStreamOutput bStream = new BytesStreamOutput();
                    XContentBuilder builder = XContentFactory.contentBuilder(formatContentType, bStream);
                    builder.copyCurrentStructure(XContentFactory.xContent(contentType).createParser(source));
                    builder.close();
                    source = bStream.bytes();
                    // update the data in the context, so we store it in the translog in this format
                    context.source(source);
                }
            }
        }
        if (!source.hasArray()) {
            source = source.toBytesArray();
        }
        fields.add(new StoredField(names().indexName(), source.array(), source.arrayOffset(), source.length()));
    }

    @Override
    public byte[] value(Object value) {
        if (value == null) {
            return null;
        }
        BytesReference bValue;
        if (value instanceof BytesRef) {
            bValue = new BytesArray((BytesRef) value);
        } else {
            bValue = (BytesReference) value;
        }
        try {
            return CompressorFactory.uncompressIfNeeded(bValue).toBytes();
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to decompress source", e);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // all are defaults, no need to write it at all
        if (!includeDefaults && enabled == Defaults.ENABLED && compress == null && compressThreshold == -1 && includes == null && excludes == null) {
            return builder;
        }
        builder.startObject(contentType());
        if (includeDefaults || enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (includeDefaults || !Objects.equal(format, Defaults.FORMAT)) {
            builder.field("format", format);
        }
        if (compress != null) {
            builder.field("compress", compress);
        } else if (includeDefaults) {
            builder.field("compress", false);
        }
        if (compressThreshold != -1) {
            builder.field("compress_threshold", new ByteSizeValue(compressThreshold).toString());
        } else if (includeDefaults) {
            builder.field("compress_threshold", -1);
        }

        if (includes != null) {
            builder.field("includes", includes);
        } else if (includeDefaults) {
            builder.field("includes", Strings.EMPTY_ARRAY);
        }

        if (excludes != null) {
            builder.field("excludes", excludes);
        } else if (includeDefaults) {
            builder.field("excludes", Strings.EMPTY_ARRAY);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        SourceFieldMapper sourceMergeWith = (SourceFieldMapper) mergeWith;
        if (mergeResult.simulate()) {
            if (this.enabled != sourceMergeWith.enabled) {
                mergeResult.addConflict("Cannot update enabled setting for [_source]");
            }
            if (Arrays.equals(includes(), sourceMergeWith.includes()) == false) {
                mergeResult.addConflict("Cannot update includes setting for [_source]");
            }
            if (Arrays.equals(excludes(), sourceMergeWith.excludes()) == false) {
                mergeResult.addConflict("Cannot update excludes setting for [_source]");
            }
        } else {
            if (sourceMergeWith.compress != null) {
                this.compress = sourceMergeWith.compress;
            }
            if (sourceMergeWith.compressThreshold != -1) {
                this.compressThreshold = sourceMergeWith.compressThreshold;
            }
        }
    }
}
