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

package org.elasticsearch.index.mapper.internal;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.lzf.LZF;
import org.elasticsearch.common.compress.lzf.LZFDecoder;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.LZFStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.document.ResetFieldSelector;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

/**
 * @author kimchy (shay.banon)
 */
public class SourceFieldMapper extends AbstractFieldMapper<byte[]> implements InternalMapper, RootMapper {

    public static final String NAME = "_source";

    public static final String CONTENT_TYPE = "_source";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = SourceFieldMapper.NAME;
        public static final boolean ENABLED = true;
        public static final long COMPRESS_THRESHOLD = -1;
        public static final Field.Index INDEX = Field.Index.NO;
        public static final Field.Store STORE = Field.Store.YES;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends Mapper.Builder<Builder, SourceFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        private long compressThreshold = Defaults.COMPRESS_THRESHOLD;

        private Boolean compress = null;

        private List<String> excludeFields;

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

        public Builder excludeFields(List<String> excludeFields) {
            this.excludeFields = excludeFields;
            return this;
        }

        @Override public SourceFieldMapper build(BuilderContext context) {
            return new SourceFieldMapper(name, enabled, compress, compressThreshold, excludeFields);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
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
                } else if (fieldName.equals("excludes") && fieldNode != null) {
                    List<Object> excludes = (List<Object>)fieldNode;
                    for (Object exclude : excludes) {
                        if (exclude instanceof String == false) {
                            throw new MapperParsingException("Property exclude only handle field names as Strings.");
                        }
                    }
                    builder.excludeFields((List<String>)fieldNode);
                }
            }
            return builder;
        }
    }


    private final boolean enabled;

    private Boolean compress;

    private long compressThreshold;

    private List<String> excludeFields;

    public SourceFieldMapper() {
        this(Defaults.NAME, Defaults.ENABLED, null, -1, null);
    }

    protected SourceFieldMapper(String name, boolean enabled, Boolean compress, long compressThreshold, List<String> excludeFields) {
        super(new Names(name, name, name, name), Defaults.INDEX, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.enabled = enabled;
        this.compress = compress;
        this.compressThreshold = compressThreshold;
        this.excludeFields = excludeFields;
    }

    public boolean enabled() {
        return this.enabled;
    }

    public ResetFieldSelector fieldSelector() {
        return SourceFieldSelector.INSTANCE;
    }

    @Override public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override public void postParse(ParseContext context) throws IOException {
    }

    @Override public void parse(ParseContext context) throws IOException {
        // nothing to do here, we will call it in pre parse
    }

    @Override public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override public boolean includeInObject() {
        return false;
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
        int dataOffset = context.sourceOffset();
        int dataLength = context.sourceLength();

        if (excludeFields != null) {
            Map<String, Object> dataMap = XContentFactory.xContent(context.source()).createParser(context.source()).mapAndClose();
            for (String field : excludeFields) {
                if (field.indexOf('.') != -1) {
                    /* Diving into the objects to find the property to delete */
                    String[] path = field.split("\\.");
                    Map<String, Object> node = null;
                    for (int i = 0; i < path.length; i++) {
                        if (i == path.length - 1) {
                            node.remove(path[i]);
                        } else {
                            Object tmpnode = dataMap.get(path[i]);
                            if (tmpnode != null && tmpnode instanceof Map) {
                                node = (Map<String, Object>) tmpnode;
                            } else {
                                break;
                            }
                        }
                    }
                } else {
                    dataMap.remove(field);
                }
            }
            data = XContentFactory.jsonBuilder().map(dataMap).copiedBytes();
            dataOffset = 0;
            dataLength = data.length;
            context.source(data, dataOffset, dataLength);
        }

        if (compress != null && compress && !LZF.isCompressed(data, dataOffset, dataLength)) {
            if (compressThreshold == -1 || dataLength > compressThreshold) {
                CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
                LZFStreamOutput streamOutput = cachedEntry.cachedLZFBytes();
                streamOutput.writeBytes(data, dataOffset, dataLength);
                streamOutput.flush();
                // we copy over the byte array, since we need to push back the cached entry
                // TODO, we we had a handle into when we are done with parsing, then we push back then and not copy over bytes
                data = cachedEntry.bytes().copiedByteArray();
                dataOffset = 0;
                dataLength = data.length;
                CachedStreamOutput.pushEntry(cachedEntry);
                // update the data in the context, so it can be compressed and stored compressed outside...
                context.source(data, dataOffset, dataLength);
            }
        }
        return new Field(names().indexName(), data, dataOffset, dataLength);
    }

    public byte[] value(Document document) {
        Fieldable field = document.getFieldable(names.indexName());
        return field == null ? null : value(field);
    }

    public byte[] nativeValue(Fieldable field) {
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
        if (excludeFields != null) {
            builder.field("excludes", excludeFields);
        }
        builder.endObject();
        return builder;
    }

    @Override public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
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
