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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.binaryField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class BinaryFieldMapper extends AbstractFieldMapper<BytesReference> {

    public static final String CONTENT_TYPE = "binary";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final long COMPRESS_THRESHOLD = -1;
        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setIndexed(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, BinaryFieldMapper> {

        private Boolean compress = null;

        private long compressThreshold = Defaults.COMPRESS_THRESHOLD;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
        }

        public Builder compress(boolean compress) {
            this.compress = compress;
            return this;
        }

        public Builder compressThreshold(long compressThreshold) {
            this.compressThreshold = compressThreshold;
            return this;
        }

        @Override
        public Builder indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override
        public BinaryFieldMapper build(BuilderContext context) {
            return new BinaryFieldMapper(buildNames(context), fieldType, compress, compressThreshold, provider);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            BinaryFieldMapper.Builder builder = binaryField(name);
            parseField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("compress") && fieldNode != null) {
                    builder.compress(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("compress_threshold") && fieldNode != null) {
                    if (fieldNode instanceof Number) {
                        builder.compressThreshold(((Number) fieldNode).longValue());
                        builder.compress(true);
                    } else {
                        builder.compressThreshold(ByteSizeValue.parseBytesSizeValue(fieldNode.toString()).bytes());
                        builder.compress(true);
                    }
                }
            }
            return builder;
        }
    }

    private Boolean compress;

    private long compressThreshold;

    protected BinaryFieldMapper(Names names, FieldType fieldType, Boolean compress, long compressThreshold, PostingsFormatProvider provider) {
        super(names, 1.0f, fieldType, null, null, provider, null, null);
        this.compress = compress;
        this.compressThreshold = compressThreshold;
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
    public Object valueForSearch(Object value) {
        return value(value);
    }

    @Override
    public BytesReference value(Object value) {
        if (value == null) {
            return null;
        }

        BytesReference bytes;
        if (value instanceof BytesRef) {
            bytes = new BytesArray((BytesRef) value);
        } else if (value instanceof BytesReference) {
            bytes = (BytesReference) value;
        } else if (value instanceof byte[]) {
            bytes = new BytesArray((byte[]) value);
        } else {
            try {
                bytes = new BytesArray(Base64.decode(value.toString()));
            } catch (IOException e) {
                throw new ElasticSearchParseException("failed to convert bytes", e);
            }
        }
        try {
            return CompressorFactory.uncompressIfNeeded(bytes);
        } catch (IOException e) {
            throw new ElasticSearchParseException("failed to decompress source", e);
        }
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        if (!fieldType().stored()) {
            return null;
        }
        byte[] value;
        if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        } else {
            value = context.parser().binaryValue();
            if (compress != null && compress && !CompressorFactory.isCompressed(value, 0, value.length)) {
                if (compressThreshold == -1 || value.length > compressThreshold) {
                    BytesStreamOutput bStream = new BytesStreamOutput();
                    StreamOutput stream = CompressorFactory.defaultCompressor().streamOutput(bStream);
                    stream.writeBytes(value, 0, value.length);
                    stream.close();
                    value = bStream.bytes().toBytes();
                }
            }
        }
        if (value == null) {
            return null;
        }
        return new Field(names.indexName(), value, fieldType);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        if (includeDefaults || !names.name().equals(names.indexNameClean())) {
            builder.field("index_name", names.indexNameClean());
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
        if (includeDefaults || fieldType.stored() != defaultFieldType().stored()) {
            builder.field("store", fieldType.stored());
        }
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        BinaryFieldMapper sourceMergeWith = (BinaryFieldMapper) mergeWith;
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