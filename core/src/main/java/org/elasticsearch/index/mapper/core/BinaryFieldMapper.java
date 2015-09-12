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

package org.elasticsearch.index.mapper.core;

import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.MapperBuilders.binaryField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class BinaryFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "binary";
    private static final ParseField COMPRESS = new ParseField("compress").withAllDeprecated("no replacement, implemented at the codec level");
    private static final ParseField COMPRESS_THRESHOLD = new ParseField("compress_threshold").withAllDeprecated("no replacement");


    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new BinaryFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, BinaryFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public BinaryFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            ((BinaryFieldType)fieldType).setTryUncompressing(context.indexCreatedVersion().before(Version.V_2_0_0_beta1));
            return new BinaryFieldMapper(name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            BinaryFieldMapper.Builder builder = binaryField(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                if (parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1) &&
                        (parserContext.parseFieldMatcher().match(fieldName, COMPRESS) || parserContext.parseFieldMatcher().match(fieldName, COMPRESS_THRESHOLD))) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    static final class BinaryFieldType extends MappedFieldType {
        private boolean tryUncompressing = false;

        public BinaryFieldType() {}

        protected BinaryFieldType(BinaryFieldType ref) {
            super(ref);
            this.tryUncompressing = ref.tryUncompressing;
        }

        @Override
        public MappedFieldType clone() {
            return new BinaryFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            BinaryFieldType that = (BinaryFieldType) o;
            return Objects.equals(tryUncompressing, that.tryUncompressing);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), tryUncompressing);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            BinaryFieldType other = (BinaryFieldType)fieldType;
            if (tryUncompressing() != other.tryUncompressing()) {
                conflicts.add("mapper [" + names().fullName() + "] has different [try_uncompressing] (IMPOSSIBLE)");
            }
        }

        public boolean tryUncompressing() {
            return tryUncompressing;
        }

        public void setTryUncompressing(boolean tryUncompressing) {
            checkIfFrozen();
            this.tryUncompressing = tryUncompressing;
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
                    throw new ElasticsearchParseException("failed to convert bytes", e);
                }
            }
            try {
                if (tryUncompressing) { // backcompat behavior
                    return CompressorFactory.uncompressIfNeeded(bytes);
                } else {
                    return bytes;
                }
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to decompress source", e);
            }
        }

        @Override
        public Object valueForSearch(Object value) {
            return value(value);
        }
    }

    protected BinaryFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!fieldType().stored() && !fieldType().hasDocValues()) {
            return;
        }
        byte[] value = context.parseExternalValue(byte[].class);
        if (value == null) {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                return;
            } else {
                value = context.parser().binaryValue();
            }
        }
        if (value == null) {
            return;
        }
        if (fieldType().stored()) {
            fields.add(new Field(fieldType().names().indexName(), value, fieldType()));
        }

        if (fieldType().hasDocValues()) {
            CustomBinaryDocValuesField field = (CustomBinaryDocValuesField) context.doc().getByKey(fieldType().names().indexName());
            if (field == null) {
                field = new CustomBinaryDocValuesField(fieldType().names().indexName(), value);
                context.doc().addWithKey(fieldType().names().indexName(), field);
            } else {
                field.add(value);
            }
        }

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class CustomBinaryDocValuesField extends NumberFieldMapper.CustomNumericDocValuesField {

        private final ObjectArrayList<byte[]> bytesList;

        private int totalSize = 0;

        public CustomBinaryDocValuesField(String name, byte[] bytes) {
            super(name);
            bytesList = new ObjectArrayList<>();
            add(bytes);
        }

        public void add(byte[] bytes) {
            bytesList.add(bytes);
            totalSize += bytes.length;
        }

        @Override
        public BytesRef binaryValue() {
            try {
                CollectionUtils.sortAndDedup(bytesList);
                int size = bytesList.size();
                final byte[] bytes = new byte[totalSize + (size + 1) * 5];
                ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
                out.writeVInt(size);  // write total number of values
                for (int i = 0; i < size; i ++) {
                    final byte[] value = bytesList.get(i);
                    int valueLength = value.length;
                    out.writeVInt(valueLength);
                    out.writeBytes(value, 0, valueLength);
                }
                return new BytesRef(bytes, 0, out.getPosition());
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }

        }
    }
}
