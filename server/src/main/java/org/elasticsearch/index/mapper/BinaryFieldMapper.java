/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BinaryFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "binary";

    private static BinaryFieldMapper toType(FieldMapper in) {
        return (BinaryFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final boolean isSyntheticSourceEnabled;
        private final Parameter<Boolean> hasDocValues;

        public Builder(String name, boolean isSyntheticSourceEnabled) {
            super(name);
            this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
            this.hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, isSyntheticSourceEnabled);
        }

        @Override
        public Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta, stored, hasDocValues };
        }

        public BinaryFieldMapper.Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        @Override
        public BinaryFieldMapper build(MapperBuilderContext context) {
            return new BinaryFieldMapper(
                leafName(),
                new BinaryFieldType(context.buildFullName(leafName()), stored.getValue(), hasDocValues.getValue(), meta.getValue()),
                builderParams(this, context),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, SourceFieldMapper.isSynthetic(c.getIndexSettings())));

    public static final class BinaryFieldType extends MappedFieldType {
        private BinaryFieldType(String name, boolean isStored, boolean hasDocValues, Map<String, String> meta) {
            super(name, false, isStored, hasDocValues, TextSearchInfo.NONE, meta);
        }

        public BinaryFieldType(String name) {
            this(name, false, true, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.BINARY;
        }

        @Override
        public BytesReference valueForDisplay(Object value) {
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
                bytes = new BytesArray(Base64.getDecoder().decode(value.toString()));
            }
            return bytes;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new BytesBinaryIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Binary fields do not support searching");
        }
    }

    private final boolean stored;
    private final boolean hasDocValues;
    private final boolean isSyntheticSourceEnabled;

    protected BinaryFieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams builderParams, Builder builder) {
        super(simpleName, mappedFieldType, builderParams);
        this.stored = builder.stored.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.isSyntheticSourceEnabled = builder.isSyntheticSourceEnabled;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        if (stored == false && hasDocValues == false) {
            return;
        }
        if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }
        indexValue(context, context.parser().binaryValue());
    }

    public void indexValue(DocumentParserContext context, byte[] value) {
        if (value == null) {
            return;
        }
        if (stored) {
            context.doc().add(new StoredField(fieldType().name(), value));
        }

        if (hasDocValues) {
            CustomBinaryDocValuesField field = (CustomBinaryDocValuesField) context.doc().getByKey(fieldType().name());
            if (field == null) {
                field = new CustomBinaryDocValuesField(fieldType().name(), value);
                context.doc().addWithKey(fieldType().name(), field);
            } else {
                field.add(value);
            }
        } else {
            // Only add an entry to the field names field if the field is stored
            // but has no doc values so exists query will work on a field with
            // no doc values
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new BinaryFieldMapper.Builder(leafName(), isSyntheticSourceEnabled).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (hasDocValues) {
            return new SyntheticSourceSupport.Native(() -> new BinaryDocValuesSyntheticFieldLoader(fullPath()) {
                @Override
                protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
                    var in = new ByteArrayStreamInput();
                    in.reset(value.bytes, value.offset, value.length);

                    int count = in.readVInt();
                    switch (count) {
                        case 0:
                            return;
                        case 1:
                            b.field(leafName());
                            break;
                        default:
                            b.startArray(leafName());
                    }

                    for (int i = 0; i < count; i++) {
                        byte[] bytes = in.readByteArray();
                        b.value(Base64.getEncoder().encodeToString(bytes));
                    }

                    if (count > 1) {
                        b.endArray();
                    }
                }
            });
        }

        return super.syntheticSourceSupport();
    }

    public static final class CustomBinaryDocValuesField extends CustomDocValuesField {

        private final List<byte[]> bytesList;

        public CustomBinaryDocValuesField(String name, byte[] bytes) {
            super(name);
            bytesList = new ArrayList<>();
            add(bytes);
        }

        public void add(byte[] bytes) {
            bytesList.add(bytes);
        }

        @Override
        public BytesRef binaryValue() {
            try {
                bytesList.sort(Arrays::compareUnsigned);
                CollectionUtils.uniquify(bytesList, Arrays::compareUnsigned);
                int bytesSize = bytesList.stream().map(a -> a.length).reduce(0, Integer::sum);
                int n = bytesList.size();
                BytesStreamOutput out = new BytesStreamOutput(bytesSize + (n + 1) * 5);
                out.writeVInt(n);  // write total number of values
                for (var value : bytesList) {
                    int valueLength = value.length;
                    out.writeVInt(valueLength);
                    out.writeBytes(value, 0, valueLength);
                }
                return out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }

        }
    }
}
