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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.MultiValuedSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.MultiValuedBinaryDVLeafFieldData;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ValueOrdering;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.BinaryDocValuesField;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
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
        /** The index a mapped field belongs to, or {@code null} for a field another mapper builds for itself. */
        @Nullable
        private final IndexSettings indexSettings;
        private final DocValuesParameter docValues;

        /**
         * A binary field another mapper builds for its own use. It keeps the layout {@link CustomBinaryDocValuesField} writes
         * whatever the index, since its owner reads it back that way.
         */
        public Builder(String name, boolean isSyntheticSourceEnabled) {
            this(name, null, isSyntheticSourceEnabled);
        }

        /** A binary field of the mapping, laid out as its index stores binary doc values. */
        public Builder(String name, IndexSettings indexSettings) {
            this(name, indexSettings, SourceFieldMapper.isSynthetic(indexSettings));
        }

        private Builder(String name, @Nullable IndexSettings indexSettings, boolean isSyntheticSourceEnabled) {
            super(name);
            this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
            this.indexSettings = indexSettings;
            final DocValuesParameter.Values nonColumnarDefault = isSyntheticSourceEnabled
                ? DocValuesParameter.Values.ENABLED_HIGH_CARDINALITY
                : DocValuesParameter.Values.DISABLED_HIGH_CARDINALITY;
            this.docValues = DocValuesParameter.of(
                indexSettings == null
                    ? nonColumnarDefault
                    : DocValuesParameter.defaultValues(indexSettings, nonColumnarDefault, DocValuesParameter.Values.Cardinality.HIGH),
                m -> toType(m).docValues,
                indexSettings
            );
        }

        @Override
        public Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta, stored, docValues, docValues.multiValue(), docValues.nullability(), docValues.onFailure() };
        }

        public BinaryFieldMapper.Builder docValues(boolean hasDocValues) {
            this.docValues.setValue(
                hasDocValues ? DocValuesParameter.Values.ENABLED_HIGH_CARDINALITY : DocValuesParameter.Values.DISABLED_HIGH_CARDINALITY
            );
            return this;
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        /**
         * How the field's doc values are framed when the ColumNAR codec stores them, or {@code null} for the layout
         * {@link CustomBinaryDocValuesField} writes.
         */
        @Nullable
        private BinaryDocValuesFormat columnarFormat() {
            final DocValuesParameter.Values values = docValues.getValue();
            if (values.enabled() == false
                || indexSettings == null
                || ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings) == false) {
                return null;
            }
            return values.multiValue() ? BinaryDocValuesFormat.COLUMNAR_PAYLOAD : BinaryDocValuesFormat.PLAIN;
        }

        @Override
        public BinaryFieldMapper build(MapperBuilderContext context) {
            return new BinaryFieldMapper(
                leafName(),
                new BinaryFieldType(
                    context.buildFullName(leafName()),
                    stored.getValue(),
                    docValues.getValue().enabled(),
                    columnarFormat(),
                    meta.getValue()
                ),
                builderParams(this, context),
                this,
                context.isSourceColumnarStored()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.getIndexSettings()));

    public static final class BinaryFieldType extends MappedFieldType {
        @Nullable
        private final BinaryDocValuesFormat columnarFormat;

        private BinaryFieldType(
            String name,
            boolean isStored,
            boolean hasDocValues,
            @Nullable BinaryDocValuesFormat columnarFormat,
            Map<String, String> meta
        ) {
            super(name, hasDocValues ? IndexType.docValuesOnly() : IndexType.NONE, isStored, meta);
            this.columnarFormat = columnarFormat;
        }

        public BinaryFieldType(String name) {
            this(name, false, true, null, Collections.emptyMap());
        }

        /**
         * How the doc values are framed when the ColumNAR codec stores them: {@link BinaryDocValuesFormat#COLUMNAR_PAYLOAD}, or
         * {@link BinaryDocValuesFormat#PLAIN} under {@code multi_value: false}. {@code null} for the layout
         * {@link CustomBinaryDocValuesField} writes.
         */
        @Nullable
        public BinaryDocValuesFormat columnarFormat() {
            return columnarFormat;
        }

        /** The field's values for one segment, read in the layout they were written in. */
        SortableBinaryDocValues values(LeafReader leafReader) throws IOException {
            return columnarFormat == null
                ? MultiValuedSortableBinaryDocValues.fromMultiValued(leafReader, name())
                : SortableBinaryDocValues.forFormat(leafReader, name(), IndexVersion.current(), columnarFormat);
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
            if (columnarFormat != null) {
                return new BytesBinaryIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.KEYWORD,
                    BinaryDocValuesField::new,
                    IndexVersion.current(),
                    columnarFormat
                );
            }
            return (cache, breakerService) -> new CustomBinaryIndexFieldData(name(), BinaryDocValuesField::new);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Binary fields do not support searching");
        }
    }

    private final boolean stored;
    private final DocValuesParameter.Values docValues;
    private final boolean hasDocValues;
    private final boolean isSyntheticSourceEnabled;
    @Nullable
    private final IndexSettings indexSettings;
    /**
     * Whether the values are left out of the {@code columnar_stored} {@code _source} and patched back from doc values on read: only
     * for the ColumNAR layouts, which keep array order and duplicates, and not under {@code on_failure: ignore}, whose set-aside
     * values only the source blob keeps.
     */
    private final boolean patchedIntoSource;

    protected BinaryFieldMapper(
        String simpleName,
        BinaryFieldType mappedFieldType,
        BuilderParams builderParams,
        Builder builder,
        boolean sourceColumnarStored
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.stored = builder.stored.getValue();
        this.docValues = builder.docValues.getValue();
        this.hasDocValues = docValues.enabled();
        this.isSyntheticSourceEnabled = builder.isSyntheticSourceEnabled;
        this.indexSettings = builder.indexSettings;
        this.patchedIntoSource = sourceColumnarStored
            && hasDocValues
            && mappedFieldType.columnarFormat() != null
            && docValues.onFailure() != DocValuesParameter.Values.OnFailure.IGNORE;
    }

    @Override
    public BinaryFieldType fieldType() {
        return (BinaryFieldType) super.fieldType();
    }

    @Override
    protected boolean shouldEnforceSingleValue(XContentParser.Token token) {
        return docValues.multiValue() == false && token != XContentParser.Token.VALUE_NULL;
    }

    @Override
    protected DocValuesParameter.Values.OnFailure onFailureBehavior() {
        return docValues.onFailure();
    }

    @Override
    public boolean isNullable() {
        return docValues.nullability();
    }

    @Override
    public StringColumnOptions columnarStringOptions() {
        final BinaryDocValuesFormat format = fieldType().columnarFormat();
        if (format == null) {
            return null;
        }
        return switch (format) {
            case COLUMNAR_PAYLOAD -> StringColumnOptions.PAYLOADS;
            case PLAIN -> StringColumnOptions.PAYLOADS.withSingleValued(true);
            case SEPARATE_COUNT, ARRAY_ORDER_INLINE_NULL -> throw new IllegalStateException(
                "binary field [" + fullPath() + "] has no columnar format [" + format + "]"
            );
        };
    }

    @Override
    public SourceLoader.SyntheticVectorsLoader syntheticVectorsLoader() {
        if (patchedIntoSource == false) {
            return null;
        }
        return context -> {
            final SortableBinaryDocValues values = fieldType().values(context.reader());
            return (doc, acc) -> {
                if (values.advanceExact(doc) == false) {
                    return;
                }
                final int count = values.docValueCount();
                if (count == 1) {
                    acc.add(new SourceLoader.LeafSyntheticVectorPath(fullPath(), base64(values.nextValue())));
                    return;
                }
                final List<String> list = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    list.add(base64(values.nextValue()));
                }
                acc.add(new SourceLoader.LeafSyntheticVectorPath(fullPath(), list));
            };
        };
    }

    private static String base64(BytesRef value) {
        return Base64.getEncoder().encodeToString(BytesRef.deepCopyOf(value).bytes);
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
            final BinaryDocValuesFormat format = fieldType().columnarFormat();
            if (format == null) {
                CustomBinaryDocValuesField field = (CustomBinaryDocValuesField) context.doc().getByKey(fieldType().name());
                if (field == null) {
                    field = new CustomBinaryDocValuesField(fieldType().name(), value);
                    context.doc().addWithKey(fieldType().name(), field);
                } else {
                    field.add(value);
                }
                return;
            }
            final BytesRef bytes = new BytesRef(value);
            switch (format) {
                // Values are kept in the order given, duplicates included, so the array round-trips.
                case COLUMNAR_PAYLOAD -> {
                    if (context.isPartOfArray() == false) {
                        ColumnarBinaryDocValuesField.recordSingleValue(context.doc(), fieldType().name(), bytes, ValueOrdering.UNSORTED);
                    } else {
                        ColumnarBinaryDocValuesField.recordValue(context.doc(), fieldType().name(), bytes, ValueOrdering.UNSORTED);
                    }
                }
                case PLAIN -> context.doc().add(new org.apache.lucene.document.BinaryDocValuesField(fieldType().name(), bytes));
                case SEPARATE_COUNT, ARRAY_ORDER_INLINE_NULL -> throw new IllegalStateException(
                    "binary field [" + fullPath() + "] has no columnar format [" + format + "]"
                );
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
        return new BinaryFieldMapper.Builder(leafName(), indexSettings, isSyntheticSourceEnabled).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        final BinaryDocValuesFormat format = fieldType().columnarFormat();
        if (hasDocValues && format != null) {
            return new SyntheticSourceSupport.Native(() -> {
                final List<CompositeSyntheticFieldLoader.Layer> layers = new ArrayList<>(2);
                layers.add(switch (format) {
                    case COLUMNAR_PAYLOAD -> new ColumnarPayloadBinaryDocValuesSyntheticFieldLoaderLayer(fullPath()) {
                        @Override
                        protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
                            b.value(base64(value));
                        }
                    };
                    case PLAIN -> new BinaryDocValuesSyntheticFieldLoaderLayer(fullPath(), IndexVersion.current()) {
                        @Override
                        protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
                            b.value(base64(value));
                        }
                    };
                    case SEPARATE_COUNT, ARRAY_ORDER_INLINE_NULL -> throw new IllegalStateException(
                        "binary field [" + fullPath() + "] has no columnar format [" + format + "]"
                    );
                });
                if (onFailureColumnEnabled()) {
                    layers.add(CompositeSyntheticFieldLoader.onFailureValuesLayer(fullPath(), IndexVersion.current()));
                }
                return new CompositeSyntheticFieldLoader(leafName(), fullPath(), layers);
            });
        }
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

    /**
     * This class is necessary because BinaryFieldMapper uses its own binary encoding format, matching that of
     * {@link MultiValuedBinaryDocValuesField.IntegratedCount}.
     */
    private static final class CustomBinaryDVLeafFieldData extends MultiValuedBinaryDVLeafFieldData {
        private final LeafReader leafReader;
        private final String fieldName;

        CustomBinaryDVLeafFieldData(
            String fieldName,
            LeafReader leafReader,
            ToScriptFieldFactory<SortableBinaryDocValues> toScriptFieldFactory
        ) {
            super(fieldName, leafReader, toScriptFieldFactory, IndexVersion.current());
            this.leafReader = leafReader;
            this.fieldName = fieldName;
        }

        @Override
        public SortableBinaryDocValues getBytesValues() {
            try {
                return MultiValuedSortableBinaryDocValues.fromMultiValued(leafReader, fieldName);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static final class CustomBinaryIndexFieldData extends BytesBinaryIndexFieldData {
        CustomBinaryIndexFieldData(String fieldName, ToScriptFieldFactory<SortableBinaryDocValues> toScriptFieldFactory) {
            super(fieldName, CoreValuesSourceType.KEYWORD, toScriptFieldFactory, IndexVersion.current());
        }

        @Override
        public MultiValuedBinaryDVLeafFieldData load(LeafReaderContext context) {
            return new CustomBinaryDVLeafFieldData(fieldName, context.reader(), toScriptFieldFactory);
        }
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
                // avoiding a streaming map/reduce for efficiency reasons
                int bytesSize = 0;
                for (byte[] bytes : bytesList) {
                    bytesSize += bytes.length;
                }
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
