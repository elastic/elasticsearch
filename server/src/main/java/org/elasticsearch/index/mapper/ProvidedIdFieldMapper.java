/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.BinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Locale;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * A mapper for the {@code _id} field that reads the from the
 * {@link SourceToParse#id()}. It also supports field data
 * if the cluster is configured to allow it.
 */
public class ProvidedIdFieldMapper extends IdFieldMapper {
    public static final NodeFeature ID_FIELD_MODE_MAPPING_ATTRIBUTE = new NodeFeature("mapper.id_field.mode_mapping_attribute");

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ProvidedIdFieldMapper.class);
    static final String ID_FIELD_DATA_DEPRECATION_MESSAGE =
        "Loading the fielddata on the _id field is deprecated and will be removed in future versions. "
            + "If you require sorting or aggregating on this field you should also include the id in the "
            + "body of your documents, and map this field as a keyword field that has [doc_values] enabled";

    public static final ProvidedIdFieldMapper DOCUMENT_ID = new ProvidedIdFieldMapper(Mode.DOCUMENT, false);
    static final ProvidedIdFieldMapper DOCUMENT_ID_DEFAULT_COLUMNAR = new ProvidedIdFieldMapper(Mode.DOCUMENT, true);
    static final ProvidedIdFieldMapper COLUMNAR_ID = new ProvidedIdFieldMapper(Mode.COLUMNAR, false);
    static final ProvidedIdFieldMapper COLUMNAR_ID_DEFAULT_COLUMNAR = new ProvidedIdFieldMapper(Mode.COLUMNAR, true);

    /**
     * Builder for {@link ProvidedIdFieldMapper} that supports the {@code mode} mapping parameter.
     */
    public static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Mode> mode;
        private final boolean columnarIdByDefault;

        Builder(boolean columnarIdByDefault) {
            super(NAME);
            mode = Parameter.enumParam(
                "mode",
                false,
                m -> ((ProvidedIdFieldMapper) m).mode,
                (Supplier<Mode>) () -> columnarIdByDefault ? Mode.COLUMNAR : Mode.DOCUMENT,
                Mode.class
            );
            this.columnarIdByDefault = columnarIdByDefault;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            if (IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled()) {
                return new Parameter<?>[] { mode };
            } else {
                return new Parameter<?>[] {};
            }
        }

        @Override
        public MetadataFieldMapper build() {
            var mode = this.mode.getValue();
            switch (mode) {
                case COLUMNAR:
                    return columnarIdByDefault ? COLUMNAR_ID_DEFAULT_COLUMNAR : COLUMNAR_ID;
                case DOCUMENT:
                    return columnarIdByDefault ? DOCUMENT_ID_DEFAULT_COLUMNAR : DOCUMENT_ID;
                default:
                    throw new IllegalArgumentException("Unsupported id field mode [" + mode + "]");
            }
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }
    }

    /**
     * Controls how the {@code _id} field is stored and retrieved.
     * <ul>
     *   <li>{@link #DOCUMENT} – document behaviour: stored as a stored field with an inverted index.</li>
     *   <li>{@link #COLUMNAR} – columnar behaviour: stored as sorted doc values with an inverted index,
     *       no stored field. Intended for use with the columnar index mode.</li>
     * </ul>
     */
    public enum Mode {
        DOCUMENT,
        COLUMNAR;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    static final class IdFieldType extends AbstractIdFieldType {

        IdFieldType() {}

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return true;
        }

        @Override
        public boolean isAggregatable(BooleanSupplier idFieldDataEnabled) {
            return idFieldDataEnabled.getAsBoolean();
        }

        @Override
        public boolean isAggregatable() {
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            final boolean idFieldDataEnabled = fieldDataContext.idFieldDataEnabled().getAsBoolean();
            if (idFieldDataEnabled == false) {
                throw new IllegalArgumentException(
                    "Fielddata access on the _id field is disallowed, "
                        + "you can re-enable it by updating the dynamic cluster setting: "
                        + IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()
                );
            }
            final IndexFieldData.Builder fieldDataBuilder = new PagedBytesIndexFieldData.Builder(
                name(),
                TextFieldMapper.Defaults.FIELDDATA_MIN_FREQUENCY,
                TextFieldMapper.Defaults.FIELDDATA_MAX_FREQUENCY,
                TextFieldMapper.Defaults.FIELDDATA_MIN_SEGMENT_SIZE,
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new DelegateDocValuesField(
                    new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(FieldData.toString(dv))),
                    n
                )
            );
            return (cache, breakerService) -> {
                deprecationLogger.warn(DeprecationCategory.AGGREGATIONS, "id_field_data", ID_FIELD_DATA_DEPRECATION_MESSAGE);
                return new WrappingIdFieldData(fieldDataBuilder.build(cache, breakerService));
            };
        }
    }

    static final class ColumnarIdFieldType extends AbstractIdFieldType {

        ColumnarIdFieldType() {
            super(true);
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return true;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            var actualFieldDataBuilder = new BinaryIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD);
            return (cache, breakerService) -> new WrappingIdFieldData(actualFieldDataBuilder.build(cache, breakerService));
        }

    }

    private static final class WrappingIdFieldData implements IndexFieldData<LeafFieldData> {
        private final IndexFieldData<?> delegate;

        WrappingIdFieldData(IndexFieldData<?> delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getFieldName() {
            return delegate.getFieldName();
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return delegate.getValuesSourceType();
        }

        @Override
        public LeafFieldData load(LeafReaderContext context) {
            return wrap(delegate.load(context));
        }

        @Override
        public LeafFieldData loadDirect(LeafReaderContext context) throws Exception {
            return wrap(delegate.loadDirect(context));
        }

        @Override
        public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
            XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
            return new SortField(getFieldName(), source, reverse);
        }

        @Override
        public BucketedSort newBucketedSort(
            BigArrays bigArrays,
            Object missingValue,
            MultiValueMode sortMode,
            Nested nested,
            SortOrder sortOrder,
            DocValueFormat format,
            int bucketSize,
            BucketedSort.ExtraData extra
        ) {
            throw new UnsupportedOperationException("can't sort on the [" + CONTENT_TYPE + "] field");
        }

        private static LeafFieldData wrap(LeafFieldData in) {
            return new LeafFieldData() {

                @Override
                public long ramBytesUsed() {
                    return in.ramBytesUsed();
                }

                @Override
                public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                    return new DelegateDocValuesField(
                        new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(getBytesValues())),
                        name
                    );
                }

                @Override
                public SortedBinaryDocValues getBytesValues() {
                    SortedBinaryDocValues inValues = in.getBytesValues();
                    return new SortedBinaryDocValues(inValues.docIdIterator()) {

                        @Override
                        public BytesRef nextValue() throws IOException {
                            BytesRef encoded = inValues.nextValue();
                            String decoded = Uid.decodeId(encoded);
                            return new BytesRef(decoded);
                        }

                        @Override
                        public int docValueCount() {
                            final int count = inValues.docValueCount();
                            // If the count is not 1 then the impl is not correct as the binary representation
                            // does not preserve order. But id fields only have one value per doc so we are good.
                            assert count == 1;
                            return inValues.docValueCount();
                        }

                        @Override
                        public boolean advanceExact(int doc) throws IOException {
                            return inValues.advanceExact(doc);
                        }

                        @Override
                        public Sparsity getSparsity() {
                            return inValues.getSparsity();
                        }

                        @Override
                        public ValueMode getValueMode() {
                            return inValues.getValueMode();
                        }
                    };
                }
            };
        }
    }

    private final Mode mode;
    private final boolean columnarIdByDefault;

    public ProvidedIdFieldMapper(Mode mode, boolean columnarIdByDefault) {
        super(mode == Mode.COLUMNAR ? new ColumnarIdFieldType() : new IdFieldType());
        this.mode = mode;
        this.columnarIdByDefault = columnarIdByDefault;
    }

    @Override
    public boolean isColumnarMode() {
        return mode == Mode.COLUMNAR;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        Builder builder = new Builder(columnarIdByDefault);
        builder.init(this);
        return builder;
    }

    @Override
    public void preParse(DocumentParserContext context) {
        if (context.sourceToParse().id() == null) {
            throw new IllegalStateException("_id should have been set on the coordinating node");
        }
        context.id(context.sourceToParse().id());
        if (mode == Mode.COLUMNAR) {
            context.doc().add(columnarIdField(context.id()));
        } else {
            context.doc().add(standardIdField(context.id()));
        }
    }

    @Override
    public void postParse(DocumentParserContext context) {
        if (mode == Mode.COLUMNAR) {
            // Nested child documents are in the same Lucene updateDocuments batch as the root document.
            // Lucene requires all documents in a batch to have a consistent field schema, so nested
            // children must also carry the binary doc values field for _id.
            var iterator = context.nonRootDocuments().iterator();
            if (iterator.hasNext()) {
                BytesRef encoded = Uid.encodeId(context.id());
                while (iterator.hasNext()) {
                    var doc = iterator.next();
                    doc.add(new BinaryDocValuesField(NAME, encoded));
                }
            }
        }
    }

    @Override
    public String documentDescription(DocumentParserContext context) {
        return "document with id '" + context.sourceToParse().id() + "'";
    }

    @Override
    public String documentDescription(ParsedDocument parsedDocument) {
        return "[" + parsedDocument.id() + "]";
    }

    public static IndexableField columnarIdField(String id) {
        BytesRef encoded = Uid.encodeId(id);
        return new ColumnarIdField(NAME, encoded);
    }

    static final class ColumnarIdField extends Field {

        static final FieldType TYPE = new FieldType();

        static {
            TYPE.setOmitNorms(true);
            TYPE.setIndexOptions(IndexOptions.DOCS);
            TYPE.setTokenized(false);
            TYPE.setDocValuesType(DocValuesType.BINARY);
            TYPE.freeze();
        }

        private BytesRef binaryValue;

        ColumnarIdField(String name, BytesRef value) {
            super(name, value, TYPE);
            binaryValue = value;
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.BINARY;
        }

        @Override
        public BytesRef binaryValue() {
            return binaryValue;
        }

        @Override
        public void setStringValue(String value) {
            super.setStringValue(value);
            binaryValue = new BytesRef(value);
        }

        @Override
        public void setBytesValue(BytesRef value) {
            super.setBytesValue(value);
            binaryValue = value;
        }

        @Override
        public StoredValue storedValue() {
            return null;
        }

    }
}
