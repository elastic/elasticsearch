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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.BinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.function.BooleanSupplier;

/**
 * A mapper for the {@code _id} field that reads the from the
 * {@link SourceToParse#id()}. It also supports field data
 * if the cluster is configured to allow it.
 */
public class ProvidedIdFieldMapper extends IdFieldMapper {
    public static final NodeFeature ID_FIELD_MODE_MAPPING_ATTRIBUTE = new NodeFeature("mapper.id_field.mode_mapping_attribute");
    public static final FeatureFlag ID_FIELD_MODE_FEATURE_FLAG = new FeatureFlag("id_field_mode");

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ProvidedIdFieldMapper.class);
    static final String ID_FIELD_DATA_DEPRECATION_MESSAGE =
        "Loading the fielddata on the _id field is deprecated and will be removed in future versions. "
            + "If you require sorting or aggregating on this field you should also include the id in the "
            + "body of your documents, and map this field as a keyword field that has [doc_values] enabled";

    public static final ProvidedIdFieldMapper DOCUMENT_ID_NO_FIELD_DATA = new ProvidedIdFieldMapper(
        () -> false,
        IdFieldMapper.Mode.DEFAULT
    );
    public static final ProvidedIdFieldMapper COLUMNAR_ID = new ProvidedIdFieldMapper(() -> false, Mode.COLUMNAR);

    /**
     * Builder for {@link ProvidedIdFieldMapper} that supports the {@code mode} mapping parameter.
     */
    public static class Builder extends MetadataFieldMapper.Builder {

        private final BooleanSupplier fieldDataEnabled;

        private final Parameter<IdFieldMapper.Mode> mode = new Parameter<>(
            "mode",
            false,
            () -> IdFieldMapper.Mode.DEFAULT,
            (n, c, o) -> IdFieldMapper.Mode.valueOf(o.toString().toUpperCase(Locale.ROOT)),
            m -> ((ProvidedIdFieldMapper) m).mode,
            (b, n, v) -> b.field(n, v.toString().toLowerCase(Locale.ROOT)),
            v -> v.toString().toLowerCase(Locale.ROOT)
        ).setSerializerCheck((includeDefaults, isConfigured, value) -> isConfigured);

        Builder(BooleanSupplier fieldDataEnabled) {
            super(NAME);
            this.fieldDataEnabled = fieldDataEnabled;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            if (ID_FIELD_MODE_FEATURE_FLAG.isEnabled()) {
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
                    return COLUMNAR_ID;
                case DEFAULT:
                    if (fieldDataEnabled.getAsBoolean()) {
                        return new ProvidedIdFieldMapper(fieldDataEnabled, mode);
                    } else {
                        return DOCUMENT_ID_NO_FIELD_DATA;
                    }
                default:
                    throw new IllegalArgumentException("Unsupported id field mode [" + mode + "]");
            }
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }
    }

    static final class IdFieldType extends AbstractIdFieldType {

        private final BooleanSupplier fieldDataEnabled;

        IdFieldType(BooleanSupplier fieldDataEnabled) {
            this.fieldDataEnabled = fieldDataEnabled;
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return true;
        }

        @Override
        public boolean isAggregatable() {
            return fieldDataEnabled.getAsBoolean();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            if (fieldDataEnabled.getAsBoolean() == false) {
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
            return new IndexFieldData.Builder() {
                @Override
                public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
                    deprecationLogger.warn(DeprecationCategory.AGGREGATIONS, "id_field_data", ID_FIELD_DATA_DEPRECATION_MESSAGE);
                    final IndexFieldData<?> fieldData = fieldDataBuilder.build(cache, breakerService);
                    return new IndexFieldData<>() {
                        @Override
                        public String getFieldName() {
                            return fieldData.getFieldName();
                        }

                        @Override
                        public ValuesSourceType getValuesSourceType() {
                            return fieldData.getValuesSourceType();
                        }

                        @Override
                        public LeafFieldData load(LeafReaderContext context) {
                            return wrap(fieldData.load(context));
                        }

                        @Override
                        public LeafFieldData loadDirect(LeafReaderContext context) throws Exception {
                            return wrap(fieldData.loadDirect(context));
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
                    };
                }
            };
        }
    }

    static final class ColumnarIdFieldType extends AbstractIdFieldType {

        public ColumnarIdFieldType() {
            super(true);
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return true;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return new BinaryIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD);
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.ID;
        }

    }

    private static LeafFieldData wrap(LeafFieldData in) {
        return new LeafFieldData() {

            @Override
            public long ramBytesUsed() {
                return in.ramBytesUsed();
            }

            @Override
            public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                return new DelegateDocValuesField(new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(getBytesValues())), name);
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                SortedBinaryDocValues inValues = in.getBytesValues();
                return new SortedBinaryDocValues() {

                    @Override
                    public BytesRef nextValue() throws IOException {
                        BytesRef encoded = inValues.nextValue();
                        return new BytesRef(
                            Uid.decodeId(Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length))
                        );
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
                };
            }
        };
    }

    private final BooleanSupplier fieldDataEnabled;
    private final IdFieldMapper.Mode mode;

    public ProvidedIdFieldMapper(BooleanSupplier fieldDataEnabled) {
        this(fieldDataEnabled, IdFieldMapper.Mode.DEFAULT);
    }

    public ProvidedIdFieldMapper(BooleanSupplier fieldDataEnabled, IdFieldMapper.Mode mode) {
        super(mode == Mode.COLUMNAR ? new ColumnarIdFieldType() : new IdFieldType(fieldDataEnabled));
        this.fieldDataEnabled = fieldDataEnabled;
        this.mode = mode;
    }

    BooleanSupplier fieldDataEnabled() {
        return fieldDataEnabled;
    }

    @Override
    public boolean isColumnarMode() {
        return mode == IdFieldMapper.Mode.COLUMNAR;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        Builder builder = new Builder(fieldDataEnabled);
        builder.init(this);
        return builder;
    }

    @Override
    public void preParse(DocumentParserContext context) {
        if (context.sourceToParse().id() == null) {
            throw new IllegalStateException("_id should have been set on the coordinating node");
        }
        context.id(context.sourceToParse().id());
        if (mode == IdFieldMapper.Mode.COLUMNAR) {
            BytesRef encoded = Uid.encodeId(context.id());
            context.doc().add(standardIdField(encoded, Field.Store.NO));
            context.doc().add(new BinaryDocValuesField(NAME, encoded));
        } else {
            context.doc().add(standardIdField(context.id()));
        }
    }

    @Override
    public void postParse(DocumentParserContext context) {
        if (mode == IdFieldMapper.Mode.COLUMNAR) {
            // Nested child documents are in the same Lucene updateDocuments batch as the root document.
            // Lucene requires all documents in a batch to have a consistent field schema, so nested
            // children must also carry the sorted doc values field for _id.
            BytesRef encoded = Uid.encodeId(context.id());
            for (LuceneDocument doc : context.nonRootDocuments()) {
                doc.add(new BinaryDocValuesField(NAME, encoded));
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

    @Override
    public String reindexId(String id) {
        return id;
    }
}
