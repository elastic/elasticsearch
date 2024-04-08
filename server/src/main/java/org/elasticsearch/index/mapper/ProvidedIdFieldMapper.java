/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
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
import java.util.Arrays;
import java.util.function.BooleanSupplier;

/**
 * A mapper for the {@code _id} field that reads the from the
 * {@link SourceToParse#id()}. It also supports field data
 * if the cluster is configured to allow it.
 */
public class ProvidedIdFieldMapper extends IdFieldMapper {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ProvidedIdFieldMapper.class);
    static final String ID_FIELD_DATA_DEPRECATION_MESSAGE =
        "Loading the fielddata on the _id field is deprecated and will be removed in future versions. "
            + "If you require sorting or aggregating on this field you should also include the id in the "
            + "body of your documents, and map this field as a keyword field that has [doc_values] enabled";

    public static final ProvidedIdFieldMapper NO_FIELD_DATA = new ProvidedIdFieldMapper(() -> false);

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

    private static LeafFieldData wrap(LeafFieldData in) {
        return new LeafFieldData() {

            @Override
            public void close() {
                in.close();
            }

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

    public ProvidedIdFieldMapper(BooleanSupplier fieldDataEnabled) {
        super(new IdFieldType(fieldDataEnabled));
    }

    @Override
    public void preParse(DocumentParserContext context) {
        if (context.sourceToParse().id() == null) {
            throw new IllegalStateException("_id should have been set on the coordinating node");
        }
        context.id(context.sourceToParse().id());
        context.doc().add(standardIdField(context.id()));
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
