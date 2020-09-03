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
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import static org.elasticsearch.search.DocValueFormat.withNanosecondResolution;

/**
 * Fetch sub phase which pulls data from doc values.
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class FetchDocValuesPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(SearchContext context) throws IOException {
        if (context.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = context.collapse().getFieldName();
            if (context.docValuesContext() == null) {
                context.docValuesContext(new FetchDocValuesContext(
                    Collections.singletonList(new FieldAndFormat(name, null))));
            } else if (context.docValuesContext().fields().stream().map(ff -> ff.field).anyMatch(name::equals) == false) {
                context.docValuesContext().fields().add(new FieldAndFormat(name, null));
            }
        }

        if (context.docValuesContext() == null) {
            return null;
        }

        List<DocValueField> fields = new ArrayList<>();
        for (FieldAndFormat fieldAndFormat : context.docValuesContext().fields()) {
            DocValueField f = buildField(context, fieldAndFormat);
            if (f != null) {
                fields.add(f);
            }
        }

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                for (DocValueField f : fields) {
                    f.setNextReader(readerContext);
                }
            }

            @Override
            public void process(HitContext hit) throws IOException {
                for (DocValueField f : fields) {
                    DocumentField hitField = hit.hit().field(f.field);
                    if (hitField == null) {
                        hitField = new DocumentField(f.field, new ArrayList<>(2));
                        // even if we request a doc values of a meta-field (e.g. _routing),
                        // docValues fields will still be document fields, and put under "fields" section of a hit.
                        hit.hit().setDocumentField(f.field, hitField);
                    }
                    f.setValues(hit.docId(), hitField);
                }
            }
        };
    }

    private abstract static class DocValueField {

        final String field;
        final DocValueFormat format;

        protected DocValueField(String field, DocValueFormat format) {
            this.field = field;
            this.format = format;
        }

        abstract void setNextReader(LeafReaderContext context);
        abstract void setValues(int doc, DocumentField hitField) throws IOException;

    }

    private static class DoubleDocValueField extends DocValueField {

        SortedNumericDoubleValues doubleValues;
        IndexNumericFieldData fieldData;

        DoubleDocValueField(String field, IndexNumericFieldData fieldData, DocValueFormat format) {
            super(field, format);
            this.fieldData = fieldData;
        }

        @Override
        void setNextReader(LeafReaderContext context) {
            doubleValues = fieldData.load(context).getDoubleValues();
        }

        @Override
        void setValues(int doc, DocumentField hitField) throws IOException {
            final List<Object> values = hitField.getValues();
            if (doubleValues.advanceExact(doc)) {
                for (int i = 0, count = doubleValues.docValueCount(); i < count; ++i) {
                    values.add(format.format(doubleValues.nextValue()));
                }
            }
        }
    }

    private static class NanoDocValueField extends DocValueField {

        SortedNumericDocValues longValues;
        IndexNumericFieldData fieldData;

        NanoDocValueField(String field, IndexNumericFieldData fieldData, DocValueFormat format) {
            super(field, withNanosecondResolution(format));
            this.fieldData = fieldData;
        }

        @Override
        void setNextReader(LeafReaderContext context) {
            longValues = ((SortedNumericIndexFieldData.NanoSecondFieldData) fieldData.load(context)).getLongValuesAsNanos();
        }

        @Override
        void setValues(int doc, DocumentField hitField) throws IOException {
            final List<Object> values = hitField.getValues();
            if (longValues.advanceExact(doc)) {
                for (int i = 0, count = longValues.docValueCount(); i < count; ++i) {
                    values.add(format.format(longValues.nextValue()));
                }
            }
        }
    }

    private static class LongDocValueField extends DocValueField {

        SortedNumericDocValues longValues;
        IndexNumericFieldData fieldData;

        LongDocValueField(String field, IndexNumericFieldData fieldData, DocValueFormat format) {
            super(field, format);
            this.fieldData = fieldData;
        }

        @Override
        void setNextReader(LeafReaderContext context) {
            longValues = fieldData.load(context).getLongValues();
        }

        @Override
        void setValues(int doc, DocumentField hitField) throws IOException {
            final List<Object> values = hitField.getValues();
            if (longValues.advanceExact(doc)) {
                for (int i = 0, count = longValues.docValueCount(); i < count; ++i) {
                    values.add(format.format(longValues.nextValue()));
                }
            }
        }

    }

    private static class BinaryDocValueField extends DocValueField {

        SortedBinaryDocValues byteValues;
        IndexFieldData<?> fieldData;

        BinaryDocValueField(String field, IndexFieldData<?> fieldData, DocValueFormat format) {
            super(field, format);
            this.fieldData = fieldData;
        }

        @Override
        void setNextReader(LeafReaderContext context) {
            byteValues = fieldData.load(context).getBytesValues();
        }

        @Override
        void setValues(int doc, DocumentField hitField) throws IOException {
            final List<Object> values = hitField.getValues();
            if (byteValues.advanceExact(doc)) {
                for (int i = 0, count = byteValues.docValueCount(); i < count; ++i) {
                    values.add(format.format(byteValues.nextValue()));
                }
            }
        }

    }

    private static DocValueField buildField(SearchContext context, FieldAndFormat fieldAndFormat) {
        MappedFieldType fieldType = context.mapperService().fieldType(fieldAndFormat.field);
        if (fieldType == null) {
            return null;
        }
        final IndexFieldData<?> indexFieldData = context.getForField(fieldType);
        DocValueFormat format = fieldType.docValueFormat(fieldAndFormat.format, null);
        if (indexFieldData instanceof IndexNumericFieldData) {
            if (((IndexNumericFieldData) indexFieldData).getNumericType().isFloatingPoint()) {
                return new DoubleDocValueField(fieldAndFormat.field, (IndexNumericFieldData) indexFieldData, format);
            }
            if (((IndexNumericFieldData) indexFieldData).getNumericType() == NumericType.DATE_NANOSECONDS) {
                return new NanoDocValueField(fieldAndFormat.field, (IndexNumericFieldData) indexFieldData, format);
            }
            return new LongDocValueField(fieldAndFormat.field, (IndexNumericFieldData) indexFieldData, format);
        }
        return new BinaryDocValueField(fieldAndFormat.field, indexFieldData, format);
    }
}
