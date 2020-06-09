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
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
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
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {

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
            return;
        }

        for (FieldAndFormat fieldAndFormat : context.docValuesContext().fields()) {
            String field = fieldAndFormat.field;
            MappedFieldType fieldType = context.mapperService().fieldType(field);
            if (fieldType != null) {
                final IndexFieldData<?> indexFieldData = context.getForField(fieldType);
                final boolean isNanosecond;
                if (indexFieldData instanceof IndexNumericFieldData) {
                    isNanosecond = ((IndexNumericFieldData) indexFieldData).getNumericType() == NumericType.DATE_NANOSECONDS;
                } else {
                    isNanosecond = false;
                }
                final DocValueFormat format;
                String formatDesc = fieldAndFormat.format;
                if (isNanosecond) {
                    format = withNanosecondResolution(fieldType.docValueFormat(formatDesc, null));
                } else {
                    format = fieldType.docValueFormat(formatDesc, null);
                }
                LeafReaderContext subReaderContext = null;
                LeafFieldData data = null;
                SortedBinaryDocValues binaryValues = null; // binary / string / ip fields
                SortedNumericDocValues longValues = null; // int / date fields
                SortedNumericDoubleValues doubleValues = null; // floating-point fields
                for (SearchHit hit : hits) {
                    // if the reader index has changed we need to get a new doc values reader instance
                    if (subReaderContext == null || hit.docId() >= subReaderContext.docBase + subReaderContext.reader().maxDoc()) {
                        int readerIndex = ReaderUtil.subIndex(hit.docId(), context.searcher().getIndexReader().leaves());
                        subReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                        data = indexFieldData.load(subReaderContext);
                        if (indexFieldData instanceof IndexNumericFieldData) {
                            NumericType numericType = ((IndexNumericFieldData) indexFieldData).getNumericType();
                            if (numericType.isFloatingPoint()) {
                                doubleValues = ((LeafNumericFieldData) data).getDoubleValues();
                            } else {
                                // by default nanoseconds are cut to milliseconds within aggregations
                                // however for doc value fields we need the original nanosecond longs
                                if (isNanosecond) {
                                    longValues = ((SortedNumericIndexFieldData.NanoSecondFieldData) data).getLongValuesAsNanos();
                                } else {
                                    longValues = ((LeafNumericFieldData) data).getLongValues();
                                }
                            }
                        } else {
                            data = indexFieldData.load(subReaderContext);
                            binaryValues = data.getBytesValues();
                        }
                    }
                    DocumentField hitField = hit.field(field);
                    if (hitField == null) {
                        hitField = new DocumentField(field, new ArrayList<>(2));
                        // even if we request a doc values of a meta-field (e.g. _routing),
                        // docValues fields will still be document fields, and put under "fields" section of a hit.
                        hit.setDocumentField(field, hitField);
                    }
                    final List<Object> values = hitField.getValues();

                    int subDocId = hit.docId() - subReaderContext.docBase;
                    if (binaryValues != null) {
                        if (binaryValues.advanceExact(subDocId)) {
                            for (int i = 0, count = binaryValues.docValueCount(); i < count; ++i) {
                                values.add(format.format(binaryValues.nextValue()));
                            }
                        }
                    } else if (longValues != null) {
                        if (longValues.advanceExact(subDocId)) {
                            for (int i = 0, count = longValues.docValueCount(); i < count; ++i) {
                                values.add(format.format(longValues.nextValue()));
                            }
                        }
                    } else if (doubleValues != null) {
                        if (doubleValues.advanceExact(subDocId)) {
                            for (int i = 0, count = doubleValues.docValueCount(); i < count; ++i) {
                                values.add(format.format(doubleValues.nextValue()));
                            }
                        }
                    } else {
                        throw new AssertionError("Unreachable code");
                    }
                }
            }
        }
    }
}
