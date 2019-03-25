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

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.SortedNumericDVIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import static org.elasticsearch.search.DocValueFormat.withNanosecondResolution;

/**
 * Query sub phase which pulls data from doc values
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class DocValueFieldsFetchSubPhase implements FetchSubPhase {

    private static final String USE_DEFAULT_FORMAT = "use_field_mapping";
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(
            LogManager.getLogger(DocValueFieldsFetchSubPhase.class));

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {

        if (context.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = context.collapse().getFieldName();
            if (context.docValueFieldsContext() == null) {
                context.docValueFieldsContext(new DocValueFieldsContext(
                        Collections.singletonList(new FieldAndFormat(name, null))));
            } else if (context.docValueFieldsContext().fields().stream().map(ff -> ff.field).anyMatch(name::equals) == false) {
                context.docValueFieldsContext().fields().add(new FieldAndFormat(name, null));
            }
        }

        if (context.docValueFieldsContext() == null) {
            return;
        }

        hits = hits.clone(); // don't modify the incoming hits
        Arrays.sort(hits, Comparator.comparingInt(SearchHit::docId));

        if (context.docValueFieldsContext().fields().stream()
                .map(f -> f.format)
                .filter(USE_DEFAULT_FORMAT::equals)
                .findAny()
                .isPresent()) {
            DEPRECATION_LOGGER.deprecated("[" + USE_DEFAULT_FORMAT + "] is a special format that was only used to " +
                    "ease the transition to 7.x. It has become the default and shouldn't be set explicitly anymore.");
        }

        for (FieldAndFormat fieldAndFormat : context.docValueFieldsContext().fields()) {
            String field = fieldAndFormat.field;
            MappedFieldType fieldType = context.mapperService().fullName(field);
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
                if (Objects.equals(formatDesc, USE_DEFAULT_FORMAT)) {
                    // TODO: Remove in 8.x
                    formatDesc = null;
                }
                if (isNanosecond) {
                    format = withNanosecondResolution(fieldType.docValueFormat(formatDesc, null));
                } else {
                    format = fieldType.docValueFormat(formatDesc, null);
                }
                LeafReaderContext subReaderContext = null;
                AtomicFieldData data = null;
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
                                doubleValues = ((AtomicNumericFieldData) data).getDoubleValues();
                            } else {
                                // by default nanoseconds are cut to milliseconds within aggregations
                                // however for doc value fields we need the original nanosecond longs
                                if (isNanosecond) {
                                    longValues = ((SortedNumericDVIndexFieldData.NanoSecondFieldData) data).getLongValuesAsNanos();
                                } else {
                                    longValues = ((AtomicNumericFieldData) data).getLongValues();
                                }
                            }
                        } else {
                            data = indexFieldData.load(subReaderContext);
                            binaryValues = data.getBytesValues();
                        }
                    }
                    if (hit.fieldsOrNull() == null) {
                        hit.fields(new HashMap<>(2));
                    }
                    DocumentField hitField = hit.getFields().get(field);
                    if (hitField == null) {
                        hitField = new DocumentField(field, new ArrayList<>(2));
                        hit.getFields().put(field, hitField);
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
