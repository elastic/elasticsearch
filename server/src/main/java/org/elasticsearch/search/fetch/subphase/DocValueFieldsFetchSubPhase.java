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
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
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
import java.util.stream.Collectors;

/**
 * Query sub phase which pulls data from doc values
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class DocValueFieldsFetchSubPhase implements FetchSubPhase {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(DocValueFieldsFetchSubPhase.class));

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {

        if (context.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = context.collapse().getFieldName();
            if (context.docValueFieldsContext() == null) {
                context.docValueFieldsContext(new DocValueFieldsContext(
                        Collections.singletonList(new FieldAndFormat(name, DocValueFieldsContext.USE_DEFAULT_FORMAT))));
            } else if (context.docValueFieldsContext().fields().stream().map(ff -> ff.field).anyMatch(name::equals) == false) {
                context.docValueFieldsContext().fields().add(new FieldAndFormat(name, DocValueFieldsContext.USE_DEFAULT_FORMAT));
            }
        }

        if (context.docValueFieldsContext() == null) {
            return;
        }

        hits = hits.clone(); // don't modify the incoming hits
        Arrays.sort(hits, Comparator.comparingInt(SearchHit::docId));

        List<String> noFormatFields = context.docValueFieldsContext().fields().stream().filter(f -> f.format == null).map(f -> f.field)
                .collect(Collectors.toList());
        if (noFormatFields.isEmpty() == false) {
            deprecationLogger.deprecated("There are doc-value fields which are not using a format. The output will "
                    + "change in 7.0 when doc value fields get formatted based on mappings by default. It is recommended to pass "
                    + "[format={}] with a doc value field in order to opt in for the future behaviour and ease the migration to "
                    + "7.0: {}", DocValueFieldsContext.USE_DEFAULT_FORMAT, noFormatFields);
        }

        for (FieldAndFormat fieldAndFormat : context.docValueFieldsContext().fields()) {
            String field = fieldAndFormat.field;
            MappedFieldType fieldType = context.mapperService().fullName(field);
            if (fieldType != null) {
                final IndexFieldData<?> indexFieldData = context.getForField(fieldType);
                final DocValueFormat format;
                if (fieldAndFormat.format == null) {
                    format = null;
                } else {
                    String formatDesc = fieldAndFormat.format;
                    if (Objects.equals(formatDesc, DocValueFieldsContext.USE_DEFAULT_FORMAT)) {
                        formatDesc = null;
                    }
                    format = fieldType.docValueFormat(formatDesc, null);
                }
                LeafReaderContext subReaderContext = null;
                AtomicFieldData data = null;
                ScriptDocValues<?> scriptValues = null; // legacy
                SortedBinaryDocValues binaryValues = null; // binary / string / ip fields
                SortedNumericDocValues longValues = null; // int / date fields
                SortedNumericDoubleValues doubleValues = null; // floating-point fields
                for (SearchHit hit : hits) {
                    // if the reader index has changed we need to get a new doc values reader instance
                    if (subReaderContext == null || hit.docId() >= subReaderContext.docBase + subReaderContext.reader().maxDoc()) {
                        int readerIndex = ReaderUtil.subIndex(hit.docId(), context.searcher().getIndexReader().leaves());
                        subReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                        data = indexFieldData.load(subReaderContext);
                        if (format == null) {
                            scriptValues = data.getLegacyFieldValues();
                        } else if (indexFieldData instanceof IndexNumericFieldData) {
                            if (((IndexNumericFieldData) indexFieldData).getNumericType().isFloatingPoint()) {
                                doubleValues = ((AtomicNumericFieldData) data).getDoubleValues();
                            } else {
                                longValues = ((AtomicNumericFieldData) data).getLongValues();
                            }
                        } else {
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
                    if (scriptValues != null) {
                        scriptValues.setNextDocId(subDocId);
                        values.addAll(scriptValues);
                    } else if (binaryValues != null) {
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
