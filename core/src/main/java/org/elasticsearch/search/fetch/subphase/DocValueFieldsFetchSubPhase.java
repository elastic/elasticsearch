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

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Query sub phase which pulls data from doc values
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class DocValueFieldsFetchSubPhase implements FetchSubPhase {

    // TODO: Remove in 7.0
    private static final String USE_DEFAULT_FORMAT = "use_field_mapping";
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(DocValueFieldsFetchSubPhase.class));

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws IOException {
        List<DocValueFieldsContext.Field> docValueFields = Collections.emptyList();
        if (context.docValueFieldsContext() != null) {
            docValueFields = context.docValueFieldsContext().fields();
        }
        if (context.collapse() != null) {
            String name = context.collapse().getFieldType().name();
            docValueFields = new ArrayList<>(docValueFields);
            docValueFields.add(new DocValueFieldsContext.Field(name, null));
        }
        for (DocValueFieldsContext.Field field : docValueFields) {
            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<>(2));
            }
            SearchHitField hitField = hitContext.hit().getFields().get(field.getName());
            if (hitField == null) {
                hitField = new SearchHitField(field.getName(), new ArrayList<>(2));
                hitContext.hit().getFields().put(field.getName(), hitField);
            }
            MappedFieldType fieldType = context.mapperService().fullName(field.getName());
            if (fieldType != null) {
                List<Object> values = Collections.emptyList();
                String formatName = field.getFormat();
                if (USE_DEFAULT_FORMAT.equals(formatName)) {
                    // 5.0..5.4 did not format doc values fields, so we exposed the ability to
                    // use the format associated with the field with a special format name
                    // `use_field_mapping`, which we are keeping in 6.x to ease the transition from
                    // 5.x to 6.x
                    DEPRECATION_LOGGER.deprecated("Format [{}] is deprecated, just omit the format or set it to null in order to use "
                            + "the field defaults", USE_DEFAULT_FORMAT);
                    formatName = null;
                }
                final DocValueFormat format = fieldType.docValueFormat(formatName, null);
                final IndexFieldData<?> fieldData = context.fieldData().getForField(fieldType);
                if (fieldData instanceof IndexNumericFieldData) {
                    IndexNumericFieldData numericFieldData = (IndexNumericFieldData) fieldData;
                    if (numericFieldData.getNumericType().isFloatingPoint()) {
                        SortedNumericDoubleValues dv = numericFieldData.load(hitContext.readerContext()).getDoubleValues();
                        if (dv.advanceExact(hitContext.docId())) {
                            final int count = dv.docValueCount();
                            values = new ArrayList<>(count);
                            for (int i = 0; i < count; ++i) {
                                values.add(format.format(dv.nextValue()));
                            }
                        }
                    } else {
                        SortedNumericDocValues dv = numericFieldData.load(hitContext.readerContext()).getLongValues();
                        if (dv.advanceExact(hitContext.docId())) {
                            final int count = dv.docValueCount();
                            values = new ArrayList<>(count);
                            for (int i = 0; i < count; ++i) {
                                values.add(format.format(dv.nextValue()));
                            }
                        }
                    }
                } else {
                    SortedBinaryDocValues dv = fieldData.load(hitContext.readerContext()).getBytesValues();
                    if (dv.advanceExact(hitContext.docId())) {
                        final int count = dv.docValueCount();
                        values = new ArrayList<>(count);
                        for (int i = 0; i < count; ++i) {
                            values.add(format.format(dv.nextValue()));
                        }
                    }
                }

                hitField.getValues().addAll(values);
            }
        }
    }
}
