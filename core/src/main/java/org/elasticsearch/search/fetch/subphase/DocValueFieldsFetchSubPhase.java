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

import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Query sub phase which pulls data from doc values
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class DocValueFieldsFetchSubPhase implements FetchSubPhase {

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = context.collapse().getFieldType().name();
            if (context.docValueFieldsContext() == null) {
                context.docValueFieldsContext(new DocValueFieldsContext(Collections.singletonList(name)));
            } else if (context.docValueFieldsContext().fields().contains(name) == false) {
                context.docValueFieldsContext().fields().add(name);
            }
        }
        if (context.docValueFieldsContext() == null) {
            return;
        }
        for (String field : context.docValueFieldsContext().fields()) {
            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<>(2));
            }
            SearchHitField hitField = hitContext.hit().getFields().get(field);
            if (hitField == null) {
                hitField = new SearchHitField(field, new ArrayList<>(2));
                hitContext.hit().getFields().put(field, hitField);
            }
            MappedFieldType fieldType = context.mapperService().fullName(field);
            if (fieldType != null) {
                /* Because this is called once per document we end up creating a new ScriptDocValues for every document which is important
                 * because the values inside ScriptDocValues might be reused for different documents (Dates do this). */
                AtomicFieldData data = context.fieldData().getForField(fieldType).load(hitContext.readerContext());
                ScriptDocValues<?> values = data.getScriptValues();
                values.setNextDocId(hitContext.docId());
                hitField.getValues().addAll(values);
            }
        }
    }
}
