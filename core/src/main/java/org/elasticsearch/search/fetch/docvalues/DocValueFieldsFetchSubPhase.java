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
package org.elasticsearch.search.fetch.docvalues;

import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Query sub phase which pulls data from doc values
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class DocValueFieldsFetchSubPhase implements FetchSubPhase {

    public static final String NAME = "docvalue_fields";
    public static final ContextFactory<DocValueFieldsContext> CONTEXT_FACTORY = new ContextFactory<DocValueFieldsContext>() {

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public DocValueFieldsContext newContextInstance() {
            return new DocValueFieldsContext();
        }
    };

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.getFetchSubPhaseContext(CONTEXT_FACTORY).hitExecutionNeeded() == false) {
            return;
        }
        for (DocValueFieldsContext.DocValueField field : context.getFetchSubPhaseContext(CONTEXT_FACTORY).fields()) {
            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<>(2));
            }
            SearchHitField hitField = hitContext.hit().fields().get(field.name());
            if (hitField == null) {
                hitField = new InternalSearchHitField(field.name(), new ArrayList<>(2));
                hitContext.hit().fields().put(field.name(), hitField);
            }
            MappedFieldType fieldType = context.mapperService().fullName(field.name());
            if (fieldType != null) {
                AtomicFieldData data = context.fieldData().getForField(fieldType).load(hitContext.readerContext());
                ScriptDocValues values = data.getScriptValues();
                values.setNextDocId(hitContext.docId());
                hitField.values().addAll(values.getValues());
            }
        }
    }
}
