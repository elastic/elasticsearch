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
package org.elasticsearch.search.fetch.fielddata;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Query sub phase which pulls data from field data (using the cache if
 * available, building it if not).
 *
 * Specifying {@code "fielddata_fields": ["field1", "field2"]}
 */
public class FieldDataFieldsFetchSubPhase implements FetchSubPhase {

    public static final String[] NAMES = {"fielddata_fields", "fielddataFields"};
    public static final ContextFactory<FieldDataFieldsContext> CONTEXT_FACTORY = new ContextFactory<FieldDataFieldsContext>() {

        @Override
        public String getName() {
            return NAMES[0];
        }

        @Override
        public FieldDataFieldsContext newContextInstance() {
            return new FieldDataFieldsContext();
        }
    };

    @Inject
    public FieldDataFieldsFetchSubPhase() {
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        Map<String, SearchParseElement> parseElements = new HashMap<>();
        parseElements.put("fielddata_fields", new FieldDataFieldsParseElement());
        parseElements.put("fielddataFields", new FieldDataFieldsParseElement());
        return unmodifiableMap(parseElements);
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.getFetchSubPhaseContext(CONTEXT_FACTORY).hitExecutionNeeded();
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        for (FieldDataFieldsContext.FieldDataField field : context.getFetchSubPhaseContext(CONTEXT_FACTORY).fields()) {
            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<String, SearchHitField>(2));
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
