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
package org.elasticsearch.legacy.search.fetch.fielddata;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.index.fielddata.AtomicFieldData;
import org.elasticsearch.legacy.index.fielddata.ScriptDocValues;
import org.elasticsearch.legacy.index.mapper.FieldMapper;
import org.elasticsearch.legacy.search.SearchHitField;
import org.elasticsearch.legacy.search.SearchParseElement;
import org.elasticsearch.legacy.search.fetch.FetchSubPhase;
import org.elasticsearch.legacy.search.internal.InternalSearchHit;
import org.elasticsearch.legacy.search.internal.InternalSearchHitField;
import org.elasticsearch.legacy.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Query sub phase which pulls data from field data (using the cache if
 * available, building it if not).
 *
 * Specifying {@code "fielddata_fields": ["field1", "field2"]}
 */
public class FieldDataFieldsFetchSubPhase implements FetchSubPhase {

    @Inject
    public FieldDataFieldsFetchSubPhase() {
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("fielddata_fields", new FieldDataFieldsParseElement())
                .put("fielddataFields", new FieldDataFieldsParseElement());
        return parseElements.build();
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticsearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.hasFieldDataFields();
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticsearchException {
        for (FieldDataFieldsContext.FieldDataField field : context.fieldDataFields().fields()) {
            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<String, SearchHitField>(2));
            }
            SearchHitField hitField = hitContext.hit().fields().get(field.name());
            if (hitField == null) {
                hitField = new InternalSearchHitField(field.name(), new ArrayList<>(2));
                hitContext.hit().fields().put(field.name(), hitField);
            }
            FieldMapper mapper = context.mapperService().smartNameFieldMapper(field.name());
            if (mapper != null) {
                AtomicFieldData data = context.fieldData().getForField(mapper).load(hitContext.readerContext());
                ScriptDocValues values = data.getScriptValues();
                values.setNextDocId(hitContext.docId());
                hitField.values().addAll(values.getValues());
            }
        }
    }
}
