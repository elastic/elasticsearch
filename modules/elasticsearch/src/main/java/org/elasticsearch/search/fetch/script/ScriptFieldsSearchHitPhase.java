/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch.script;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.SearchHitPhase;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptFieldsSearchHitPhase implements SearchHitPhase {

    @Inject public ScriptFieldsSearchHitPhase() {
    }

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("script_fields", new ScriptFieldsParseElement())
                .put("scriptFields", new ScriptFieldsParseElement());
        return parseElements.build();
    }

    @Override public boolean executionNeeded(SearchContext context) {
        return context.hasScriptFields();
    }

    @Override public void execute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        for (ScriptFieldsContext.ScriptField scriptField : context.scriptFields().fields()) {
            scriptField.script().setNextReader(hitContext.reader());
            scriptField.script().setNextDocId(hitContext.docId());

            Object value;
            try {
                value = scriptField.script().run();
                value = scriptField.script().unwrap(value);
            } catch (RuntimeException e) {
                if (scriptField.ignoreException()) {
                    continue;
                }
                throw e;
            }

            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<String, SearchHitField>(2));
            }

            SearchHitField hitField = hitContext.hit().fields().get(scriptField.name());
            if (hitField == null) {
                hitField = new InternalSearchHitField(scriptField.name(), new ArrayList<Object>(2));
                hitContext.hit().fields().put(scriptField.name(), hitField);
            }
            hitField.values().add(value);
        }
    }
}
