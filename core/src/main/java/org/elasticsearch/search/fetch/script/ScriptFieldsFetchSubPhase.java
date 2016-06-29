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
package org.elasticsearch.search.fetch.script;

import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public final class ScriptFieldsFetchSubPhase implements FetchSubPhase {

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.hasScriptFields() == false) {
            return;
        }
        for (ScriptFieldsContext.ScriptField scriptField : context.scriptFields().fields()) {
            LeafSearchScript leafScript;
            try {
                leafScript = scriptField.script().getLeafSearchScript(hitContext.readerContext());
            } catch (IOException e1) {
                throw new IllegalStateException("Failed to load script", e1);
            }
            leafScript.setDocument(hitContext.docId());

            final Object value;
            try {
                value = leafScript.unwrap(leafScript.run());
            } catch (RuntimeException e) {
                if (scriptField.ignoreException()) {
                    continue;
                }
                throw e;
            }

            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<>(2));
            }

            SearchHitField hitField = hitContext.hit().fields().get(scriptField.name());
            if (hitField == null) {
                final List<Object> values;
                if (value instanceof Collection) {
                    // TODO: use diamond operator once JI-9019884 is fixed
                    values = new ArrayList<>((Collection<?>) value);
                } else {
                    values = Collections.singletonList(value);
                }
                hitField = new InternalSearchHitField(scriptField.name(), values);
                hitContext.hit().fields().put(scriptField.name(), hitField);
            }
        }
    }
}
