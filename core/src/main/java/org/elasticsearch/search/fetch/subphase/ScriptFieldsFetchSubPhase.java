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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public final class ScriptFieldsFetchSubPhase implements FetchSubPhase {

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {
        if (context.hasScriptFields() == false) {
            return;
        }

        hits = hits.clone(); // don't modify the incoming hits
        Arrays.sort(hits, Comparator.comparingInt(SearchHit::docId));

        int lastReaderId = -1;
        SearchScript[] leafScripts = null;
        List<ScriptFieldsContext.ScriptField> scriptFields = context.scriptFields().fields();
        final IndexReader reader = context.searcher().getIndexReader();
        for (SearchHit hit : hits) {
            int readerId = ReaderUtil.subIndex(hit.docId(), reader.leaves());
            LeafReaderContext leafReaderContext = reader.leaves().get(readerId);
            if (readerId != lastReaderId) {
                leafScripts = createLeafScripts(leafReaderContext, scriptFields);
                lastReaderId = readerId;
            }
            int docId = hit.docId() - leafReaderContext.docBase;
            for (int i = 0; i < leafScripts.length; i++) {
                leafScripts[i].setDocument(docId);
                final Object value;
                try {
                    value = leafScripts[i].run();
                } catch (RuntimeException e) {
                    if (scriptFields.get(i).ignoreException()) {
                        continue;
                    }
                    throw e;
                }
                if (hit.fieldsOrNull() == null) {
                    hit.fields(new HashMap<>(2));
                }
                String scriptFieldName = scriptFields.get(i).name();
                DocumentField hitField = hit.getFields().get(scriptFieldName);
                if (hitField == null) {
                    final List<Object> values;
                    if (value instanceof Collection) {
                        values = new ArrayList<>((Collection<?>) value);
                    } else {
                        values = Collections.singletonList(value);
                    }
                    hitField = new DocumentField(scriptFieldName, values);
                    hit.getFields().put(scriptFieldName, hitField);
                }
            }
        }
    }

    private SearchScript[] createLeafScripts(LeafReaderContext context,
                                             List<ScriptFieldsContext.ScriptField> scriptFields) {
        SearchScript[] scripts = new SearchScript[scriptFields.size()];
        for (int i = 0; i < scripts.length; i++) {
            try {
                scripts[i] = scriptFields.get(i).script().newInstance(context);
            } catch (IOException e1) {
                throw new IllegalStateException("Failed to load script " + scriptFields.get(i).name(), e1);
            }
        }
        return scripts;
    }
}
