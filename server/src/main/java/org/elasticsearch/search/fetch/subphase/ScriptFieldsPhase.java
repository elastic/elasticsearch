/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class ScriptFieldsPhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.scriptFields() == null || context.scriptFields().fields().isEmpty()) {
            return null;
        }
        List<ScriptFieldsContext.ScriptField> scriptFields = context.scriptFields().fields();
        return new FetchSubPhaseProcessor() {

            FieldScript[] leafScripts = null;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                leafScripts = createLeafScripts(readerContext, scriptFields);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                // If script fields need source then they will load it via SearchLookup,
                // which has its own lazy loading config that kicks in if not overridden
                // by other sub phases that require source. However, if script fields
                // are present then we enforce metadata loading
                return new StoredFieldsSpec(false, true, Set.of());
            }

            @Override
            public void process(HitContext hitContext) {
                int docId = hitContext.docId();
                for (int i = 0; i < leafScripts.length; i++) {
                    leafScripts[i].setDocument(docId);
                    final Object value;
                    try {
                        value = leafScripts[i].execute();
                        CollectionUtils.ensureNoSelfReferences(value, "ScriptFieldsPhase leaf script " + i);
                    } catch (RuntimeException e) {
                        if (scriptFields.get(i).ignoreException()) {
                            continue;
                        }
                        throw e;
                    }
                    String scriptFieldName = scriptFields.get(i).name();
                    DocumentField hitField = hitContext.hit().field(scriptFieldName);
                    if (hitField == null) {
                        final List<Object> values;
                        if (value instanceof Collection) {
                            values = new ArrayList<>((Collection<?>) value);
                        } else {
                            values = Collections.singletonList(value);
                        }
                        hitField = new DocumentField(scriptFieldName, values);
                        // script fields are never meta-fields
                        hitContext.hit().setDocumentField(scriptFieldName, hitField);
                    }
                }
            }
        };
    }

    private static FieldScript[] createLeafScripts(LeafReaderContext context, List<ScriptFieldsContext.ScriptField> scriptFields) {
        FieldScript[] scripts = new FieldScript[scriptFields.size()];
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
