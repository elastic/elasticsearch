/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class BooleanScriptFieldScriptTests extends ScriptFieldScriptTestCase<BooleanScriptFieldScript.Factory> {
    public static final BooleanScriptFieldScript.Factory DUMMY = (fieldName, params, lookup) -> ctx -> new BooleanScriptFieldScript(
        fieldName,
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            emit(false);
        }
    };

    @Override
    protected ScriptContext<BooleanScriptFieldScript.Factory> context() {
        return BooleanScriptFieldScript.CONTEXT;
    }

    @Override
    protected BooleanScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                BooleanScriptFieldScript script = new BooleanScriptFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(mock(MapperService.class), (ft, lookup) -> null),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractScriptFieldScript.MAX_VALUES * 1000; i++) {
                            emit(i % 2 == 0);
                        }
                    }
                };
                // There isn't a limit to the number of values so this won't throw
                script.execute();
            }
        }
    }
}
