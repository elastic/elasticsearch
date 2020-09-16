/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

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

public class BooleanFieldScriptTests extends FieldScriptTestCase<BooleanFieldScript.Factory> {
    public static final BooleanFieldScript.Factory DUMMY = (fieldName, params, lookup) -> ctx -> new BooleanFieldScript(
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
    protected ScriptContext<BooleanFieldScript.Factory> context() {
        return BooleanFieldScript.CONTEXT;
    }

    @Override
    protected BooleanFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                BooleanFieldScript script = new BooleanFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(mock(MapperService.class), (ft, lookup) -> null),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES * 1000; i++) {
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
