/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

    @Override
    protected BooleanFieldScript.Factory fromSource() {
        return BooleanFieldScript.PARSE_FROM_SOURCE;
    }

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                BooleanFieldScript script = new BooleanFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(field -> null, (ft, lookup) -> null),
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
