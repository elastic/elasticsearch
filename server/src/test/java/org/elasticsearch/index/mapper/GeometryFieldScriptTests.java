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
import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class GeometryFieldScriptTests extends FieldScriptTestCase<GeometryFieldScript.Factory> {
    public static final GeometryFieldScript.Factory DUMMY = (fieldName, params, lookup, onScriptError) -> ctx -> new GeometryFieldScript(
        fieldName,
        params,
        lookup,
        OnScriptError.FAIL,
        ctx
    ) {
        @Override
        public void execute() {
            emitFromObject("POINT(0 0)");
        }
    };

    @Override
    protected ScriptContext<GeometryFieldScript.Factory> context() {
        return GeometryFieldScript.CONTEXT;
    }

    @Override
    protected GeometryFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    @Override
    protected GeometryFieldScript.Factory fromSource() {
        return GeometryFieldScript.PARSE_FROM_SOURCE;
    }

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                GeometryFieldScript script = new GeometryFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(field -> null, (ft, lookup, fdt) -> null, (ctx, doc) -> null),
                    OnScriptError.FAIL,
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES; i++) {
                            new Emit(this).emit("POINT(0 0)");
                        }
                    }
                };
                Exception e = expectThrows(IllegalArgumentException.class, script::execute);
                assertThat(
                    e.getMessage(),
                    equalTo("Runtime field [test] is emitting [101] values while the maximum number of values allowed is [100]")
                );
            }
        }
    }
}
