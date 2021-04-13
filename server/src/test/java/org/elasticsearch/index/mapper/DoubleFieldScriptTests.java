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
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DoubleFieldScriptTests extends FieldScriptTestCase<DoubleFieldScript.Factory> {
    public static final DoubleFieldScript.Factory DUMMY = (fieldName, params, lookup) -> ctx -> new DoubleFieldScript(
        fieldName,
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            emit(1.0);
        }
    };

    @Override
    protected ScriptContext<DoubleFieldScript.Factory> context() {
        return DoubleFieldScript.CONTEXT;
    }

    @Override
    protected DoubleFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    public void testAsDocValues() {
        DoubleFieldScript script = new DoubleFieldScript(
                "test",
                Map.of(),
                new SearchLookup(field -> null, (ft, lookup) -> null),
                null
        ) {
            @Override
            public void execute() {
                emit(3.1);
                emit(2.29);
                emit(-12.47);
                emit(-12.46);
                emit(Double.MAX_VALUE);
                emit(0.0);
            }
        };
        script.execute();

        assertArrayEquals(new double[] {-12.47, -12.46, 0.0, 2.29, 3.1, Double.MAX_VALUE}, script.asDocValues(), 0.000000001);
    }

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                DoubleFieldScript script = new DoubleFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(field -> null, (ft, lookup) -> null),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES; i++) {
                            emit(1.0);
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
