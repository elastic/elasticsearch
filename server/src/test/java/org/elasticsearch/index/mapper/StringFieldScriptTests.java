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
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class StringFieldScriptTests extends FieldScriptTestCase<StringFieldScript.Factory> {
    public static final StringFieldScript.Factory DUMMY = (fieldName, params, lookup) -> ctx -> new StringFieldScript(
        fieldName,
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            emit("foo");
        }
    };

    @Override
    protected ScriptContext<StringFieldScript.Factory> context() {
        return StringFieldScript.CONTEXT;
    }

    @Override
    protected StringFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    @Override
    protected StringFieldScript.Factory fromSource() {
        return StringFieldScript.PARSE_FROM_SOURCE;
    }

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                StringFieldScript script = new StringFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(field -> null, (ft, lookup) -> null),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES; i++) {
                            emit("test");
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

    public void testTooManyChars() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                StringFieldScript script = new StringFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(field -> null, (ft, lookup) -> null),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        StringBuilder big = new StringBuilder();
                        while (big.length() < StringFieldScript.MAX_CHARS / 4) {
                            big.append("test");
                        }
                        String bigString = big.toString();
                        for (int i = 0; i <= 4; i++) {
                            emit(bigString);
                        }
                    }
                };
                Exception e = expectThrows(IllegalArgumentException.class, script::execute);
                assertThat(
                    e.getMessage(),
                    equalTo("Runtime field [test] is emitting [1310720] characters while the maximum number of values allowed is [1048576]")
                );
            }
        }
    }
}
