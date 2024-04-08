/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class CompositeFieldScriptTests extends ESTestCase {

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                CompositeFieldScript script = new CompositeFieldScript(
                    "composite",
                    Collections.emptyMap(),
                    new SearchLookup(field -> null, (ft, lookup, ftd) -> null, (ctx, doc) -> null),
                    OnScriptError.FAIL,
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES; i++) {
                            emit("leaf", "value" + i);
                        }
                    }
                };
                Exception e = expectThrows(IllegalArgumentException.class, script::execute);
                assertThat(
                    e.getMessage(),
                    equalTo("Runtime field [composite] is emitting [101] values while the maximum number of values allowed is [100]")
                );
            }
        }
    }

    public void testTooManyChars() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                StringBuilder big = new StringBuilder();
                while (big.length() < StringFieldScript.MAX_CHARS / 4) {
                    big.append("test");
                }
                String bigString = big.toString();
                CompositeFieldScript script = new CompositeFieldScript(
                    "composite",
                    Collections.emptyMap(),
                    new SearchLookup(field -> null, (ft, lookup, ftd) -> null, (ctx, doc) -> null),
                    OnScriptError.FAIL,
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= 4; i++) {
                            emit("leaf", bigString);
                        }
                    }
                };
                StringFieldScript stringFieldScript = new StringFieldScript(
                    "composite.leaf",
                    Collections.emptyMap(),
                    new SearchLookup(field -> null, (ft, lookup, ftd) -> null, (ctx, doc) -> null),
                    OnScriptError.FAIL,
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        emitFromCompositeScript(script);
                    }
                };

                Exception e = expectThrows(IllegalArgumentException.class, stringFieldScript::execute);
                assertThat(
                    e.getMessage(),
                    equalTo(
                        "Runtime field [composite.leaf] is emitting [1310720] characters "
                            + "while the maximum number of values allowed is [1048576]"
                    )
                );
            }
        }
    }
}
