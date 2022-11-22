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
import org.elasticsearch.common.Strings;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
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
                    new SearchLookup(field -> null, (ft, lookup, fdt) -> null, new SourceLookup.ReaderSourceProvider()),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES; i++) {
                            new Emit(this).emit("test");
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
                    new SearchLookup(field -> null, (ft, lookup, fdt) -> null, new SourceLookup.ReaderSourceProvider()),
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
                            new Emit(this).emit(bigString);
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

    public final void testFromSourceDoesNotEnforceValuesLimit() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            int numValues = AbstractFieldScript.MAX_VALUES + randomIntBetween(1, 100);
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.startArray("field");
            for (int i = 0; i < numValues; i++) {
                builder.value("value" + i);
            }
            builder.endArray();
            builder.endObject();
            iw.addDocument(List.of(new StoredField("_source", new BytesRef(Strings.toString(builder)))));
            try (DirectoryReader reader = iw.getReader()) {
                StringFieldScript.LeafFactory leafFactory = fromSource().newFactory(
                    "field",
                    Collections.emptyMap(),
                    new SearchLookup(field -> null, (ft, lookup, fdt) -> null, new SourceLookup.ReaderSourceProvider())
                );
                StringFieldScript stringFieldScript = leafFactory.newInstance(reader.leaves().get(0));
                List<String> results = stringFieldScript.resultsForDoc(0);
                assertEquals(numValues, results.size());
            }
        }
    }

    public final void testFromSourceDoesNotEnforceCharsLimit() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            StringBuilder big = new StringBuilder();
            while (big.length() < StringFieldScript.MAX_CHARS / 4) {
                big.append("test");
            }
            String bigString = big.toString();
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.startArray("field");
            for (int i = 0; i <= 4; i++) {
                builder.value(bigString);
            }
            builder.endArray();
            builder.endObject();
            iw.addDocument(List.of(new StoredField("_source", new BytesRef(Strings.toString(builder)))));
            try (DirectoryReader reader = iw.getReader()) {
                StringFieldScript.LeafFactory leafFactory = fromSource().newFactory(
                    "field",
                    Collections.emptyMap(),
                    new SearchLookup(field -> null, (ft, lookup, fdt) -> null, new SourceLookup.ReaderSourceProvider())
                );
                StringFieldScript stringFieldScript = leafFactory.newInstance(reader.leaves().get(0));
                List<String> results = stringFieldScript.resultsForDoc(0);
                assertEquals(5, results.size());
            }
        }
    }
}
