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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DateFieldScriptTests extends FieldScriptTestCase<DateFieldScript.Factory> {
    public static final DateFieldScript.Factory DUMMY = (fieldName, params, lookup, formatter) -> ctx -> new DateFieldScript(
        fieldName,
        params,
        lookup,
        formatter,
        ctx
    ) {
        @Override
        public void execute() {
            emit(1595431354874L);
        }
    };

    @Override
    protected ScriptContext<DateFieldScript.Factory> context() {
        return DateFieldScript.CONTEXT;
    }

    @Override
    protected DateFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    @Override
    protected DateFieldScript.Factory fromSource() {
        return DateFieldScript.PARSE_FROM_SOURCE;
    }

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                DateFieldScript script = new DateFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(field -> null, (ft, lookup, fdt) -> null, new SourceLookup.ReaderSourceProvider()),
                    DateFormatter.forPattern(randomDateFormatterPattern()).withLocale(randomLocale(random())),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES; i++) {
                            new Emit(this).emit(0);
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

    public final void testFromSourceDoesNotEnforceValuesLimit() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            int numValues = AbstractFieldScript.MAX_VALUES + randomIntBetween(1, 100);
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.startArray("field");
            for (int i = 0; i < numValues; i++) {
                builder.value(i);
            }
            builder.endArray();
            builder.endObject();
            iw.addDocument(List.of(new StoredField("_source", new BytesRef(Strings.toString(builder)))));
            try (DirectoryReader reader = iw.getReader()) {
                DateFieldScript.LeafFactory leafFactory = fromSource().newFactory(
                    "field",
                    Collections.emptyMap(),
                    new SearchLookup(field -> null, (ft, lookup, fdt) -> null, new SourceLookup.ReaderSourceProvider()),
                    DateFormatter.forPattern("epoch_millis")
                );
                DateFieldScript dateFieldScript = leafFactory.newInstance(reader.leaves().get(0));
                List<Long> results = new ArrayList<>();
                dateFieldScript.runForDoc(0, results::add);
                assertEquals(numValues, results.size());
            }
        }
    }
}
