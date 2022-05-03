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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
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
                    new SearchLookup(field -> null, (ft, lookup) -> null),
                    DateFormatter.forPattern(randomDateFormatterPattern()).withLocale(randomLocale(random())),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES; i++) {
                            emit(0);
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
