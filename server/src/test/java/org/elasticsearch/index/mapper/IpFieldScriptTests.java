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
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class IpFieldScriptTests extends FieldScriptTestCase<IpFieldScript.Factory> {
    public static final IpFieldScript.Factory DUMMY = (fieldName, params, lookup) -> ctx -> new IpFieldScript(
        fieldName,
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            emit("192.168.0.1");
        }
    };

    @Override
    protected ScriptContext<IpFieldScript.Factory> context() {
        return IpFieldScript.CONTEXT;
    }

    @Override
    protected IpFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    @Override
    protected IpFieldScript.Factory fromSource() {
        return IpFieldScript.PARSE_FROM_SOURCE;
    }

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IpFieldScript script = new IpFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(field -> null, (ft, lookup) -> null),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES; i++) {
                            emit("192.168.0.1");
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
