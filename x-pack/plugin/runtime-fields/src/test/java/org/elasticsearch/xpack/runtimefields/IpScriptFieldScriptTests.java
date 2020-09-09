/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

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

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class IpScriptFieldScriptTests extends ScriptFieldScriptTestCase<IpScriptFieldScript.Factory> {
    public static final IpScriptFieldScript.Factory DUMMY = (fieldName, params, lookup) -> ctx -> new IpScriptFieldScript(
        fieldName,
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            emitValue("192.168.0.1");
        }
    };

    @Override
    protected ScriptContext<IpScriptFieldScript.Factory> context() {
        return IpScriptFieldScript.CONTEXT;
    }

    @Override
    protected IpScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }

    public void testTooManyValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IpScriptFieldScript script = new IpScriptFieldScript(
                    "test",
                    Map.of(),
                    new SearchLookup(mock(MapperService.class), (ft, lookup) -> null),
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractScriptFieldScript.MAX_VALUES; i++) {
                            emitValue("192.168.0.1");
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
