/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class IpFieldScriptTests extends FieldScriptTestCase<IpFieldScript.Factory> {
    public static final IpFieldScript.Factory DUMMY = (fieldName, params, lookup, onScriptError) -> ctx -> new IpFieldScript(
        fieldName,
        params,
        lookup,
        OnScriptError.FAIL,
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
                    new SearchLookup(field -> null, (ft, lookup, fdt) -> null, (ctx, doc) -> null),
                    OnScriptError.FAIL,
                    reader.leaves().get(0)
                ) {
                    @Override
                    public void execute() {
                        for (int i = 0; i <= AbstractFieldScript.MAX_VALUES; i++) {
                            new Emit(this).emit("192.168.0.1");
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
                builder.value("192.168.0." + i);
            }
            builder.endArray();
            builder.endObject();
            iw.addDocument(List.of(new StoredField("_source", new BytesRef(Strings.toString(builder)))));
            try (DirectoryReader reader = iw.getReader()) {
                IpFieldScript.LeafFactory leafFactory = fromSource().newFactory(
                    "field",
                    Collections.emptyMap(),
                    new SearchLookup(
                        field -> null,
                        (ft, lookup, fdt) -> null,
                        SourceProvider.fromLookup(MappingLookup.EMPTY, null, SourceFieldMetrics.NOOP)
                    ),
                    OnScriptError.FAIL
                );
                IpFieldScript ipFieldScript = leafFactory.newInstance(reader.leaves().get(0));
                List<InetAddress> results = new ArrayList<>();
                ipFieldScript.runForDoc(0, results::add);
                assertEquals(numValues, results.size());
            }
        }
    }
}
