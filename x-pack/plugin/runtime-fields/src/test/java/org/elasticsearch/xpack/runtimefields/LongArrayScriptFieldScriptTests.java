/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LongArrayScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    LongArrayScriptFieldScript,
    LongArrayScriptFieldScript.Factory,
    LongArrayScriptFieldScript.LeafFactory,
    Long> {

    public void testConstant() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        };
        assertThat(execute(indexBuilder, "new long[] {10, 100}"), equalTo(List.of(10L, 100L, 10L, 100L)));
    }

    public void testSource() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10, \"bar\": 20}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 100, \"bar\": 200}"))));
        };
        assertThat(execute(indexBuilder, "new long[] {source['foo'], source['bar']}"), equalTo(List.of(10L, 20L, 100L, 200L)));
    }

    public void testDocValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10), new SortedNumericDocValuesField("foo", 20)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 100), new SortedNumericDocValuesField("foo", 200)));
        };
        assertThat(
            execute(indexBuilder, "def foo = doc['foo']; new long[] {foo[0], foo[1]}", new NumberFieldType("foo", NumberType.LONG)),
            equalTo(List.of(10L, 20L, 100L, 200L))
        );
    }

    @Override
    protected ScriptContext<LongArrayScriptFieldScript.Factory> scriptContext() {
        return LongArrayScriptFieldScript.CONTEXT;
    }

    @Override
    protected LongArrayScriptFieldScript.LeafFactory newLeafFactory(
        LongArrayScriptFieldScript.Factory factory,
        Map<String, Object> params,
        SourceLookup source,
        DocLookup fieldData
    ) {
        return factory.newFactory(params, source, fieldData);
    }

    @Override
    protected LongArrayScriptFieldScript newInstance(LongArrayScriptFieldScript.LeafFactory leafFactory, LeafReaderContext context)
        throws IOException {
        return leafFactory.newInstance(context);
    }

    @Override
    protected void collect(LongArrayScriptFieldScript script, List<Long> result) {
        for (long l : script.execute()) {
            result.add(l);
        }
    }
}
