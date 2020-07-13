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
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.equalTo;

public class LongScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    LongScriptFieldScript.Factory,
    LongScriptFieldScript.LeafFactory,
    Long> {

    public void testConstant() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        };
        assertThat(execute(indexBuilder, "value(10)"), equalTo(List.of(10L, 10L)));
    }

    public void testTwoConstants() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        };
        assertThat(execute(indexBuilder, "value(10); value(20)"), equalTo(List.of(10L, 20L, 10L, 20L)));
    }

    public void testSource() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10}"))));
        };
        assertThat(execute(indexBuilder, "value(source['foo'] * 10)"), equalTo(List.of(10L, 100L)));
    }

    public void testTwoSourceFields() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1, \"bar\": 2}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10, \"bar\": 20}"))));
        };
        assertThat(execute(indexBuilder, "value(source['foo'] * 10); value(source['bar'] * 10)"), equalTo(List.of(10L, 20L, 100L, 200L)));
    }

    public void testDocValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10)));
        };
        assertThat(
            execute(indexBuilder, "value(doc['foo'].value * 10)", new NumberFieldType("foo", NumberType.LONG)),
            equalTo(List.of(10L, 100L))
        );
    }

    public void testTwoDocValuesValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1), new SortedNumericDocValuesField("foo", 2)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10), new SortedNumericDocValuesField("foo", 20)));
        };
        assertThat(
            execute(indexBuilder, "for (long l : doc['foo']) {value(l * 10)}", new NumberFieldType("foo", NumberType.LONG)),
            equalTo(List.of(10L, 20L, 100L, 200L))
        );
    }

    @Override
    protected ScriptContext<LongScriptFieldScript.Factory> scriptContext() {
        return LongScriptFieldScript.CONTEXT;
    }

    @Override
    protected LongScriptFieldScript.LeafFactory newLeafFactory(
        LongScriptFieldScript.Factory factory,
        Map<String, Object> params,
        SearchLookup searchLookup
    ) {
        return factory.newFactory(params, searchLookup);
    }

    @Override
    protected IntFunction<List<Long>> newInstance(LongScriptFieldScript.LeafFactory leafFactory, LeafReaderContext context)
        throws IOException {
        List<Long> results = new ArrayList<>();
        LongScriptFieldScript script = leafFactory.newInstance(context, results::add);
        return docId -> {
            results.clear();
            script.setDocument(docId);
            script.execute();
            return results;
        };
    }
}
