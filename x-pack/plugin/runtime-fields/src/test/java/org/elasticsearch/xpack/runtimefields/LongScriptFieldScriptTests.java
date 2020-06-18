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

public class LongScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    LongScriptFieldScript,
    LongScriptFieldScript.Factory,
    LongScriptFieldScript.LeafFactory,
    Long> {

    public void testConstant() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        };
        assertThat(execute(indexBuilder, "sync.accept(10)"), equalTo(List.of(10L, 10L)));
    }

    public void testTwoConstants() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        };
        assertThat(execute(indexBuilder, "sync.accept(10); sync.accept(20)"), equalTo(List.of(10L, 20L, 10L, 20L)));
    }

    public void testSource() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 100}"))));
        };
        assertThat(execute(indexBuilder, "sync.accept(source['foo'])"), equalTo(List.of(10L, 100L)));
    }

    public void testTwoSourceFields() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10, \"bar\": 20}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 100, \"bar\": 200}"))));
        };
        assertThat(execute(indexBuilder, "sync.accept(source['foo']); sync.accept(source['bar'])"), equalTo(List.of(10L, 20L, 100L, 200L)));
    }

    public void testDocValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 100)));
        };
        assertThat(
            execute(indexBuilder, "sync.accept(doc['foo'].value)", new NumberFieldType("foo", NumberType.LONG)),
            equalTo(List.of(10L, 100L))
        );
    }

    public void testTwoDocValuesValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10), new SortedNumericDocValuesField("foo", 20)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 100), new SortedNumericDocValuesField("foo", 200)));
        };
        assertThat(
            execute(indexBuilder, "def foo = doc['foo']; sync.accept(foo[0]); sync.accept(foo[1])", new NumberFieldType("foo", NumberType.LONG)),
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
        SourceLookup source,
        DocLookup fieldData
    ) {
        return factory.newFactory(params, source, fieldData);
    }

    @Override
    protected LongScriptFieldScript newInstance(LongScriptFieldScript.LeafFactory leafFactory, LeafReaderContext context, List<Long> result)
        throws IOException {

        return leafFactory.newInstance(context, result::add);
    }
}
