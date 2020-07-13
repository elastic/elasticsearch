/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.equalTo;

public class StringScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    StringScriptFieldScript.Factory,
    StringScriptFieldScript.LeafFactory,
    String> {

    public void testConstant() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
        };
        assertThat(execute(indexBuilder, "value('cat')"), equalTo(List.of("cat", "cat")));
    }

    public void testTwoConstants() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
        };
        assertThat(execute(indexBuilder, "value('cat'); value('dog')"), equalTo(List.of("cat", "dog", "cat", "dog")));
    }

    public void testSource() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"cat\"}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"dog\"}"))));
        };
        assertThat(execute(indexBuilder, "value(source['foo'] + 'o')"), equalTo(List.of("cato", "dogo")));
    }

    public void testTwoSourceFields() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"cat\", \"bar\": \"chicken\"}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"dog\", \"bar\": \"pig\"}"))));
        };
        assertThat(
            execute(indexBuilder, "value(source['foo'] + 'o'); value(source['bar'] + 'ie')"),
            equalTo(List.of("cato", "chickenie", "dogo", "pigie"))
        );
    }

    public void testDocValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("cat"))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("dog"))));
        };
        assertThat(execute(indexBuilder, "value(doc['foo'].value + 'o')", new KeywordFieldType("foo")), equalTo(List.of("cato", "dogo")));
    }

    public void testTwoDocValuesValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("foo", new BytesRef("cat")),
                    new SortedSetDocValuesField("foo", new BytesRef("chicken"))
                )
            );
            iw.addDocument(
                List.of(new SortedSetDocValuesField("foo", new BytesRef("dog")), new SortedSetDocValuesField("foo", new BytesRef("pig")))
            );
        };
        assertThat(
            execute(indexBuilder, "for (String s: doc['foo']) {value(s + 'o')}", new KeywordFieldType("foo")),
            equalTo(List.of("cato", "chickeno", "dogo", "pigo"))
        );
    }

    @Override
    protected ScriptContext<StringScriptFieldScript.Factory> scriptContext() {
        return StringScriptFieldScript.CONTEXT;
    }

    @Override
    protected StringScriptFieldScript.LeafFactory newLeafFactory(
        StringScriptFieldScript.Factory factory,
        Map<String, Object> params,
        SearchLookup searchLookup
    ) {
        return factory.newFactory(params, searchLookup);
    }

    @Override
    protected IntFunction<List<String>> newInstance(StringScriptFieldScript.LeafFactory leafFactory, LeafReaderContext context)
        throws IOException {
        return leafFactory.newInstance(context)::resultsForDoc;
    }
}
