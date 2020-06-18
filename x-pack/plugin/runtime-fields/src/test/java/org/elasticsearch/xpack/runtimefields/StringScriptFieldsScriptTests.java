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
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class StringScriptFieldsScriptTests extends ScriptFieldScriptTestCase<
    StringScriptFieldsScript,
    StringScriptFieldsScript.Factory,
    StringScriptFieldsScript.LeafFactory,
    String> {

    public void testConstant() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
        };
        assertThat(execute(indexBuilder, "sync.accept(\"cat\")"), equalTo(List.of("cat", "cat")));
    }

    public void testTwoConstants() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
        };
        assertThat(execute(indexBuilder, "sync.accept(\"cat\"); sync.accept(\"dog\")"), equalTo(List.of("cat", "dog", "cat", "dog")));
    }

    public void testSource() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"cat\"}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"dog\"}"))));
        };
        assertThat(execute(indexBuilder, "sync.accept(source['foo'])"), equalTo(List.of("cat", "dog")));
    }

    public void testTwoSourceFields() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"cat\", \"bar\": \"chicken\"}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"dog\", \"bar\": \"pig\"}"))));
        };
        assertThat(
            execute(indexBuilder, "sync.accept(source['foo']); sync.accept(source['bar'])"),
            equalTo(List.of("cat", "chicken", "dog", "pig"))
        );
    }

    public void testDocValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("cat"))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("dog"))));
        };
        assertThat(execute(indexBuilder, "sync.accept(doc['foo'].value)", new KeywordFieldType("foo")), equalTo(List.of("cat", "dog")));
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
            execute(indexBuilder, "def foo = doc['foo']; sync.accept(foo.get(0)); sync.accept(foo.get(1))", new KeywordFieldType("foo")),
            equalTo(List.of("cat", "chicken", "dog", "pig"))
        );
    }

    @Override
    protected ScriptContext<StringScriptFieldsScript.Factory> scriptContext() {
        return StringScriptFieldsScript.CONTEXT;
    }

    @Override
    protected StringScriptFieldsScript.LeafFactory newLeafFactory(
        StringScriptFieldsScript.Factory factory,
        Map<String, Object> params,
        SourceLookup source,
        DocLookup fieldData
    ) {
        return factory.newFactory(params, source, fieldData);
    }

    @Override
    protected StringScriptFieldsScript newInstance(
        StringScriptFieldsScript.LeafFactory leafFactory,
        LeafReaderContext context,
        List<String> result
    ) throws IOException {
        return leafFactory.newInstance(context, result::add);
    }
}
