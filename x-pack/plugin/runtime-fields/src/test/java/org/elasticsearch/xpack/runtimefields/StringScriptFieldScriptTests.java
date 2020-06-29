/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class StringScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    StringScriptFieldScript,
    StringScriptFieldScript.Factory,
    StringScriptFieldScript.LeafFactory,
    String> {

    public void testConstant() throws IOException {
        assertThat(randomStrings().collect("value('cat')"), equalTo(List.of("cat", "cat")));
    }

    public void testTwoConstants() throws IOException {
        assertThat(randomStrings().collect("value('cat'); value('dog')"), equalTo(List.of("cat", "dog", "cat", "dog")));
    }

    public void testSource() throws IOException {
        assertThat(singleValueInSource().collect("value(source['foo'] + 'o')"), equalTo(List.of("cato", "dogo")));
    }

    public void testMultipleSourceValues() throws IOException {
        assertThat(
            multiValueInSource().collect("for (String v : source['foo']) {value(v + 'o')}"),
            equalTo(List.of("cato", "chickeno", "dogo", "pigo"))
        );
    }

    public void testDocValues() throws IOException {
        assertThat(singleValueInDocValues().collect("value(doc['foo'].value + 'o')"), equalTo(List.of("cato", "dogo")));
    }

    public void testMultipleDocValuesValues() throws IOException {
        assertThat(twoValuesInDocValues().collect(ADD_O), equalTo(List.of("cato", "pigo", "chickeno", "dogo")));
    }

    public void testTermQuery() throws IOException {
        TestCase c = twoValuesInDocValues();
        StringScriptFieldScript.LeafFactory addO = c.script(ADD_O);
        assertThat(c.collect(addO.termQuery("foo", "cat"), addO), equalTo(List.of()));
        assertThat(c.collect(addO.termQuery("foo", "cato"), addO), equalTo(List.of("cato", "pigo")));
        assertThat(c.collect(addO.termQuery("foo", "dogo"), addO), equalTo(List.of("chickeno", "dogo")));
    }

    public void testPrefixQuery() throws IOException {
        TestCase c = twoValuesInDocValues();
        StringScriptFieldScript.LeafFactory addO = c.script(ADD_O);
        assertThat(c.collect(addO.prefixQuery("foo", "catdog"), addO), equalTo(List.of()));
        assertThat(c.collect(addO.prefixQuery("foo", "cat"), addO), equalTo(List.of("cato", "pigo")));
        assertThat(c.collect(addO.prefixQuery("foo", "dogo"), addO), equalTo(List.of("chickeno", "dogo")));
        assertThat(c.collect(addO.prefixQuery("foo", "d"), addO), equalTo(List.of("chickeno", "dogo")));
    }

    @Override
    protected MappedFieldType[] fieldTypes() {
        return new MappedFieldType[] { new KeywordFieldType("foo") };
    }

    @Override
    protected ScriptContext<StringScriptFieldScript.Factory> scriptContext() {
        return StringScriptFieldScript.CONTEXT;
    }

    @Override
    protected StringScriptFieldScript.LeafFactory newLeafFactory(
        StringScriptFieldScript.Factory factory,
        Map<String, Object> params,
        SourceLookup source,
        DocLookup fieldData
    ) {
        return factory.newFactory(params, source, fieldData);
    }

    @Override
    protected StringScriptFieldScript newInstance(
        StringScriptFieldScript.LeafFactory leafFactory,
        LeafReaderContext context,
        List<String> result
    ) throws IOException {
        return leafFactory.newInstance(context, result::add);
    }

    private TestCase randomStrings() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
        });
    }

    private TestCase singleValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"cat\"}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"dog\"}"))));
        });
    }

    private TestCase multiValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\", \"chicken\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\", \"pig\"]}"))));
        });
    }

    private TestCase singleValueInDocValues() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("cat"))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("dog"))));
        });
    }

    private TestCase twoValuesInDocValues() throws IOException {
        return testCase(iw -> {
            List<IndexableField> doc = new ArrayList<>();
            doc.add(new SortedSetDocValuesField("foo", new BytesRef("cat")));
            doc.add(new SortedSetDocValuesField("foo", new BytesRef("pig")));
            iw.addDocument(doc);
            doc.clear();
            doc.add(new SortedSetDocValuesField("foo", new BytesRef("chicken")));
            doc.add(new SortedSetDocValuesField("foo", new BytesRef("dog")));
            iw.addDocument(doc);
        });
    }

    private static final String ADD_O = "for (String s: doc['foo']) {value(s + 'o')}";
}
