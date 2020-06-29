/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MappedFieldType;
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
        assertThat(randomLongs().collect("value(10)"), equalTo(List.of(10L, 10L)));
    }

    public void testTwoConstants() throws IOException {
        assertThat(randomLongs().collect("value(10); value(20)"), equalTo(List.of(10L, 20L, 10L, 20L)));
    }

    public void testSource() throws IOException {
        assertThat(singleValueInSource().collect("value(source['foo'] * 10)"), equalTo(List.of(10L, 100L)));
    }

    public void testMultileSourceValues() throws IOException {
        assertThat(multiValueInSource().collect("for (long l : source['foo']){value(l * 10)}"), equalTo(List.of(10L, 20L, 100L, 200L)));
    }

    public void testDocValues() throws IOException {
        assertThat(singleValueInDocValues().collect("value(doc['foo'].value * 10)"), equalTo(List.of(10L, 100L)));
    }

    public void testMultipleDocValuesValues() throws IOException {
        assertThat(
            multipleValuesInDocValues().collect("for (long l : doc['foo']) {value(l * 10)}"),
            equalTo(List.of(10L, 20L, 100L, 200L))
        );
    }

    private TestCase randomLongs() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        });
    }

    private TestCase singleValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10}"))));
        });
    }

    private TestCase multiValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1, 2]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [10, 20]}"))));
        });
    }

    private TestCase singleValueInDocValues() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10)));
        });
    }

    private TestCase multipleValuesInDocValues() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1), new SortedNumericDocValuesField("foo", 2)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10), new SortedNumericDocValuesField("foo", 20)));
        });
    }

    @Override
    protected MappedFieldType[] fieldTypes() {
        return new MappedFieldType[] { new NumberFieldType("foo", NumberType.LONG) };
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
