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
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.equalTo;

public class IntegerScriptFieldScriptTests extends ScriptFieldScriptTestCase<
        IntegerScriptFieldScript.Factory,
        IntegerScriptFieldScript.LeafFactory,
        Integer>{
    
    public void testConstant() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        };
        assertThat(execute(indexBuilder, "value(3)"), equalTo(List.of(3, 3)));
    }
    
    public void testTwoConstants() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        };
        assertThat(execute(indexBuilder, "value(3); value(2)"), equalTo(List.of(3, 2, 3, 2)));
    }
    
    public void testSource() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10}"))));
        };
        assertThat(execute(indexBuilder, "value(source['foo'] * 10)"), equalTo(List.of(10, 100)));
    }
    
    public void testTwoSourceFields() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1, \"bar\": 2}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10, \"bar\": 20}"))));
        };
        assertThat(
                execute(indexBuilder, "value(source['foo'] * 10); value(source['bar'] * 10)"),
                equalTo(List.of(10, 20, 100, 200))
        );
    }
    
    public void testDocValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10)));
        };
        assertThat(
                execute(indexBuilder, "value((int)doc['foo'].value * 10)", new NumberFieldMapper.NumberFieldType("foo", NumberFieldMapper.NumberType.INTEGER)),
                equalTo(List.of(10, 100))
        );
    }
    
    public void testTwoDocValuesValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(
                    List.of(
                            new SortedNumericDocValuesField("foo", 1),
                            new SortedNumericDocValuesField("foo", 2)
                    )
            );
            iw.addDocument(
                    List.of(
                            new SortedNumericDocValuesField("foo", 10),
                            new SortedNumericDocValuesField("foo", 20)
                    )
            );
        };
        assertThat(
                execute(indexBuilder, "for (int d : doc['foo']) {value(d * 10)}", new NumberFieldMapper.NumberFieldType("foo", NumberFieldMapper.NumberType.INTEGER)),
                equalTo(List.of(10, 20, 100, 200))
        );
    }
    
    
    @Override
    protected ScriptContext<IntegerScriptFieldScript.Factory> scriptContext() {
        return IntegerScriptFieldScript.CONTEXT;
    }
    
    @Override
    protected IntegerScriptFieldScript.LeafFactory newLeafFactory(
        IntegerScriptFieldScript.Factory factory, 
        Map<String, Object> params, 
        SearchLookup searchLookup) {
        return factory.newFactory(params, searchLookup);
    }
    
    @Override
    protected IntFunction<List<Integer>> newInstance(IntegerScriptFieldScript.LeafFactory leafFactory, LeafReaderContext context) throws IOException {
        IntegerScriptFieldScript script = leafFactory.newInstance(context);
        return docId -> {
            script.runForDoc(docId);
            List<Integer> list = new ArrayList<>(script.count());
            for (int i = 0; i < script.count(); i++) {
                list.add(script.values()[i]);
            }
            return list;
        };
    }
}
