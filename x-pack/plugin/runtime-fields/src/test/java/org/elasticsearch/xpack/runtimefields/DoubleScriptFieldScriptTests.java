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
import org.apache.lucene.util.NumericUtils;
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

public class DoubleScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    DoubleScriptFieldScript,
    DoubleScriptFieldScript.Factory,
    DoubleScriptFieldScript.LeafFactory,
    Double> {

    public void testConstant() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(randomDouble()))));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(randomDouble()))));
        };
        assertThat(execute(indexBuilder, "value(3.14)"), equalTo(List.of(3.14, 3.14)));
    }

    public void testTwoConstants() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        };
        assertThat(execute(indexBuilder, "value(3.14); value(2.72)"), equalTo(List.of(3.14, 2.72, 3.14, 2.72)));
    }

    public void testSource() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10}"))));
        };
        assertThat(execute(indexBuilder, "value(source['foo'] * 10.1)"), equalTo(List.of(10.1, 101.0)));
    }

    public void testTwoSourceFields() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1, \"bar\": 2}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10, \"bar\": 20}"))));
        };
        assertThat(
            execute(indexBuilder, "value(source['foo'] * 10.1); value(source['bar'] * 10.2)"),
            equalTo(List.of(10.1, 20.4, 101.0, 204.0))
        );
    }

    public void testDocValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(1.1))));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(10.1))));
        };
        assertThat(
            execute(indexBuilder, "value(doc['foo'].value * 9.9)", new NumberFieldType("foo", NumberType.DOUBLE)),
            equalTo(List.of(10.89, 99.99))
        );
    }

    public void testTwoDocValuesValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(1.1)),
                    new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(2.2))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(10.1)),
                    new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(20.1))
                )
            );
        };
        assertThat(
            execute(indexBuilder, "for (double d : doc['foo']) {value(d * 9.9)}", new NumberFieldType("foo", NumberType.DOUBLE)),
            equalTo(List.of(10.89, 21.78, 99.99, 198.99))
        );
    }

    @Override
    protected ScriptContext<DoubleScriptFieldScript.Factory> scriptContext() {
        return DoubleScriptFieldScript.CONTEXT;
    }

    @Override
    protected DoubleScriptFieldScript.LeafFactory newLeafFactory(
        DoubleScriptFieldScript.Factory factory,
        Map<String, Object> params,
        SourceLookup source,
        DocLookup fieldData
    ) {
        return factory.newFactory(params, source, fieldData);
    }

    @Override
    protected DoubleScriptFieldScript newInstance(
        DoubleScriptFieldScript.LeafFactory leafFactory,
        LeafReaderContext context,
        List<Double> result
    ) throws IOException {
        return leafFactory.newInstance(context, result::add);
    }
}
