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
        assertThat(execute(indexBuilder, "\"cat\""), equalTo(List.of("cat", "cat")));
    }

    public void testSource() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"cat\"}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"dog\"}"))));
        };
        assertThat(execute(indexBuilder, "source['foo']"), equalTo(List.of("cat", "dog")));
    }

    public void testDocValues() throws IOException {
        CheckedConsumer<RandomIndexWriter, IOException> indexBuilder = iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("cat"))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("dog"))));
        };
        assertThat(execute(indexBuilder, "doc['foo'].value", new KeywordFieldType("foo")), equalTo(List.of("cat", "dog")));
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
    protected StringScriptFieldsScript newInstance(StringScriptFieldsScript.LeafFactory leafFactory, LeafReaderContext context) throws IOException {
        return leafFactory.newInstance(context);
    }

    @Override
    protected void collect(StringScriptFieldsScript script, List<String> result) {
        result.add(script.execute());
    }
}
