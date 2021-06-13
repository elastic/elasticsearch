/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;

public class BucketScriptAggregatorTests extends AggregatorTestCase {
    private final String SCRIPT_NAME = "script_name";

    @Override
    protected ScriptService getMockScriptService() {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME,
            Collections.singletonMap(SCRIPT_NAME, script -> script.get("the_avg")),
            Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    public void testScript() throws IOException {
        MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType("number_field", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType("the_field");

        FiltersAggregationBuilder filters = new FiltersAggregationBuilder("placeholder", new MatchAllQueryBuilder())
            .subAggregation(new TermsAggregationBuilder("the_terms").userValueTypeHint(ValueType.STRING).field("the_field")
                .subAggregation(new AvgAggregationBuilder("the_avg").field("number_field")))
            .subAggregation(new BucketScriptPipelineAggregationBuilder("bucket_script",
                Collections.singletonMap("the_avg", "the_terms['test1']>the_avg.value"),
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, SCRIPT_NAME, Collections.emptyMap())));


        testCase(filters, new MatchAllDocsQuery(), iw -> {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("the_field", new BytesRef("test1")));
            doc.add(new SortedNumericDocValuesField("number_field", 19));
            iw.addDocument(doc);

            doc = new Document();
            doc.add(new SortedSetDocValuesField("the_field", new BytesRef("test2")));
            doc.add(new SortedNumericDocValuesField("number_field", 55));
            iw.addDocument(doc);
        }, f -> {
           assertThat(((InternalSimpleValue)(f.getBuckets().get(0).getAggregations().get("bucket_script"))).value,
               equalTo(19.0));
        }, fieldType, fieldType1);
    }

    private void testCase(FiltersAggregationBuilder aggregationBuilder, Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalFilters> verify, MappedFieldType... fieldType) throws IOException {

        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                InternalFilters filters;
                filters = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
                verify.accept(filters);
            }
        }
    }
}
