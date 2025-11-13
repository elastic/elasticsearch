/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase()
// @LuceneTestCase.AwaitsFix(bugUrl = "TODO")
public class LargeTopHitsIT extends ESIntegTestCase {

    private static final String TERMS_AGGS_FIELD_1 = "terms1";
    private static final String TERMS_AGGS_FIELD_2 = "terms2";
    private static final String TERMS_AGGS_FIELD_3 = "terms3";
    private static final String SORT_FIELD = "sort";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(CustomScriptPlugin.class, FetchPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of("5", script -> "5", "doc['sort'].value", CustomScriptPlugin::sortDoubleScript);
        }

        private static Double sortDoubleScript(Map<String, Object> vars) {
            Map<?, ?> doc = (Map) vars.get("doc");
            return ((Number) ((ScriptDocValues<?>) doc.get("sort")).get(0)).doubleValue();
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() {
            return Collections.singletonMap("Math.random()", script -> randomDouble());
        }
    }

    public static String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        initSmallIdx();
        // initLargeIdx();
        ensureSearchable();
    }

    private void initSmallIdx() throws IOException {
        createIndex("small_idx");
        ensureGreen("small_idx");
        populateIndex("small_idx", 50);
    }

    private void initLargeIdx() throws IOException {
        createIndex("large_idx");
        ensureGreen("large_idx");
        populateIndex("large_idx", 50_000);
    }

    public void testSmallSingleQuery() {
        query("small_idx");
    }

    // public void test1000SmallQueries() {
    // for (int i = 0; i < 1000; i++) {
    // query("small_idx");
    // }
    // }

    private void createIndex(String idxName) {
        assertAcked(
            prepareCreate(idxName).setMapping(
                TERMS_AGGS_FIELD_1,
                "type=keyword",
                TERMS_AGGS_FIELD_2,
                "type=keyword",
                TERMS_AGGS_FIELD_3,
                "type=keyword",
                "text",
                "type=text,store=true",
                "large_text_1",
                "type=text,store=false",
                "large_text_2",
                "type=text,store=false",
                "large_text_3",
                "type=text,store=false",
                "large_text_4",
                "type=text,store=false",
                "large_text_5",
                "type=text,store=false"
            )
        );
    }

    private void populateIndex(String idxName, int nDocs) throws IOException {
        for (int i = 0; i < nDocs; i++) {
            List<IndexRequestBuilder> builders = new ArrayList<>();
            builders.add(
                prepareIndex(idxName).setId(Integer.toString(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field(TERMS_AGGS_FIELD_1, "val" + (i / 1000))
                            .field(TERMS_AGGS_FIELD_2, "val" + (i / 10))
                            .field(TERMS_AGGS_FIELD_3, "val" + (i / 3))
                            .field(SORT_FIELD, i)
                            .field("text", "some text to entertain")
                            .field("large_text_1", Strings.repeat("this is a text field 1 ", 20000))
                            .field("large_text_2", Strings.repeat("this is a text field 2 ", 20000))
                            .field("large_text_3", Strings.repeat("this is a text field 3 ", 20000))
                            .field("large_text_4", Strings.repeat("this is a text field 4 ", 20000))
                            .field("large_text_5", Strings.repeat("this is a text field 5 ", 20000))
                            .field("field1", 5)
                            .field("field2", 2.71)
                            .endObject()
                    )
            );

            indexRandom(true, builders);
        }
    }

    private static void query(String indexName) {
        assertNoFailuresAndResponse(
            prepareSearch(indexName).addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(TERMS_AGGS_FIELD_1)
                    .subAggregation(
                        terms("terms").executionHint(randomExecutionHint())
                            .field(TERMS_AGGS_FIELD_2)
                            .subAggregation(
                                terms("terms").executionHint(randomExecutionHint())
                                    .field(TERMS_AGGS_FIELD_2)
                                    .subAggregation(topHits("hits").sort(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC)))
                            )
                    )
            ),
            response -> {
                Terms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
            }
        );
    }

    public static class FetchPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
            return Collections.singletonList(fetchContext -> {
                if (fetchContext.getIndexName().equals("small_idx")) {
                    return new FetchSubPhaseProcessor() {

                        private LeafSearchLookup leafSearchLookup;

                        @Override
                        public void setNextReader(LeafReaderContext ctx) {
                            leafSearchLookup = fetchContext.getSearchExecutionContext().lookup().getLeafSearchLookup(ctx);
                        }

                        @Override
                        public void process(FetchSubPhase.HitContext hitContext) {
                            leafSearchLookup.setDocument(hitContext.docId());
                            FieldLookup fieldLookup = leafSearchLookup.fields().get("text");
                            hitContext.hit().setDocumentField(new DocumentField("text_stored_lookup", fieldLookup.getValues()));
                        }

                        @Override
                        public StoredFieldsSpec storedFieldsSpec() {
                            return StoredFieldsSpec.NO_REQUIREMENTS;
                        }
                    };
                }
                return null;
            });
        }
    }
}
