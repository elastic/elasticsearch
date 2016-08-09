/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.script.NativeSignificanceScoreScriptNoParams;
import org.elasticsearch.search.aggregations.bucket.script.NativeSignificanceScoreScriptWithParams;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.GND;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ScriptHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.search.aggregations.bucket.SharedSignificantTermsTestMethods;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.significantTerms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SignificantTermsSignificanceScoreIT extends ESIntegTestCase {

    static final String INDEX_NAME = "testidx";
    static final String DOC_TYPE = "doc";
    static final String TEXT_FIELD = "text";
    static final String CLASS_FIELD = "class";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CustomSignificanceHeuristicPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(CustomSignificanceHeuristicPlugin.class);
    }

    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(SignificantTermsAggregatorFactory.ExecutionMode.values()).toString();
    }

    public void testPlugin() throws Exception {
        String type = randomBoolean() ? "text" : "long";
        String settings = "{\"index.number_of_shards\": 1, \"index.number_of_replicas\": 0}";
        SharedSignificantTermsTestMethods.index01Docs(type, settings, this);
        SearchResponse response = client().prepareSearch(INDEX_NAME).setTypes(DOC_TYPE)
                .addAggregation(
                        terms("class")
                        .field(CLASS_FIELD)
                                .subAggregation((significantTerms("sig_terms"))
                                .field(TEXT_FIELD)
                                .significanceHeuristic(new SimpleHeuristic())
                                .minDocCount(1)
                        )
                )
                .execute()
                .actionGet();
        assertSearchResponse(response);
        StringTerms classes = response.getAggregations().get("class");
        assertThat(classes.getBuckets().size(), equalTo(2));
        for (Terms.Bucket classBucket : classes.getBuckets()) {
            Map<String, Aggregation> aggs = classBucket.getAggregations().asMap();
            assertTrue(aggs.containsKey("sig_terms"));
            SignificantTerms agg = (SignificantTerms) aggs.get("sig_terms");
            assertThat(agg.getBuckets().size(), equalTo(2));
            Iterator<SignificantTerms.Bucket> bucketIterator = agg.iterator();
            SignificantTerms.Bucket sigBucket = bucketIterator.next();
            String term = sigBucket.getKeyAsString();
            String classTerm = classBucket.getKeyAsString();
            assertTrue(term.equals(classTerm));
            assertThat(sigBucket.getSignificanceScore(), closeTo(2.0, 1.e-8));
            sigBucket = bucketIterator.next();
            assertThat(sigBucket.getSignificanceScore(), closeTo(1.0, 1.e-8));
        }

        // we run the same test again but this time we do not call assertSearchResponse() before the assertions
        // the reason is that this would trigger toXContent and we would like to check that this has no potential side effects

        response = client().prepareSearch(INDEX_NAME).setTypes(DOC_TYPE)
                .addAggregation(
                        terms("class")
                        .field(CLASS_FIELD)
                                .subAggregation((significantTerms("sig_terms"))
                                .field(TEXT_FIELD)
                                .significanceHeuristic(new SimpleHeuristic())
                                .minDocCount(1)
                        )
                )
                .execute()
                .actionGet();

        classes = (StringTerms) response.getAggregations().get("class");
        assertThat(classes.getBuckets().size(), equalTo(2));
        for (Terms.Bucket classBucket : classes.getBuckets()) {
            Map<String, Aggregation> aggs = classBucket.getAggregations().asMap();
            assertTrue(aggs.containsKey("sig_terms"));
            SignificantTerms agg = (SignificantTerms) aggs.get("sig_terms");
            assertThat(agg.getBuckets().size(), equalTo(2));
            Iterator<SignificantTerms.Bucket> bucketIterator = agg.iterator();
            SignificantTerms.Bucket sigBucket = bucketIterator.next();
            String term = sigBucket.getKeyAsString();
            String classTerm = classBucket.getKeyAsString();
            assertTrue(term.equals(classTerm));
            assertThat(sigBucket.getSignificanceScore(), closeTo(2.0, 1.e-8));
            sigBucket = bucketIterator.next();
            assertThat(sigBucket.getSignificanceScore(), closeTo(1.0, 1.e-8));
        }
    }

    public static class CustomSignificanceHeuristicPlugin extends Plugin implements ScriptPlugin, SearchPlugin {
        @Override
        public List<SearchExtensionSpec<SignificanceHeuristic, SignificanceHeuristicParser>> getSignificanceHeuristics() {
            return singletonList(new SearchExtensionSpec<SignificanceHeuristic, SignificanceHeuristicParser>(SimpleHeuristic.NAME,
                    SimpleHeuristic::new, SimpleHeuristic::parse));
        }

        @Override
        public List<NativeScriptFactory> getNativeScripts() {
            return Arrays.asList(new NativeSignificanceScoreScriptNoParams.Factory(),
                    new NativeSignificanceScoreScriptWithParams.Factory());
        }
    }

    public static class SimpleHeuristic extends SignificanceHeuristic {
        public static final String NAME = "simple";

        public SimpleHeuristic() {
        }

        /**
         * Read from a stream.
         */
        public SimpleHeuristic(StreamInput in) throws IOException {
            // Nothing to read
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // Nothing to write
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME).endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return 1;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            return true;
        }

        /**
         * @param subsetFreq   The frequency of the term in the selected sample
         * @param subsetSize   The size of the selected sample (typically number of docs)
         * @param supersetFreq The frequency of the term in the superset from which the sample was taken
         * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
         * @return a "significance" score
         */
        @Override
        public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
            return subsetFreq / subsetSize > supersetFreq / supersetSize ? 2.0 : 1.0;
        }

        public static SignificanceHeuristic parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher)
                throws IOException, QueryShardException {
            parser.nextToken();
            return new SimpleHeuristic();
        }
    }

    public void testXContentResponse() throws Exception {
        String type = randomBoolean() ? "text" : "long";
        String settings = "{\"index.number_of_shards\": 1, \"index.number_of_replicas\": 0}";
        SharedSignificantTermsTestMethods.index01Docs(type, settings, this);
        SearchResponse response = client().prepareSearch(INDEX_NAME).setTypes(DOC_TYPE)
                .addAggregation(terms("class").field(CLASS_FIELD).subAggregation(significantTerms("sig_terms").field(TEXT_FIELD)))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        StringTerms classes = response.getAggregations().get("class");
        assertThat(classes.getBuckets().size(), equalTo(2));
        for (Terms.Bucket classBucket : classes.getBuckets()) {
            Map<String, Aggregation> aggs = classBucket.getAggregations().asMap();
            assertTrue(aggs.containsKey("sig_terms"));
            SignificantTerms agg = (SignificantTerms) aggs.get("sig_terms");
            assertThat(agg.getBuckets().size(), equalTo(1));
            String term = agg.iterator().next().getKeyAsString();
            String classTerm = classBucket.getKeyAsString();
            assertTrue(term.equals(classTerm));
        }

        XContentBuilder responseBuilder = XContentFactory.jsonBuilder();
        responseBuilder.startObject();
        classes.toXContent(responseBuilder, null);
        responseBuilder.endObject();

        String result = "{\"class\":{\"doc_count_error_upper_bound\":0,\"sum_other_doc_count\":0,"
                + "\"buckets\":["
                + "{"
                + "\"key\":\"0\","
                + "\"doc_count\":4,"
                + "\"sig_terms\":{"
                + "\"doc_count\":4,"
                + "\"buckets\":["
                + "{"
                + "\"key\":" + (type.equals("long") ? "0," : "\"0\",")
                + "\"doc_count\":4,"
                + "\"score\":0.39999999999999997,"
                + "\"bg_count\":5"
                + "}"
                + "]"
                + "}"
                + "},"
                + "{"
                + "\"key\":\"1\","
                + "\"doc_count\":3,"
                + "\"sig_terms\":{"
                + "\"doc_count\":3,"
                + "\"buckets\":["
                + "{"
                + "\"key\":" + (type.equals("long") ? "1," : "\"1\",")
                + "\"doc_count\":3,"
                + "\"score\":0.75,"
                + "\"bg_count\":4"
                + "}]}}]}}";
        assertThat(responseBuilder.string(), equalTo(result));

    }

    public void testDeletesIssue7951() throws Exception {
        String settings = "{\"index.number_of_shards\": 1, \"index.number_of_replicas\": 0}";
        assertAcked(prepareCreate(INDEX_NAME).setSettings(settings)
                .addMapping("doc", "text", "type=keyword", CLASS_FIELD, "type=keyword"));
        String[] cat1v1 = {"constant", "one"};
        String[] cat1v2 = {"constant", "uno"};
        String[] cat2v1 = {"constant", "two"};
        String[] cat2v2 = {"constant", "duo"};
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "1")
                .setSource(TEXT_FIELD, cat1v1, CLASS_FIELD, "1"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "2")
                .setSource(TEXT_FIELD, cat1v2, CLASS_FIELD, "1"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "3")
                .setSource(TEXT_FIELD, cat2v1, CLASS_FIELD, "2"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "4")
                .setSource(TEXT_FIELD, cat2v2, CLASS_FIELD, "2"));
        indexRandom(true, false, indexRequestBuilderList);

        // Now create some holes in the index with selective deletes caused by updates.
        // This is the scenario that caused this issue https://github.com/elastic/elasticsearch/issues/7951
        // Scoring algorithms throw exceptions if term docFreqs exceed the reported size of the index
        // from which they are taken so need to make sure this doesn't happen.
        String[] text = cat1v1;
        indexRequestBuilderList.clear();
        for (int i = 0; i < 50; i++) {
            text = text == cat1v2 ? cat1v1 : cat1v2;
            indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "1").setSource(TEXT_FIELD, text, CLASS_FIELD, "1"));
        }
        indexRandom(true, false, indexRequestBuilderList);

        client().prepareSearch(INDEX_NAME).setTypes(DOC_TYPE)
                .addAggregation(
                        terms("class")
                        .field(CLASS_FIELD)
                        .subAggregation(
                                significantTerms("sig_terms")
                                        .field(TEXT_FIELD)
                                        .minDocCount(1)))
                .execute()
                .actionGet();
    }

    public void testBackgroundVsSeparateSet() throws Exception {
        String type = randomBoolean() ? "text" : "long";
        String settings = "{\"index.number_of_shards\": 1, \"index.number_of_replicas\": 0}";
        SharedSignificantTermsTestMethods.index01Docs(type, settings, this);
        testBackgroundVsSeparateSet(new MutualInformation(true, true), new MutualInformation(true, false));
        testBackgroundVsSeparateSet(new ChiSquare(true, true), new ChiSquare(true, false));
        testBackgroundVsSeparateSet(new GND(true), new GND(false));
    }

    // compute significance score by
    // 1. terms agg on class and significant terms
    // 2. filter buckets and set the background to the other class and set is_background false
    // both should yield exact same result
    public void testBackgroundVsSeparateSet(SignificanceHeuristic significanceHeuristicExpectingSuperset,
                                            SignificanceHeuristic significanceHeuristicExpectingSeparateSets) throws Exception {

        SearchResponse response1 = client().prepareSearch(INDEX_NAME).setTypes(DOC_TYPE)
                .addAggregation(terms("class")
                        .field(CLASS_FIELD)
                        .subAggregation(
                                significantTerms("sig_terms")
                                        .field(TEXT_FIELD)
                                        .minDocCount(1)
                                        .significanceHeuristic(
                                                significanceHeuristicExpectingSuperset)))
                .execute()
                .actionGet();
        assertSearchResponse(response1);
        SearchResponse response2 = client().prepareSearch(INDEX_NAME).setTypes(DOC_TYPE)
                .addAggregation(filter("0", QueryBuilders.termQuery(CLASS_FIELD, "0"))
                        .subAggregation(significantTerms("sig_terms")
                                .field(TEXT_FIELD)
                                .minDocCount(1)
                                .backgroundFilter(QueryBuilders.termQuery(CLASS_FIELD, "1"))
                                .significanceHeuristic(significanceHeuristicExpectingSeparateSets)))
                .addAggregation(filter("1", QueryBuilders.termQuery(CLASS_FIELD, "1"))
                        .subAggregation(significantTerms("sig_terms")
                                .field(TEXT_FIELD)
                                .minDocCount(1)
                                .backgroundFilter(QueryBuilders.termQuery(CLASS_FIELD, "0"))
                                .significanceHeuristic(significanceHeuristicExpectingSeparateSets)))
                .execute()
                .actionGet();

        StringTerms classes = response1.getAggregations().get("class");

        SignificantTerms sigTerms0 = ((SignificantTerms) (classes.getBucketByKey("0").getAggregations().asMap().get("sig_terms")));
        assertThat(sigTerms0.getBuckets().size(), equalTo(2));
        double score00Background = sigTerms0.getBucketByKey("0").getSignificanceScore();
        double score01Background = sigTerms0.getBucketByKey("1").getSignificanceScore();
        SignificantTerms sigTerms1 = ((SignificantTerms) (classes.getBucketByKey("1").getAggregations().asMap().get("sig_terms")));
        double score10Background = sigTerms1.getBucketByKey("0").getSignificanceScore();
        double score11Background = sigTerms1.getBucketByKey("1").getSignificanceScore();

        Aggregations aggs = response2.getAggregations();

        sigTerms0 = (SignificantTerms) ((InternalFilter) aggs.get("0")).getAggregations().getAsMap().get("sig_terms");
        double score00SeparateSets = sigTerms0.getBucketByKey("0").getSignificanceScore();
        double score01SeparateSets = sigTerms0.getBucketByKey("1").getSignificanceScore();

        sigTerms1 = (SignificantTerms) ((InternalFilter) aggs.get("1")).getAggregations().getAsMap().get("sig_terms");
        double score10SeparateSets = sigTerms1.getBucketByKey("0").getSignificanceScore();
        double score11SeparateSets = sigTerms1.getBucketByKey("1").getSignificanceScore();

        assertThat(score00Background, equalTo(score00SeparateSets));
        assertThat(score01Background, equalTo(score01SeparateSets));
        assertThat(score10Background, equalTo(score10SeparateSets));
        assertThat(score11Background, equalTo(score11SeparateSets));
    }

    public void testScoresEqualForPositiveAndNegative() throws Exception {
        indexEqualTestData();
        testScoresEqualForPositiveAndNegative(new MutualInformation(true, true));
        testScoresEqualForPositiveAndNegative(new ChiSquare(true, true));
    }

    public void testScoresEqualForPositiveAndNegative(SignificanceHeuristic heuristic) throws Exception {

        //check that results for both classes are the same with exclude negatives = false and classes are routing ids
        SearchResponse response = client().prepareSearch("test")
                .addAggregation(terms("class").field("class").subAggregation(significantTerms("mySignificantTerms")
                        .field("text")
                        .executionHint(randomExecutionHint())
                        .significanceHeuristic(heuristic)
                        .minDocCount(1).shardSize(1000).size(1000)))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        StringTerms classes = response.getAggregations().get("class");
        assertThat(classes.getBuckets().size(), equalTo(2));
        Iterator<Terms.Bucket> classBuckets = classes.getBuckets().iterator();

        Aggregations aggregations = classBuckets.next().getAggregations();
        SignificantTerms sigTerms = aggregations.get("mySignificantTerms");

        Collection<SignificantTerms.Bucket> classA = sigTerms.getBuckets();
        Iterator<SignificantTerms.Bucket> classBBucketIterator = sigTerms.getBuckets().iterator();
        assertThat(classA.size(), greaterThan(0));
        for (SignificantTerms.Bucket classABucket : classA) {
            SignificantTerms.Bucket classBBucket = classBBucketIterator.next();
            assertThat(classABucket.getKey(), equalTo(classBBucket.getKey()));
            assertThat(classABucket.getSignificanceScore(), closeTo(classBBucket.getSignificanceScore(), 1.e-5));
        }
    }

    private void indexEqualTestData() throws ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test").setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0).addMapping("doc",
                "text", "type=text,fielddata=true", "class", "type=keyword"));
        createIndex("idx_unmapped");

        ensureGreen();
        String data[] = {
                "A\ta",
                "A\ta",
                "A\tb",
                "A\tb",
                "A\tb",
                "B\tc",
                "B\tc",
                "B\tc",
                "B\tc",
                "B\td",
                "B\td",
                "B\td",
                "B\td",
                "B\td",
                "A\tc d",
                "B\ta b"
        };

        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < data.length; i++) {
            String[] parts = data[i].split("\t");
            indexRequestBuilders.add(client().prepareIndex("test", "doc", "" + i)
                    .setSource("class", parts[0], "text", parts[1]));
        }
        indexRandom(true, false, indexRequestBuilders);
    }

    public void testScriptScore() throws ExecutionException, InterruptedException, IOException {
        indexRandomFrequencies01(randomBoolean() ? "text" : "long");
        ScriptHeuristic scriptHeuristic = getScriptSignificanceHeuristic();
        SearchResponse response = client().prepareSearch(INDEX_NAME)
                .addAggregation(terms("class").field(CLASS_FIELD)
                        .subAggregation(significantTerms("mySignificantTerms")
                        .field(TEXT_FIELD)
                        .executionHint(randomExecutionHint())
                        .significanceHeuristic(scriptHeuristic)
                        .minDocCount(1).shardSize(2).size(2)))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        for (Terms.Bucket classBucket : ((Terms) response.getAggregations().get("class")).getBuckets()) {
            SignificantTerms sigTerms = classBucket.getAggregations().get("mySignificantTerms");
            for (SignificantTerms.Bucket bucket : sigTerms.getBuckets()) {
                assertThat(bucket.getSignificanceScore(),
                        is((double) bucket.getSubsetDf() + bucket.getSubsetSize() + bucket.getSupersetDf() + bucket.getSupersetSize()));
            }
        }
    }

    private ScriptHeuristic getScriptSignificanceHeuristic() throws IOException {
        Script script = null;
        if (randomBoolean()) {
            Map<String, Object> params = null;
            params = new HashMap<>();
            params.put("param", randomIntBetween(1, 100));
            script = new Script("native_significance_score_script_with_params", ScriptType.INLINE, "native", params);
        } else {
            script = new Script("native_significance_score_script_no_params", ScriptType.INLINE, "native", null);
        }
        return new ScriptHeuristic(script);
    }

    private void indexRandomFrequencies01(String type) throws ExecutionException, InterruptedException {
        String textMappings = "type=" + type;
        if (type.equals("text")) {
            textMappings += ",fielddata=true";
        }
        assertAcked(prepareCreate(INDEX_NAME).addMapping(DOC_TYPE, TEXT_FIELD, textMappings, CLASS_FIELD, "type=keyword"));
        String[] gb = {"0", "1"};
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < randomInt(20); i++) {
            int randNum = randomInt(2);
            String[] text = new String[1];
            if (randNum == 2) {
                text = gb;
            } else {
                text[0] = gb[randNum];
            }
            indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE)
                    .setSource(TEXT_FIELD, text, CLASS_FIELD, randomBoolean() ? "one" : "zero"));
        }
        indexRandom(true, indexRequestBuilderList);
    }

    public void testReduceFromSeveralShards() throws IOException, ExecutionException, InterruptedException {
        SharedSignificantTermsTestMethods.aggregateAndCheckFromSeveralShards(this);
    }

}
