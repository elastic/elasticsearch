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
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class TermsShardMinDocCountTests extends ElasticsearchIntegrationTest {
    private static final String index = "someindex";
    private static final String type = "testtype";
    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(SignificantTermsAggregatorFactory.ExecutionMode.values()).toString();
    }

    // see https://github.com/elasticsearch/elasticsearch/issues/5998
    @Test
    public void shardMinDocCountSignificantTermsTest() throws Exception {

        String termtype = "string";
        if (randomBoolean()) {
            termtype = "long";
        }
        assertAcked(prepareCreate(index).setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0).addMapping(type, "{\"properties\":{\"text\": {\"type\": \"" + termtype + "\"}}}"));
        ensureYellow(index);
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

        addTermsDocs("1", 1, 0, indexBuilders);//high score but low doc freq
        addTermsDocs("2", 1, 0, indexBuilders);
        addTermsDocs("3", 1, 0, indexBuilders);
        addTermsDocs("4", 1, 0, indexBuilders);
        addTermsDocs("5", 3, 1, indexBuilders);//low score but high doc freq
        addTermsDocs("6", 3, 1, indexBuilders);
        addTermsDocs("7", 0, 3, indexBuilders);// make sure the terms all get score > 0 except for this one
        indexRandom(true, false, indexBuilders);

        // first, check that indeed when not setting the shardMinDocCount parameter 0 terms are returned
        SearchResponse response = client().prepareSearch(index)
                .addAggregation(
                        (new FilterAggregationBuilder("inclass").filter(FilterBuilders.termFilter("class", true)))
                                .subAggregation(new SignificantTermsBuilder("mySignificantTerms").field("text").minDocCount(2).size(2).executionHint(randomExecutionHint()))
                )
                .execute()
                .actionGet();
        assertSearchResponse(response);
        InternalFilter filteredBucket = response.getAggregations().get("inclass");
        SignificantTerms sigterms = filteredBucket.getAggregations().get("mySignificantTerms");
        assertThat(sigterms.getBuckets().size(), equalTo(0));


        response = client().prepareSearch(index)
                .addAggregation(
                        (new FilterAggregationBuilder("inclass").filter(FilterBuilders.termFilter("class", true)))
                                .subAggregation(new SignificantTermsBuilder("mySignificantTerms").field("text").minDocCount(2).shardMinDocCount(2).size(2).executionHint(randomExecutionHint()))
                )
                .execute()
                .actionGet();
        assertSearchResponse(response);
        filteredBucket = response.getAggregations().get("inclass");
        sigterms = filteredBucket.getAggregations().get("mySignificantTerms");
        assertThat(sigterms.getBuckets().size(), equalTo(2));

    }

    private void addTermsDocs(String term, int numInClass, int numNotInClass, List<IndexRequestBuilder> builders) {
        String sourceClass = "{\"text\": \"" + term + "\", \"class\":" + "true" + "}";
        String sourceNotClass = "{\"text\": \"" + term + "\", \"class\":" + "false" + "}";
        for (int i = 0; i < numInClass; i++) {
            builders.add(client().prepareIndex(index, type).setSource(sourceClass));
        }
        for (int i = 0; i < numNotInClass; i++) {
            builders.add(client().prepareIndex(index, type).setSource(sourceNotClass));
        }
    }

    // see https://github.com/elasticsearch/elasticsearch/issues/5998
    @Test
    public void shardMinDocCountTermsTest() throws Exception {
        final String [] termTypes = {"string", "long", "integer", "float", "double"};
        String termtype = termTypes[randomInt(termTypes.length - 1)];

        assertAcked(prepareCreate(index).setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0).addMapping(type, "{\"properties\":{\"text\": {\"type\": \"" + termtype + "\"}}}"));
        ensureYellow(index);
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

        addTermsDocs("1", 1, indexBuilders);//low doc freq but high score
        addTermsDocs("2", 1, indexBuilders);
        addTermsDocs("3", 1, indexBuilders);
        addTermsDocs("4", 1, indexBuilders);
        addTermsDocs("5", 3, indexBuilders);//low score but high doc freq
        addTermsDocs("6", 3, indexBuilders);
        indexRandom(true, false, indexBuilders);

        // first, check that indeed when not setting the shardMinDocCount parameter 0 terms are returned
        SearchResponse response = client().prepareSearch(index)
                .addAggregation(
                        new TermsBuilder("myTerms").field("text").minDocCount(2).size(2).executionHint(randomExecutionHint()).order(Terms.Order.term(true))
                )
                .execute()
                .actionGet();
        assertSearchResponse(response);
        Terms sigterms = response.getAggregations().get("myTerms");
        assertThat(sigterms.getBuckets().size(), equalTo(0));


        response = client().prepareSearch(index)
                .addAggregation(
                        new TermsBuilder("myTerms").field("text").minDocCount(2).shardMinDocCount(2).size(2).executionHint(randomExecutionHint()).order(Terms.Order.term(true))
                )
                .execute()
                .actionGet();
        assertSearchResponse(response);
        sigterms = response.getAggregations().get("myTerms");
        assertThat(sigterms.getBuckets().size(), equalTo(2));

    }

    private void addTermsDocs(String term, int numDocs, List<IndexRequestBuilder> builders) {
        String sourceClass = "{\"text\": \"" + term + "\"}";
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex(index, type).setSource(sourceClass));
        }

    }
}
