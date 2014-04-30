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

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms.Bucket;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SignificantTermsMinDocCountTests extends ElasticsearchIntegrationTest {
    private static final String index = "someindex";
    private static final String type = "testtype";
    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(SignificantTermsAggregatorFactory.ExecutionMode.values()).toString();
    }
    @Test
    public void shardMinDocCountTest() throws Exception {

        String termtype = "string";
        if (randomBoolean()) {
            termtype = "long";
        }
        assertAcked(prepareCreate(index).setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0).addMapping(type, "{\"properties\":{\"text\": {\"type\": \"" + termtype + "\"}}}"));
        ensureYellow(index);
        indexTermsDocs("1", 1, 0);//high score but low doc freq
        indexTermsDocs("2", 1, 0);
        indexTermsDocs("3", 1, 0);
        indexTermsDocs("4", 1, 0);
        indexTermsDocs("5", 3, 1);//low score but high doc freq
        indexTermsDocs("6", 3, 1);
        indexTermsDocs("7", 0, 3);// make sure the terms all get score > 0 except for this one
        refresh();
        SearchResponse response = client().prepareSearch(index)
                .addAggregation(
                        (new FilterAggregationBuilder("inclass").filter(FilterBuilders.termFilter("class", true)))
                                .subAggregation(new SignificantTermsBuilder("mySignificantTerms").field("text").minDocCount(2).shardMinDocCount(2).size(2).executionHint(randomExecutionHint()))
                )
                .execute()
                .actionGet();
        assertSearchResponse(response);
        InternalFilter filteredBucket = response.getAggregations().get("inclass");
        SignificantTerms sigterms = filteredBucket.getAggregations().get("mySignificantTerms");
        assertThat(sigterms.getBuckets().size(), equalTo(2));

    }

    private void indexTermsDocs(String term, int numInClass, int numNotInClass) {
        String sourceClass = "{\"text\": \"" + term + "\", \"class\":" + "true" + "}";
        String sourceNotClass = "{\"text\": \"" + term + "\", \"class\":" + "false" + "}";
        for (int i = 0; i < numInClass; i++) {
            client().prepareIndex(index, type).setSource(sourceClass).execute().actionGet();
        }
        for (int i = 0; i < numNotInClass; i++) {
            client().prepareIndex(index, type).setSource(sourceNotClass).execute().actionGet();
        }
    }

}
