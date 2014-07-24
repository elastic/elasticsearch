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

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.exclude.Exclude;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.exclude;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class ExcludeTests extends ElasticsearchIntegrationTest {

    static int numDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        List<IndexRequestBuilder> builders = new ArrayList<>();

        // case 1

        numDocs = randomIntBetween(3, 20);
        for (int i = 0; i < numDocs / 2; i++) {
            builders.add(client().prepareIndex("idx", "type1", ""+i+1).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .field("tag", "tag1")
                    .endObject()));
        }
        for (int i = numDocs / 2; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx", "type1", ""+i+1).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .field("tag", "tag2")
                    .endObject()));
        }

        // case 2

        final String[] gender = {"f", "f", "f", "f", "m", "m", "mf"};
        final String[] colour = {"r", "r", "r", "b", "b", "r", "r"};
        final String[] name = {"f1", "f2", "f3", "f4", "m1", "m2", "mf1"};

        for (int i = 0; i < gender.length ; i++) {
            builders.add(client().prepareIndex("idx2", "type2", "" + i + 1).setSource(jsonBuilder()
                    .startObject()
                    .field("gender", gender[i])
                    .field("colour", colour[i])
                    .field("name", name[i])
                    .endObject()));
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void asGlobalWithStatsSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .setQuery(QueryBuilders.termQuery("tag", "tag1"))
                .addAggregation(exclude("global").excludeQuery(true)
                        .subAggregation(stats("value_stats").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Exclude exclude = response.getAggregations().get("global");
        assertThat(exclude, notNullValue());
        assertThat(exclude.getName(), equalTo("global"));
        assertThat(exclude.getDocCount(), equalTo((long) numDocs));
        assertThat(exclude.getAggregations().asList().isEmpty(), is(false));

        Stats stats = exclude.getAggregations().get("value_stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("value_stats"));
        long sum = 0;
        for (int i = 0; i < numDocs; ++i) {
            sum += i + 1;
        }
        assertThat(stats.getAvg(), equalTo((double) sum / numDocs));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo((double) numDocs));
        assertThat(stats.getCount(), equalTo((long) numDocs));
        assertThat(stats.getSum(), equalTo((double) sum));
    }

    @Test
    public void queryAsFilterWithStatsSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .setQuery(QueryBuilders.termQuery("tag", "tag1"))
                .addAggregation(exclude("exclude").excludeQuery(false)
                        .subAggregation(stats("value_stats").field("value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Exclude exclude = response.getAggregations().get("exclude");
        assertThat(exclude, notNullValue());
        assertThat(exclude.getName(), equalTo("exclude"));
        assertThat(exclude.getDocCount(), equalTo((long) (numDocs / 2)));
        assertThat(exclude.getAggregations().asList().isEmpty(), is(false));

        Stats stats = exclude.getAggregations().get("value_stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("value_stats"));
        long sum = 0;
        for (int i = 0; i < numDocs / 2; ++i) {
            sum += i + 1;
        }
        assertThat(stats.getAvg(), equalTo((double) sum / (numDocs / 2)));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo((double) (numDocs / 2)));
        assertThat(stats.getCount(), equalTo((long) (numDocs / 2)));
        assertThat(stats.getSum(), equalTo((double) sum));
    }

    @Test
    public void noQueryOtherFiltersAndUntouchedQuery() throws Exception {
        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(
                        QueryBuilders.filteredQuery(
                                QueryBuilders.matchAllQuery(),
                                FilterBuilders.andFilter(
                                        FilterBuilders.termFilter("gender", "f").filterName("gender"),
                                        FilterBuilders.termFilter("colour", "r").filterName("colour")
                                )
                        )
                )
                .addAggregation(exclude("exclude").excludeQuery(true).excludeFilters(Lists.newArrayList("gender"))
                        .subAggregation(terms("facet").field("gender")))
                .addAggregation(exclude("untouched")
                        .subAggregation(terms("facet").field("gender")))
                .execute().actionGet();

        assertSearchResponse(response);

        assertThat(response.getHits().getTotalHits(), equalTo(3l));

        Exclude exclude = response.getAggregations().get("exclude");
        assertThat(exclude, notNullValue());
        assertThat(exclude.getName(), equalTo("exclude"));
        assertThat(exclude.getDocCount(), equalTo(5l));
        assertThat(exclude.getAggregations().asList().isEmpty(), is(false));

        Terms excludedTerms = exclude.getAggregations().get("facet");
        assertThat(excludedTerms, notNullValue());
        assertThat(excludedTerms.getName(), equalTo("facet"));

        for(Terms.Bucket bucket : excludedTerms.getBuckets()){
            if(bucket.getKey().equals("f")) {
                assertThat(bucket.getDocCount(), equalTo(3l));
            } else if(bucket.getKey().equals("m")) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else if(bucket.getKey().equals("mf")) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            }
        }

        Exclude untouched = response.getAggregations().get("untouched");
        assertThat(untouched, notNullValue());
        assertThat(untouched.getName(), equalTo("untouched"));
        assertThat(untouched.getDocCount(), equalTo(3l));
        assertThat(untouched.getAggregations().asList().isEmpty(), is(false));

        Terms globalTerms = untouched.getAggregations().get("facet");
        assertThat(globalTerms, notNullValue());
        assertThat(globalTerms.getName(), equalTo("facet"));

        for(Terms.Bucket bucket : globalTerms.getBuckets()){
            if(bucket.getKey().equals("f")) {
                assertThat(bucket.getDocCount(), equalTo(3l));
            } else if(bucket.getKey().equals("m") || bucket.getKey().equals("mf")) {
                fail("gender filter should filter these cases");
            }
        }
    }

    @Test
    public void queryAsFilterAndOtherFilters() throws Exception {
        SearchResponse response = client().prepareSearch("idx2")
                .setQuery(
                        QueryBuilders.filteredQuery(
                                QueryBuilders.regexpQuery("gender","m?f"),
                                FilterBuilders.andFilter(
                                        FilterBuilders.termFilter("gender", "f").filterName("gender"),
                                        FilterBuilders.termFilter("colour", "r").filterName("colour")
                                )
                        )
                )
                .addAggregation(exclude("exclude").excludeFilters(Lists.newArrayList("gender"))
                        .subAggregation(terms("facet").field("gender")))
                .addAggregation(exclude("exclude_more").excludeQuery(true).excludeFilters(Lists.newArrayList("colour"))
                        .subAggregation(terms("facet").field("gender")))
                .execute().actionGet();

        assertSearchResponse(response);

        assertThat(response.getHits().getTotalHits(), equalTo(3l));

        Exclude exclude = response.getAggregations().get("exclude");
        assertThat(exclude, notNullValue());
        assertThat(exclude.getName(), equalTo("exclude"));
        assertThat(exclude.getDocCount(), equalTo(4l));
        assertThat(exclude.getAggregations().asList().isEmpty(), is(false));

        Terms excludedTerms = exclude.getAggregations().get("facet");
        assertThat(excludedTerms, notNullValue());
        assertThat(excludedTerms.getName(), equalTo("facet"));

        for(Terms.Bucket bucket : excludedTerms.getBuckets()){
            if(bucket.getKey().equals("f")) {
                assertThat(bucket.getDocCount(), equalTo(3l));
            } else if(bucket.getKey().equals("mf")) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else if(bucket.getKey().equals("m")) {
                fail("query should filter this case");
            }
        }

        Exclude excludeMore = response.getAggregations().get("exclude_more");
        assertThat(excludeMore, notNullValue());
        assertThat(excludeMore.getName(), equalTo("exclude_more"));
        assertThat(excludeMore.getDocCount(), equalTo(4l));
        assertThat(excludeMore.getAggregations().asList().isEmpty(), is(false));

        Terms excludeMoreTerms = excludeMore.getAggregations().get("facet");
        assertThat(excludeMoreTerms, notNullValue());
        assertThat(excludeMoreTerms.getName(), equalTo("facet"));

        for(Terms.Bucket bucket : excludeMoreTerms.getBuckets()){
            if(bucket.getKey().equals("f")) {
                assertThat(bucket.getDocCount(), equalTo(4l));
            } else if(bucket.getKey().equals("m") || bucket.getKey().equals("mf")) {
                fail("gender filter should filter these cases");
            }
        }
    }

    @Test
    public void nonTopLevel() throws Exception {
        try {
            client().prepareSearch("idx")
                    .setQuery(QueryBuilders.termQuery("tag", "tag1"))
                    .addAggregation(exclude("exclude")
                            .subAggregation(exclude("exclude")))
                    .execute().actionGet();

            fail("expected to fail executing non-top-level exclude aggregator. exclude aggregations are only allowed as top level" +
                    "aggregations");
        } catch (ElasticsearchException ese) {
            // go team!!
        }
    }
}
