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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class AggregationsBinaryIT extends ESIntegTestCase {

    private static final String STRING_FIELD_NAME = "s_value";
    private static final String INT_FIELD_NAME = "i_value";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            builders.add(client().prepareIndex("idx", "type").setSource(
                    jsonBuilder().startObject().field(STRING_FIELD_NAME, "val" + i).field(INT_FIELD_NAME, i).endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void testAggregationsBinary() throws Exception {
        TermsBuilder termsBuilder = AggregationBuilders.terms("terms").field(STRING_FIELD_NAME);
        TermsBuilder subTerm = AggregationBuilders.terms("subterms").field(INT_FIELD_NAME);

        // Create an XContentBuilder from sub aggregation
        XContentBuilder subTermContentBuilder = JsonXContent.contentBuilder().startObject();
        subTerm.toXContent(subTermContentBuilder, ToXContent.EMPTY_PARAMS);
        subTermContentBuilder.endObject();

        // Add sub aggregation as a XContentBuilder (binary_aggregation)
        termsBuilder.subAggregation(subTermContentBuilder);

        SearchResponse response = client().prepareSearch("idx").setTypes("type").addAggregation(termsBuilder).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsString(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Aggregations subAggs = bucket.getAggregations();
            assertThat(subAggs, notNullValue());
            assertThat(subAggs.asList().size(), equalTo(1));
            Terms subTerms = subAggs.get("subterms");
            assertThat(subTerms, notNullValue());
            List<Bucket> subTermsBuckets = subTerms.getBuckets();
            assertThat(subTermsBuckets, notNullValue());
            assertThat(subTermsBuckets.size(), equalTo(1));
            assertThat(((Number) subTermsBuckets.get(0).getKey()).intValue(), equalTo(i));
            assertThat(subTermsBuckets.get(0).getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void testAggregationsBinarySameContentType() throws Exception {
        TermsBuilder termsBuilder = AggregationBuilders.terms("terms").field(STRING_FIELD_NAME);
        TermsBuilder subTerm = AggregationBuilders.terms("subterms").field(INT_FIELD_NAME);

        // Create an XContentBuilder from sub aggregation

        XContentBuilder subTermContentBuilder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
        subTermContentBuilder.startObject();
        subTerm.toXContent(subTermContentBuilder, ToXContent.EMPTY_PARAMS);
        subTermContentBuilder.endObject();

        // Add sub aggregation as a XContentBuilder (binary_aggregation)
        termsBuilder.subAggregation(subTermContentBuilder);

        SearchResponse response = client().prepareSearch("idx").setTypes("type").addAggregation(termsBuilder).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsString(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Aggregations subAggs = bucket.getAggregations();
            assertThat(subAggs, notNullValue());
            assertThat(subAggs.asList().size(), equalTo(1));
            Terms subTerms = subAggs.get("subterms");
            assertThat(subTerms, notNullValue());
            List<Bucket> subTermsBuckets = subTerms.getBuckets();
            assertThat(subTermsBuckets, notNullValue());
            assertThat(subTermsBuckets.size(), equalTo(1));
            assertThat(((Number) subTermsBuckets.get(0).getKey()).intValue(), equalTo(i));
            assertThat(subTermsBuckets.get(0).getDocCount(), equalTo(1l));
        }
    }
}
