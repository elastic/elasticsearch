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

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

@ESIntegTestCase.SuiteScopeTestCase
public class UnmappedFieldValidationIT extends ESIntegTestCase {

    @Override
    protected int maximumNumberOfShards() {
        return 5;
    }
    

    @Override
    protected int minimumNumberOfShards() {
        return 5;
    }


    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").addMapping("type", "foo", "type=keyword").get());
        assertAcked(prepareCreate("idx_unmapped").addMapping("type", "bar", "type=keyword").get());
        indexRandom(true,
                client().prepareIndex("idx", "type").setSource("foo", "1"),
                client().prepareIndex("idx", "type").setSource("foo", "2"),
                client().prepareIndex("idx", "type").setSource("foo", "3"),
                client().prepareIndex("idx", "type").setSource("foo", "4"),
                client().prepareIndex("idx", "type").setSource("foo", "5"),
                client().prepareIndex("idx_unmapped", "type").setSource("bar", "1"),
                client().prepareIndex("idx_unmapped", "type").setSource("bar", "2"),
                client().prepareIndex("idx_unmapped", "type").setSource("bar", "3"),
                client().prepareIndex("idx_unmapped", "type").setSource("bar", "4"),
                client().prepareIndex("idx_unmapped", "type").setSource("bar", "5")
                );
    }

    public void testUnmappedTerms() {
        
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(terms("my_terms").field(randomFrom("foo", "bar"))).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(5, terms.getBuckets().size());
    }
    
    public void testStrictUnmappedStringTermsSingleIndex() {
        try {
            client().prepareSearch("idx_unmapped").setBatchedReduceSize(2).setCheckFieldNames(true)
                .addAggregation(terms("my_terms1").field("foo")).get();
            fail("All unmapped fields failure expected");
        } catch (SearchPhaseExecutionException e) {
            assertTrue(e.getCause().getMessage().contains("The following fields were unmapped in all indices searched: [foo]"));
        }
    }
    
    public void testStrictUnmappedStringTermsMultiIndex() {
        try {
            client().prepareSearch("idx","idx_unmapped").setCheckFieldNames(true)
                .addAggregation(terms("my_terms").field("all_unmapped_str")).get();
            fail("All unmapped fields failure expected");
        } catch (SearchPhaseExecutionException e) {
            assertTrue(e.getCause().getMessage().contains(
                    "The following fields were unmapped in all indices searched: [all_unmapped_str]"));
        }
    }  
    
    public void testStrictUnmappedStringTermsMultiIndexMultiReduce() {
        expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch("idx", "idx_unmapped").setBatchedReduceSize(2).setCheckFieldNames(true)
                        .addAggregation(terms("my_terms").field("all_unmapped_str")).get()
               );
    }
    
    public void testStrictPartiallyUnmappedStringTerms() {
        SearchResponse response = client().prepareSearch("idx","idx_unmapped").setBatchedReduceSize(2)
                .setCheckFieldNames(true)
                .addAggregation(terms("my_terms").field("foo")).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(5, terms.getBuckets().size());
    }  
    
    public void testLaxUnmappedStringTerms() {
        SearchResponse response = client().prepareSearch("idx","idx_unmapped").setCheckFieldNames(false)
                .addAggregation(terms("my_terms").field("all_unmapped_str")).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(0, terms.getBuckets().size());
    }       
}
