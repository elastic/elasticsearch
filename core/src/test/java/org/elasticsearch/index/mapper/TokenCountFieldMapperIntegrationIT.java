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

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TokenCountFieldMapperIntegrationIT extends ESIntegTestCase {
    @ParametersFactory
    public static Iterable<Object[]> buildParameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (boolean storeCountedFields : new boolean[] { true, false }) {
            for (boolean loadCountedFields : new boolean[] { true, false }) {
                parameters.add(new Object[] { storeCountedFields, loadCountedFields });
            }
        }
        return parameters;
    }

    private final boolean storeCountedFields;
    private final boolean loadCountedFields;

    public TokenCountFieldMapperIntegrationIT(@Name("storeCountedFields") boolean storeCountedFields,
            @Name("loadCountedFields") boolean loadCountedFields) {
        this.storeCountedFields = storeCountedFields;
        this.loadCountedFields = loadCountedFields;
    }

    /**
     * It is possible to get the token count in a search response.
     */
    public void testSearchReturnsTokenCount() throws IOException {
        init();

        assertSearchReturns(searchById("single"), "single");
        assertSearchReturns(searchById("bulk1"), "bulk1");
        assertSearchReturns(searchById("bulk2"), "bulk2");
        assertSearchReturns(searchById("multi"), "multi");
        assertSearchReturns(searchById("multibulk1"), "multibulk1");
        assertSearchReturns(searchById("multibulk2"), "multibulk2");
        assertSearchReturns(searchById("numbers1"), "numbers1");
        assertSearchReturns(searchById("numbers2"), "numbers2");
        assertSearchReturns(searchById("nan1"), "nan1");
        assertSearchReturns(searchById("multinumbers1"), "multinumbers1");
    }

    /**
     * It is possible to search by token count.
     */
    public void testSearchByTokenCount() throws IOException {
        init();

        assertSearchReturns(searchByNumericRange(4, 4).get(), "single");
        assertSearchReturns(searchByNumericRange(10, 10).get(), "multibulk2");
        assertSearchReturns(searchByNumericRange(7, 10).get(), "multi", "multibulk1", "multibulk2");
        assertSearchReturns(searchByNumericRange(1, 10).get(), "single", "bulk1", "bulk2", "multi", "multibulk1",
            "multibulk2", "numbers1", "numbers2", "nan1", "multinumbers1");
        assertSearchReturns(searchByNumericRange(12, 12).get());
    }

    /**
     * It is possible to search by sum of token.
     */
    public void testSearchByTokenSum() throws IOException {
        init();

        assertSearchReturns(searchByNumericRangeWithTokenSum(100, 100).get(), "numbers1");
        assertSearchReturns(searchByNumericRangeWithTokenSum(600, 600).get(), "numbers2");
        assertSearchReturns(searchByNumericRangeWithTokenSum(100, 600).get(), "numbers1", "numbers2");
        assertSearchReturns(searchByNumericRangeWithTokenSum(700, 700).get(), "multinumbers1");
        assertSearchReturns(searchByNumericRangeWithTokenSum(1100, 1100).get(), "multinumbers1");
        assertSearchReturns(searchByNumericRangeWithTokenSum(500, 800).get(), "numbers2", "multinumbers1");
        assertSearchReturns(searchByNumericRangeWithTokenSum(100, 1100).get(), "numbers1", "numbers2", "multinumbers1");
        assertSearchReturns(searchByNumericRangeWithTokenSum(-1, -1).get(), "single", "bulk1", "bulk2", "multi",
                                    "multibulk1", "multibulk2", "nan1");
    }
    /**
     * It is possible to search by token count.
     */
    public void testFacetByTokenCount() throws IOException {
        init();

        String facetField = randomFrom(Arrays.asList(
            "foo.token_count", "foo.token_count_unstored", "foo.token_count_with_doc_values"));
        SearchResponse result = searchByNumericRange(1, 10)
                .addAggregation(AggregationBuilders.terms("facet").field(facetField)).get();
        assertSearchReturns(result, "single", "bulk1", "bulk2", "multi", "multibulk1", "multibulk2", "numbers1",
                "numbers2", "nan1", "multinumbers1");
        assertThat(result.getAggregations().asList().size(), equalTo(1));
        Terms terms = (Terms) result.getAggregations().asList().get(0);
        assertThat(terms.getBuckets().size(), equalTo(9));
    }

    /**
     * It is possible to search by token sum.
     */
    public void testFacetByTokenSum() throws IOException {
        init();

        String facetField = "foo.token_count_is_number";
        SearchResponse result = searchByNumericRangeWithTokenSum(100, 800)
            .addAggregation(AggregationBuilders.terms("facet").field(facetField)).get();
        assertSearchReturns(result, "numbers1", "numbers2", "multinumbers1");
        assertThat(result.getAggregations().asList().size(), equalTo(1));
        Terms terms = (Terms) result.getAggregations().asList().get(0);
        assertThat(terms.getBuckets().size(), equalTo(4));
    }

    private void init() throws IOException {
        prepareCreate("test").addMapping("test", jsonBuilder().startObject()
                .startObject("test")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "text")
                            .field("store", storeCountedFields)
                            .field("analyzer", "simple")
                            .startObject("fields")
                                .startObject("token_count")
                                    .field("type", "token_count")
                                    .field("analyzer", "standard")
                                    .field("store", true)
                                .endObject()
                                .startObject("token_count_unstored")
                                    .field("type", "token_count")
                                    .field("analyzer", "standard")
                                .endObject()
                                .startObject("token_count_with_doc_values")
                                    .field("type", "token_count")
                                    .field("analyzer", "standard")
                                    .field("doc_values", true)
                                .endObject()
                                .startObject("token_count_is_number")
                                    .field("type", "token_count")
                                    .field("analyzer", "standard")
                                    .field("store", true)
                                    .field("is_number", true)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().endObject()).get();
        ensureGreen();

        assertEquals(DocWriteResponse.Result.CREATED, prepareIndex("single", "I have four terms").get().getResult());
        BulkResponse bulk = client().prepareBulk()
                .add(prepareIndex("bulk1", "bulk three terms"))
                .add(prepareIndex("bulk2", "this has five bulk terms"))
                .add(prepareIndex("numbers1", "100"))
                .add(prepareIndex("numbers2", "100 200 300"))
                .add(prepareIndex("nan1", "100 bingo")).get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertEquals(DocWriteResponse.Result.CREATED,
                     prepareIndex("multi", "two terms", "wow now I have seven lucky terms").get().getResult());
        bulk = client().prepareBulk()
                .add(prepareIndex("multibulk1", "one", "oh wow now I have eight unlucky terms"))
            .add(prepareIndex("multibulk2", "six is a bunch of terms", "ten!  ten terms is just crazy!  too many too count!"))
            .add(prepareIndex("multinumbers1", "300 400", "500 600"))
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());

        assertThat(refresh().getFailedShards(), equalTo(0));
    }

    private IndexRequestBuilder prepareIndex(String id, String... texts) throws IOException {
        return client().prepareIndex("test", "test", id).setSource("foo", texts);
    }

    private SearchResponse searchById(String id) {
        return prepareSearch().setQuery(QueryBuilders.termQuery("_id", id)).get();
    }

    private SearchRequestBuilder searchByNumericRange(int low, int high) {
        return prepareSearch().setQuery(QueryBuilders.rangeQuery(randomFrom(
                Arrays.asList("foo.token_count", "foo.token_count_unstored", "foo.token_count_with_doc_values")
        )).gte(low).lte(high));
    }

    private SearchRequestBuilder searchByNumericRangeWithTokenSum(int low, int high) {
        return prepareSearch().setQuery(QueryBuilders.rangeQuery("foo.token_count_is_number").gte(low).lte(high));
    }

    private SearchRequestBuilder prepareSearch() {
        SearchRequestBuilder request = client().prepareSearch("test").setTypes("test");
        request.addStoredField("foo.token_count");
        request.addStoredField("foo.token_count_is_number");
        if (loadCountedFields) {
            request.addStoredField("foo");
        }
        return request;
    }

    private void assertSearchReturns(SearchResponse result, String... ids) {
        assertThat(result.getHits().getTotalHits(), equalTo((long) ids.length));
        assertThat(result.getHits().hits().length, equalTo(ids.length));
        List<String> foundIds = new ArrayList<>();
        for (SearchHit hit : result.getHits()) {
            foundIds.add(hit.id());
        }
        assertThat(foundIds, containsInAnyOrder(ids));
        for (SearchHit hit : result.getHits()) {
            String id = hit.id();
            if (id.equals("single")) {
                assertSearchHit(hit, 4);
                assertSearchHitWithTokenSum(hit, -1);
            } else if (id.equals("bulk1")) {
                assertSearchHit(hit, 3);
                assertSearchHitWithTokenSum(hit, -1);
            } else if (id.equals("bulk2")) {
                assertSearchHit(hit, 5);
                assertSearchHitWithTokenSum(hit, -1);
            } else if (id.equals("multi")) {
                assertSearchHit(hit, 2, 7);
                assertSearchHitWithTokenSum(hit, -1, -1);
            } else if (id.equals("multibulk1")) {
                assertSearchHit(hit, 1, 8);
                assertSearchHitWithTokenSum(hit, -1, -1);
            } else if (id.equals("multibulk2")) {
                assertSearchHit(hit, 6, 10);
                assertSearchHitWithTokenSum(hit, -1, -1);
            } else if (id.equals("numbers1")) {
                assertSearchHit(hit, 1);
                assertSearchHitWithTokenSum(hit, 100);
            } else if (id.equals("numbers2")) {
                assertSearchHit(hit, 3);
                assertSearchHitWithTokenSum(hit, 600);
            } else if (id.equals("nan1")) {
                assertSearchHit(hit, 2);
                assertSearchHitWithTokenSum(hit, -1);
            } else if (id.equals("multinumbers1")) {
                assertSearchHit(hit, 2, 2);
                assertSearchHitWithTokenSum(hit, 700, 1100);
            } else {
                throw new ElasticsearchException("Unexpected response!");
            }
        }
    }

    private void assertSearchHit(SearchHit hit, int... termCounts) {
        assertThat(hit.field("foo.token_count"), not(nullValue()));
        assertThat(hit.field("foo.token_count").values().size(), equalTo(termCounts.length));
        for (int i = 0; i < termCounts.length; i++) {
            assertThat((Integer) hit.field("foo.token_count").values().get(i), equalTo(termCounts[i]));
        }

        if (loadCountedFields && storeCountedFields) {
            assertThat(hit.field("foo").values().size(), equalTo(termCounts.length));
        }
    }

    private void assertSearchHitWithTokenSum(SearchHit hit, int... termCounts) {
        assertThat(hit.field("foo.token_count_is_number"), not(nullValue()));
        assertThat(hit.field("foo.token_count_is_number").values().size(), equalTo(termCounts.length));
        for (int i = 0; i < termCounts.length; i++) {
            assertThat((Integer) hit.field("foo.token_count_is_number").values().get(i), equalTo(termCounts[i]));
        }

        if (loadCountedFields && storeCountedFields) {
            assertThat(hit.field("foo").values().size(), equalTo(termCounts.length));
        }
    }
}
