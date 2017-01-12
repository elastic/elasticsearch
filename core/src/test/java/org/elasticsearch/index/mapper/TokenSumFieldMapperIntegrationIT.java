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

public class TokenSumFieldMapperIntegrationIT extends ESIntegTestCase {
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

    public TokenSumFieldMapperIntegrationIT(@Name("storeCountedFields") boolean storeCountedFields,
                                            @Name("loadCountedFields") boolean loadCountedFields) {
        this.storeCountedFields = storeCountedFields;
        this.loadCountedFields = loadCountedFields;
    }

    /**
     * It is possible to get the token sum in a search response.
     */
    public void testSearchReturnsTokenSum() throws IOException {
        init();

        assertSearchReturns(searchById("single"), "single");
        assertSearchReturns(searchById("numbers1"), "numbers1");
        assertSearchReturns(searchById("numbers2"), "numbers2");
        assertSearchReturns(searchById("nan1"), "nan1");
        assertSearchReturns(searchById("nan2"), "nan2");
        assertSearchReturns(searchById("nan3"), "nan3");
        assertSearchReturns(searchById("multi"), "multi");
        assertSearchReturns(searchById("multinumbers1"), "multinumbers1");
        assertSearchReturns(searchById("multinumbers2"), "multinumbers2");
    }

    /**
     * It is possible to search by sum of token.
     */
    public void testSearchByTokenSum() throws IOException {
        init();

        assertSearchReturns(searchByNumericRange(1, 1).get(), "single");
        assertSearchReturns(searchByNumericRange(6, 6).get(), "numbers1");
        assertSearchReturns(searchByNumericRange(21, 21).get(), "numbers2");
        assertSearchReturns(searchByNumericRange(6, 21).get(), "numbers1", "numbers2");
        assertSearchReturns(searchByNumericRange(-1, -1).get(), "nan1", "nan2", "nan3");
        assertSearchReturns(searchByNumericRange(25, 25).get(), "multi");
        assertSearchReturns(searchByNumericRange(30, 35).get(), "multinumbers1", "multinumbers2");
        assertSearchReturns(searchByNumericRange(1000, 1000).get(), "multinumbers2");
        assertSearchReturns(searchByNumericRange(1, 21).get(), "single", "numbers1", "numbers2", "multi", "multinumbers1", "multinumbers2");
    }

    /**
     * It is possible to search by token sum.
     */
    public void testFacetByTokenSum() throws IOException {
        init();

        String facetField = "foo.token_sum";
        SearchResponse result = searchByNumericRange(6, 21)
            .addAggregation(AggregationBuilders.terms("facet").field(facetField)).get();
        assertSearchReturns(result, "numbers1", "numbers2");
        assertThat(result.getAggregations().asList().size(), equalTo(1));
        Terms terms = (Terms) result.getAggregations().asList().get(0);
        assertThat(terms.getBuckets().size(), equalTo(2));
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
                                .startObject("token_sum")
                                    .field("type", "token_sum")
                                    .field("analyzer", "standard")
                                    .field("store", true)
                                .endObject()
                                .startObject("token_sum_unstored")
                                    .field("type", "token_sum")
                                    .field("analyzer", "standard")
                                .endObject()
                                .startObject("token_sum_with_doc_values")
                                    .field("type", "token_sum")
                                    .field("analyzer", "standard")
                                    .field("doc_values", true)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().endObject()).get();
        ensureGreen();

        assertEquals(DocWriteResponse.Result.CREATED, prepareIndex("single", "1").get().getResult());
        BulkResponse bulk = client().prepareBulk()
                .add(prepareIndex("numbers1", "1 2 3"))
                .add(prepareIndex("numbers2", "1 2 3 4 5 6"))
                .add(prepareIndex("nan1", "nan 2 3"))
                .add(prepareIndex("nan2", "1 nan 3"))
                .add(prepareIndex("nan3", "1 2 nan")).get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertEquals(DocWriteResponse.Result.CREATED,
                     prepareIndex("multi", "1 2", "3 4 5 6 7").get().getResult());
        bulk = client().prepareBulk()
                .add(prepareIndex("multinumbers1", "1 2", "3 4 5 6 7 8"))
            .add(prepareIndex("multinumbers2", "1 2", "3 4 5 6 7 8", "1000"))
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
                Arrays.asList("foo.token_sum", "foo.token_sum_unstored", "foo.token_sum_with_doc_values")
        )).gte(low).lte(high));
    }


    private SearchRequestBuilder prepareSearch() {
        SearchRequestBuilder request = client().prepareSearch("test").setTypes("test");
        request.addStoredField("foo.token_sum");
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
                assertSearchHit(hit, 1);
            } else if (id.equals("numbers1")) {
                assertSearchHit(hit, 6);
            } else if (id.equals("numbers2")) {
                assertSearchHit(hit, 21);
            } else if (id.equals("nan1")) {
                assertSearchHit(hit, -1);
            } else if (id.equals("nan2")) {
                assertSearchHit(hit, -1);
            } else if (id.equals("nan3")) {
                assertSearchHit(hit, -1);
            } else if (id.equals("multi")) {
                assertSearchHit(hit, 3, 25);
            } else if (id.equals("multinumbers1")) {
                assertSearchHit(hit, 3, 33);
            } else if (id.equals("multinumbers2")) {
                assertSearchHit(hit, 3, 33, 1000);
            } else {
                throw new ElasticsearchException("Unexpected response!");
            }
        }
    }


    private void assertSearchHit(SearchHit hit, int... termCounts) {
        assertThat(hit.field("foo.token_sum"), not(nullValue()));
        assertThat(hit.field("foo.token_sum").values().size(), equalTo(termCounts.length));
        for (int i = 0; i < termCounts.length; i++) {
            assertThat((Integer) hit.field("foo.token_sum").values().get(i), equalTo(termCounts[i]));
        }

        if (loadCountedFields && storeCountedFields) {
            assertThat(hit.field("foo").values().size(), equalTo(termCounts.length));
        }
    }
}
