/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.percolator;

import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class PercolatorFacetsTests extends AbstractIntegrationTest {

    @Test
    public void testFacets() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        int numQueries = atLeast(250);
        int numUniqueQueries = between(1, numQueries / 2);
        String[] values = new String[numUniqueQueries];
        for (int i = 0; i < values.length; i++) {
            values[i] = "value" + i;
        }
        int[] expectedCount = new int[numUniqueQueries];

        logger.info("--> registering {} queries", numQueries);
        for (int i = 0; i < numQueries; i++) {
            String value = values[i % numUniqueQueries];
            expectedCount[i % numUniqueQueries]++;
            QueryBuilder queryBuilder = matchQuery("field1", value);
            client().prepareIndex("test", "_percolator", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", queryBuilder).field("field2", "b").endObject())
                    .execute().actionGet();
        }
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        for (int i = 0; i < numQueries; i++) {
            String value = values[i % numUniqueQueries];
            PercolateRequestBuilder percolateRequestBuilder = client().preparePercolate()
                    .setIndices("test").setDocumentType("type")
                    .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", value).endObject()))
                    .addFacet(FacetBuilders.termsFacet("a").field("field2"));

            if (randomBoolean()) {
                percolateRequestBuilder.setPercolateQuery(matchAllQuery());
            }
            if (randomBoolean()) {
                percolateRequestBuilder.setScore(true);
            } else {
                percolateRequestBuilder.setSort(true).setSize(numQueries);
            }

            boolean countOnly = randomBoolean();
            if (countOnly) {
                percolateRequestBuilder.setOnlyCount(countOnly);
            }

            PercolateResponse response = percolateRequestBuilder.execute().actionGet();
            assertThat(response.getCount(), equalTo((long) expectedCount[i % numUniqueQueries]));
            if (!countOnly) {
                assertThat(response.getMatches(), arrayWithSize(expectedCount[i % numUniqueQueries]));
            }

            assertThat(response.getFacets().facets().size(), equalTo(1));
            assertThat(response.getFacets().facets().get(0).getName(), equalTo("a"));
            assertThat(((TermsFacet)response.getFacets().facets().get(0)).getEntries().size(), equalTo(1));
            assertThat(((TermsFacet)response.getFacets().facets().get(0)).getEntries().get(0).getCount(), equalTo(expectedCount[i % values.length]));
            assertThat(((TermsFacet)response.getFacets().facets().get(0)).getEntries().get(0).getTerm().string(), equalTo("b"));
        }
    }

}
