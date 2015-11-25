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

package org.elasticsearch.common;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;

// Not a real unit tests with mocks, but with a single node, because we mock the scroll
// search behaviour and it changes then this test will not catch this.
public class SearchScrollIteratorTests extends ESSingleNodeTestCase {

    public void testSearchScrollIterator() {
        createIndex("index");
        int numDocs = scaledRandomIntBetween(0, 128);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("index", "type", Integer.toString(i))
                    .setSource("field", "value" + i)
                    .get();
        }
        client().admin().indices().prepareRefresh().get();

        int i = 0;
        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // randomize size, because that also controls how many actual searches will happen:
        sourceBuilder.size(scaledRandomIntBetween(1, 10));
        searchRequest.source(sourceBuilder);
        Iterable<SearchHit> hits = SearchScrollIterator.createIterator(client(), TimeValue.timeValueSeconds(10), searchRequest);
        for (SearchHit hit : hits) {
            assertThat(hit.getId(), equalTo(Integer.toString(i)));
            assertThat(hit.getSource().get("field"), equalTo("value" + i));
            i++;
        }
        assertThat(i, equalTo(numDocs));
    }

}
