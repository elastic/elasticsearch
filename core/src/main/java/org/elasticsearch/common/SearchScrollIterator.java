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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.Collections;
import java.util.Iterator;

/**
 * An iterator that easily helps to consume all hits from a scroll search.
 */
public final class SearchScrollIterator implements Iterator<SearchHit> {

    /**
     * Creates an iterator that returns all matching hits of a scroll search via an iterator.
     * The iterator will return all hits per scroll search and execute additional scroll searches
     * to get more hits until all hits have been returned by the scroll search on the ES side.
     */
    public static Iterable<SearchHit> createIterator(Client client, TimeValue scrollTimeout, SearchRequest searchRequest) {
        searchRequest.scroll(scrollTimeout);
        SearchResponse searchResponse = client.search(searchRequest).actionGet(scrollTimeout);
        if (searchResponse.getHits().getTotalHits() == 0) {
            return Collections.emptyList();
        } else {
            return () -> new SearchScrollIterator(client, scrollTimeout, searchResponse);
        }
    }

    private final Client client;
    private final TimeValue scrollTimeout;

    private int currentIndex;
    private SearchHit[] currentHits;
    private SearchResponse searchResponse;

    private SearchScrollIterator(Client client, TimeValue scrollTimeout, SearchResponse searchResponse) {
        this.client = client;
        this.scrollTimeout = scrollTimeout;
        this.searchResponse = searchResponse;
        this.currentHits = searchResponse.getHits().getHits();
    }

    @Override
    public boolean hasNext() {
        if (currentIndex < currentHits.length) {
            return true;
        } else {
            if (searchResponse == null) {
                return false;
            }

            SearchScrollRequest request = new SearchScrollRequest(searchResponse.getScrollId());
            request.scroll(scrollTimeout);
            searchResponse = client.searchScroll(request).actionGet(scrollTimeout);
            if (searchResponse.getHits().getHits().length == 0) {
                searchResponse = null;
                return false;
            } else {
                currentHits = searchResponse.getHits().getHits();
                currentIndex = 0;
                return true;
            }
        }
    }

    @Override
    public SearchHit next() {
        return currentHits[currentIndex++];
    }
}
