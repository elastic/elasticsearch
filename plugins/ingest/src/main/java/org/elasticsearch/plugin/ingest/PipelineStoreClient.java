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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Collections;
import java.util.Iterator;

public class PipelineStoreClient extends AbstractLifecycleComponent {

    private volatile Client client;
    private final Injector injector;
    private final TimeValue scrollTimeout;

    @Inject
    public PipelineStoreClient(Settings settings, Injector injector) {
        super(settings);
        this.injector = injector;
        this.scrollTimeout = settings.getAsTime("ingest.pipeline.store.scroll.timeout", TimeValue.timeValueSeconds(30));
    }

    @Override
    protected void doStart() {
        client = injector.getInstance(Client.class);
    }

    @Override
    protected void doStop() {
        client.close();
    }

    @Override
    protected void doClose() {
    }

    public Iterable<SearchHit> readAllPipelines() {
        // TODO: the search should be replaced with an ingest API when it is available
        SearchResponse searchResponse = client.prepareSearch(PipelineStore.INDEX)
                .setVersion(true)
                .setScroll(scrollTimeout)
                .addSort("_doc", SortOrder.ASC)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .get();

        if (searchResponse.getHits().getTotalHits() == 0) {
            return Collections.emptyList();
        }
        logger.debug("reading [{}] pipeline documents", searchResponse.getHits().totalHits());
        return new Iterable<SearchHit>() {
            @Override
            public Iterator<SearchHit> iterator() {
                return new SearchScrollIterator(searchResponse);
            }
        };
    }

    public boolean existPipeline(String pipelineId) {
        GetResponse response = client.prepareGet(PipelineStore.INDEX, PipelineStore.TYPE, pipelineId).get();
        return response.isExists();
    }

    class SearchScrollIterator implements Iterator<SearchHit> {

        private SearchResponse searchResponse;

        private int currentIndex;
        private SearchHit[] currentHits;

        SearchScrollIterator(SearchResponse searchResponse) {
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

                searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                        .setScroll(scrollTimeout)
                        .get();
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
            SearchHit hit = currentHits[currentIndex++];
            if (logger.isTraceEnabled()) {
                logger.trace("reading pipeline document [{}] with source [{}]", hit.getId(), hit.sourceAsString());
            }
            return hit;
        }
    }

}
