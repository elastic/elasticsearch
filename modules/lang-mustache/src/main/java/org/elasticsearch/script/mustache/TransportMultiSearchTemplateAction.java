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

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;

public class TransportMultiSearchTemplateAction extends HandledTransportAction<MultiSearchTemplateRequest, MultiSearchTemplateResponse> {

    private final TransportSearchTemplateAction searchTemplateAction;

    @Inject
    public TransportMultiSearchTemplateAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                              ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                              TransportSearchTemplateAction searchTemplateAction) {
        super(settings, MultiSearchTemplateAction.NAME, threadPool, transportService, actionFilters, resolver,
                MultiSearchTemplateRequest::new);
        this.searchTemplateAction = searchTemplateAction;
    }

    @Override
    protected void doExecute(MultiSearchTemplateRequest request, ActionListener<MultiSearchTemplateResponse> listener) {
        final AtomicArray<MultiSearchTemplateResponse.Item> responses = new AtomicArray<>(request.requests().size());
        final AtomicInteger counter = new AtomicInteger(responses.length());

        for (int i = 0; i < responses.length(); i++) {
            final int index = i;
            searchTemplateAction.execute(request.requests().get(i), new ActionListener<SearchTemplateResponse>() {
                @Override
                public void onResponse(SearchTemplateResponse searchTemplateResponse) {
                    responses.set(index, new MultiSearchTemplateResponse.Item(searchTemplateResponse, null));
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    responses.set(index, new MultiSearchTemplateResponse.Item(null, e));
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    MultiSearchTemplateResponse.Item[] items = responses.toArray(new MultiSearchTemplateResponse.Item[responses.length()]);
                    listener.onResponse(new MultiSearchTemplateResponse(items));
                }
            });
        }
    }
}
