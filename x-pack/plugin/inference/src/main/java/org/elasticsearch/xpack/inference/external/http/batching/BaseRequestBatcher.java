/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

class BaseRequestBatcher<GroupingKey> implements RequestBatcher<GroupingKey> {
    private final HashMap<GroupingKey, Entry<GroupingKey>> entries = new LinkedHashMap<>();

    private final BatchingComponents components;
    private final HttpClientContext context;

    BaseRequestBatcher(BatchingComponents components, HttpClientContext context) {
        this.components = Objects.requireNonNull(components);
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public Iterator<Runnable> iterator() {
        return new RequestIterator<>(entries.values().iterator(), components, context);
    }

    @Override
    public void add(BatchableRequest<GroupingKey> request) {
        if (request.input() == null || request.requestCreator() == null || request.listener() == null) {
            return;
        }

        entries.compute(request.requestCreator().key(), (account, entry) -> {
            if (entry == null) {
                return new Entry<>(
                    request.requestCreator(),
                    request.input().size(),
                    new LinkedList<>(List.of(new RequestRange(request.input(), 0, request.input().size(), request.listener())))
                );
            }

            entry.requests().add(new RequestRange(request.input(), entry.total, entry.total + request.input().size(), request.listener()));
            return new Entry<>(entry.requestCreator, entry.total + request.input().size(), entry.requests);
        });
    }

    @Override
    public void add(List<? extends BatchableRequest<GroupingKey>> requests) {
        for (var request : requests) {
            add(request);
        }
    }

    private record Entry<K>(RequestCreator<K> requestCreator, int total, List<RequestRange> requests) {}

    /**
     * Records the start and end index within the final batched request that this individual request
     * embody.
     * @param input the text input
     * @param start the index of the beginning of this request (inclusive)
     * @param end the index of the end of this request (exclusive)
     * @param listener the listener for this particular request
     */
    private record RequestRange(List<String> input, int start, int end, ActionListener<InferenceServiceResults> listener) {}

    private record RequestIterator<K>(Iterator<Entry<K>> iterator, BatchingComponents components, HttpClientContext context)
        implements
            Iterator<Runnable> {

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Runnable next() {
            var entry = iterator.next();

            var combinedInput = entry.requests.stream().map(RequestRange::input).flatMap(Collection::stream).toList();

            // TODO make a new ActionListener class that takes multiple listeners
            ActionListener<InferenceServiceResults> otherListener = ActionListener.wrap(result -> {
                for (var request : entry.requests) {
                    request.listener.onResponse(result.subList(request.start, request.end));
                }
            }, e -> {
                for (var request : entry.requests) {
                    request.listener.onFailure(e);
                }
            });

            return entry.requestCreator.createRequest(combinedInput, components, context, otherListener);
        }
    }
}
