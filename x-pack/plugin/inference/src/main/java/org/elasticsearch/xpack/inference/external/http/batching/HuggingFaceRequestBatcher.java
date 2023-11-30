/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

// TODO TextExpansionResults shouldn't be here, the batcher must handle ALL response types
// we could have the InferenceServiceResults interface require a subList method that splits up the results or something
// or maybe the response types just implement two interfaces InferenceServiceResults and Splitable or something
public class HuggingFaceRequestBatcher implements RequestBatcher<HuggingFaceAccount> {
    private final HashMap<HuggingFaceAccount, Entry> entries = new LinkedHashMap<>();

    private final BatchingComponents components;
    private final HttpClientContext context;

    public HuggingFaceRequestBatcher(BatchingComponents components, HttpClientContext context) {
        this.components = Objects.requireNonNull(components);
        this.context = Objects.requireNonNull(context);
    }

    public Iterator<Runnable> iterator() {
        return new RequestIterator(entries.values().iterator(), components, context);
    }

    @Override
    public void add(BatchableRequest<HuggingFaceAccount> request) {
        entries.compute(request.handler().creator().key(), (account, entry) -> {
            if (entry == null) {
                return new Entry(
                    request.handler(),
                    request.input().size(),
                    new LinkedList<>(List.of(new InternalRequest(request.input(), 0, request.input().size(), request.listener())))
                );
            }

            entry.requests()
                .add(new InternalRequest(request.input(), entry.total, entry.total + request.input().size(), request.listener()));
            return new Entry(entry.handler, entry.total + request.input().size(), entry.requests);
        });
    }

    @Override
    public void add(List<? extends BatchableRequest<HuggingFaceAccount>> requests) {
        for (var request : requests) {
            add(request);
        }
    }

    public record Entry(TransactionHandler<HuggingFaceAccount> handler, int total, List<InternalRequest> requests) {

    }

    private record InternalRequest(List<String> input, int start, int end, ActionListener<HttpResult> listener) {

    }

    private record RequestIterator(Iterator<Entry> iterator, BatchingComponents components, HttpClientContext context)
        implements
            Iterator<Runnable> {

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Runnable next() {
            var entry = iterator.next();

            var l = entry.requests.stream().map(InternalRequest::input).flatMap(Collection::stream).toList();

            // TODO make a new ActionListener class that takes multiple listeners
            ActionListener<HttpResult> otherListener = ActionListener.wrap(result -> {

                // TODO: loop through the entry.requests and use List.subList to to extract the results for each listener and call
                // onResponse
            }, e -> {
                // TODO: loop over the entry.listener and call onFailure(e)
            });

            return entry.handler().creator().createRequest(l, components, context, otherListener);
        }
    }

    public record Factory(BatchingComponents components) implements RequestBatcherFactory<HuggingFaceAccount> {
        @Override
        public RequestBatcher<HuggingFaceAccount> create(HttpClientContext context) {
            return new HuggingFaceRequestBatcher(components, context);
        }
    }
}
