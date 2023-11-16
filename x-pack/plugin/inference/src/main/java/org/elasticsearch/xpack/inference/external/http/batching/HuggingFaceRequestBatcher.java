/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

public class HuggingFaceRequestBatcher implements RequestBatcher<HuggingFaceAccount, TextExpansionResults>, Iterable<Runnable> {
    private final HashMap<HuggingFaceAccount, Entry> entries = new LinkedHashMap<>();

    public Iterator<Runnable> iterator() {
        return new RequestIterator(entries.values().iterator());
    }

    @Override
    public void add(Handler<HuggingFaceAccount, TextExpansionResults> handler, List<String> input, ActionListener<HttpResult> listener) {
        entries.compute(handler.creator().key(), (account, entry) -> {
            if (entry == null) {
                return new Entry(
                    handler.creator(),
                    input.size(),
                    new LinkedList<>(List.of(new InternalRequest(input, 0, input.size(), listener)))
                );
            }

            entry.requests().add(new InternalRequest(input, entry.total, entry.total + input.size(), listener));
            return new Entry(entry.creator, entry.total + input.size(), entry.requests);
        });
    }

    public record Entry(RequestCreator<HuggingFaceAccount> creator, int total, List<InternalRequest> requests) {

    }

    private record InternalRequest(List<String> input, int start, int end, ActionListener<HttpResult> listener) {

    }

    private record RequestIterator(Iterator<Entry> iterator, RequestCreator.Components components) implements Iterator<Runnable> {

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Runnable next() {
            var entry = iterator.next();

            var l = entry.requests.stream().map(InternalRequest::input).flatMap(Collection::stream).toList();

            ActionListener<HttpResult> otherListener = ActionListener.wrap(result -> {
                // TODO: loop through the entry.requests and use List.subList to to extract the results for each listener and call
                // onResponse
            }, e -> {
                // TODO: loop over the entry.listener and call onFailure(e)
            });

            return entry.creator().createRequest(l, components, otherListener);
        }
    }
}
