/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.http.batching.RequestBatcher;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequest;
import org.elasticsearch.xpack.inference.external.request.huggingface.HuggingFaceElserRequestEntity;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

public class HuggingFaceRequestBatcher implements RequestBatcher<HuggingFaceAccount>, Iterable<Runnable> {
    private final HashMap<HuggingFaceAccount, Entry> entries = new LinkedHashMap<>();

    public Iterator<Runnable> iterator() {
        return new RequestIterator(entries.values().iterator());
    }

    @Override
    public void add(RequestCreator<HuggingFaceAccount> creator, List<String> input, ActionListener<HttpResult> listener) {
        entries.compute(creator.key(), (account, entry) -> {
            if (entry == null) {
                return new Entry(creator, new LinkedList<>(List.of(new InternalRequest(request.input()))));
            }

            entry.requests().add(new InternalRequest(request.input()));
            return entry;
        });
    }

    public record Entry(RequestCreator<HuggingFaceAccount> creator, List<InternalRequest> requests) {

    }

    public record IndividualRequest(RequestCreator<HuggingFaceAccount> creator, List<String> input) {

    }

    private record InternalRequest(List<String> input, ActionListener<HttpResult> ) {

    }


    private record RequestIterator(Iterator<Entry> iterator) implements Iterator<Runnable> {

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Runnable next() {
            var entry = iterator.next();

            return entry.creator().create(entry.);
        }
    }
}
