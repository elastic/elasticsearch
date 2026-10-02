/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.xpack.core.inference.results.StreamingCompletionResults;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

public class StreamingInferenceTestUtils {

    public static Deque<ServerSentEvent> events(String... data) {
        var item = new ArrayDeque<ServerSentEvent>();
        Arrays.stream(data).map(ServerSentEvent::new).forEach(item::offer);
        return item;
    }

    public static Deque<ServerSentEvent> events(List<Pair<String, String>> data) {
        var item = new ArrayDeque<ServerSentEvent>();
        data.forEach(pair -> item.offer(new ServerSentEvent(pair.getKey(), pair.getValue())));
        return item;
    }

    @SuppressWarnings("unchecked")
    public static Matcher<Iterable<? extends StreamingCompletionResults.Result>> containsResults(String... results) {
        Matcher<StreamingCompletionResults.Result>[] resultMatcher = Arrays.stream(results)
            .map(StreamingCompletionResults.Result::new)
            .map(Matchers::equalTo)
            .toArray(Matcher[]::new);
        return Matchers.contains(resultMatcher);
    }
}
