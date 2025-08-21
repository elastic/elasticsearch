/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class StreamingInferenceTestUtils {

    public static Deque<ServerSentEvent> events(String... data) {
        var item = new ArrayDeque<ServerSentEvent>();
        Arrays.stream(data).map(ServerSentEvent::new).forEach(item::offer);
        return item;
    }

    @SuppressWarnings("unchecked")
    public static Matcher<Iterable<? extends StreamingChatCompletionResults.Result>> containsResults(String... results) {
        Matcher<StreamingChatCompletionResults.Result>[] resultMatcher = Arrays.stream(results)
            .map(StreamingChatCompletionResults.Result::new)
            .map(Matchers::equalTo)
            .toArray(Matcher[]::new);
        return Matchers.contains(resultMatcher);
    }
}
