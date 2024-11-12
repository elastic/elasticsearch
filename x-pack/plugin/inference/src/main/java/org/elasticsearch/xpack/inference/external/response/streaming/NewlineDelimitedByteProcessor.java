/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.regex.Pattern;

/**
 * Processes HttpResult bytes into lines separated by newlines, delimited by either line-feed or carriage-return line-feed.
 * Downstream is responsible for validating the structure of the lines after they have been separated.
 * Because Upstream (Apache) can send us a single line split between two HttpResults, this processor will aggregate bytes from the last
 * HttpResult and append them to the front of the next HttpResult.
 * When onComplete is called, the last batch is always flushed to the downstream onNext.
 */
public class NewlineDelimitedByteProcessor extends DelegatingProcessor<HttpResult, Deque<String>> {
    private static final Pattern END_OF_LINE_REGEX = Pattern.compile("\\n|\\r\\n");
    private volatile String previousTokens = "";

    @Override
    protected void next(HttpResult item) {
        // discard empty result and go to the next
        if (item.isBodyEmpty()) {
            upstream().request(1);
            return;
        }

        var body = previousTokens + new String(item.body(), StandardCharsets.UTF_8);
        var lines = END_OF_LINE_REGEX.split(body, -1); // -1 because we actually want trailing empty strings

        var results = new ArrayDeque<String>(lines.length);
        for (var i = 0; i < lines.length - 1; i++) {
            var line = lines[i].trim();
            if (line.isBlank() == false) {
                results.offer(line);
            }
        }

        previousTokens = lines[lines.length - 1].trim();

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(results);
        }
    }

    @Override
    public void onComplete() {
        if (previousTokens.isBlank()) {
            super.onComplete();
        } else if (isClosed.compareAndSet(false, true)) {
            var results = new ArrayDeque<String>(1);
            results.offer(previousTokens);
            downstream().onNext(results);
        }
    }
}
