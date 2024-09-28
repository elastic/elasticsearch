/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.regex.Pattern;

/**
 * This isn't quite the newline-delimited json spec, because the Inference providers do not explicitly say they are using the spec.
 * This processor only converts the HttpResult bytes into a list of JSON strings.
 * This processor considers a JSON line to start with a '{' and end with a '}', followed by a newline.
 * It is possible for the HttpResult to contain half of a JSON line or multiple JSON lines.
 * JSON lines are delimited by newlines, either by line-feed or carriage-return line-feed.
 * JSON lines are *not* validated beyond starting and ending with curly braces.
 */
public class NdJsonProcessor extends DelegatingProcessor<HttpResult, Deque<String>> {
    private static final Logger log = LogManager.getLogger(NdJsonProcessor.class);
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
            if (line.isBlank()) {
                continue;
            }

            if (isValidJson(line)) {
                results.offer(line);
            } else {
                upstream().cancel();
                onError(line);
                return;
            }
        }

        var lastLine = lines[lines.length - 1].trim();
        if ((lastLine.isBlank() == false) && isValidJson(lastLine)) {
            results.offer(lastLine);
            previousTokens = "";
        } else {
            previousTokens = lastLine;
        }

        if (results.isEmpty()) {
            upstream().request(1);
        } else {
            downstream().onNext(results);
        }
    }

    private void onError(String line) {
        var errorMessage = "Failed to parse ndjson line from inference provider. Line does not begin with '{' and end with '}'";
        log.warn("{}: {}", errorMessage, line);
        onError(new IOException(errorMessage));
    }

    @Override
    public void onComplete() {
        if (previousTokens.isBlank()) {
            super.onComplete();
        } else {
            // if we never sent previousTokens, that means the line is invalid, and we were waiting for more bytes.
            // since onComplete is called, that means no more bytes are coming.
            onError(previousTokens);
        }
    }

    private boolean isValidJson(String line) {
        return line.charAt(0) == '{' && line.charAt(line.length() - 1) == '}';
    }
}
