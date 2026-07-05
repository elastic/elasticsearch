/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixture;

import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Snapshot-based capture over a fixture's request log. Create before an action, inspect after.
 * Path-based filtering isolates concurrent test traffic naturally.
 */
public final class RequestCapture {
    private final List<RequestEntry> log;
    private final int startIndex;
    @Nullable
    private final String pathContains;

    private RequestCapture(List<RequestEntry> log, int startIndex, @Nullable String pathContains) {
        this.log = log;
        this.startIndex = startIndex;
        this.pathContains = pathContains;
    }

    public static RequestCapture start(List<RequestEntry> log, @Nullable String pathContains) {
        return new RequestCapture(log, log.size(), pathContains);
    }

    public static RequestCapture start(List<RequestEntry> log) {
        return start(log, null);
    }

    public List<RequestEntry> captured() {
        int end = log.size();
        if (end <= startIndex) {
            return List.of();
        }
        return log.subList(startIndex, end).stream().filter(e -> pathContains == null || e.path().contains(pathContains)).toList();
    }

    public long count(String method) {
        return captured().stream().filter(e -> e.method().equals(method)).count();
    }

    public long countHeads() {
        return count("HEAD");
    }

    public long countGets() {
        return count("GET");
    }

    public long countSuffixRangeGets() {
        return captured().stream().filter(e -> "GET".equals(e.method()) && e.range() != null && e.range().startsWith("bytes=-")).count();
    }

    public String dump() {
        return captured().stream()
            .map(e -> e.method() + " " + e.path() + (e.range() != null ? " [" + e.range() + "]" : ""))
            .collect(Collectors.joining("\n"));
    }
}
