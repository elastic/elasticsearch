/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.support;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * During query parsing this keeps track of the current prefiltering level.
 */
public final class AutoPrefilteringScope implements AutoCloseable {

    private final Deque<List<QueryBuilder>> prefiltersStack = new LinkedList<>();

    public List<QueryBuilder> getPrefilters() {
        return prefiltersStack.stream().flatMap(List::stream).toList();
    }

    public void push(List<QueryBuilder> prefilters) {
        prefiltersStack.push(prefilters);
    }

    public void pop() {
        prefiltersStack.pop();
    }

    @Override
    public void close() {
        pop();
    }
}
