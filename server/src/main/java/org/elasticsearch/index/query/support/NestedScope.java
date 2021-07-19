/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.support;

import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;

import java.util.Deque;
import java.util.LinkedList;

/**
 * During query parsing this keeps track of the current nested level.
 */
public final class NestedScope {

    private final Deque<NestedObjectMapper> levelStack = new LinkedList<>();

    /**
     * @return For the current nested level returns the object mapper that belongs to that
     */
    public NestedObjectMapper getObjectMapper() {
        return levelStack.peek();
    }

    /**
     * Sets the new current nested level and pushes old current nested level down the stack returns that level.
     */
    public NestedObjectMapper nextLevel(NestedObjectMapper level) {
        NestedObjectMapper previous = levelStack.peek();
        levelStack.push(level);
        return previous;
    }

    /**
     * Sets the previous nested level as current nested level and removes and returns the current nested level.
     */
    public ObjectMapper previousLevel() {
        return levelStack.pop();
    }

}
