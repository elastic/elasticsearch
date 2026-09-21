/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.support;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;

import java.util.ArrayDeque;
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

    /**
     * Temporarily makes {@code level} the current nested level.
     *
     * @param level an ancestor of the current level, or {@code null} for the root document space
     * @return a {@link Releasable} that restores the levels that were unwound
     */
    public Releasable unwindTo(@Nullable NestedObjectMapper level) {
        if (levelStack.peek() == level) {
            return () -> {};
        }

        Deque<NestedObjectMapper> unwound = new ArrayDeque<>();
        while (levelStack.isEmpty() == false && levelStack.peek() != level) {
            unwound.push(levelStack.pop());
        }
        assert levelStack.peek() == level : "[" + level + "] is not an ancestor of the current nested level";
        return () -> {
            while (unwound.isEmpty() == false) {
                levelStack.push(unwound.pop());
            }
        };
    }

}
