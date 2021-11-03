/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.action.ActionListener;

import static org.mockito.ArgumentMatchers.any;

/**
 * Test utilities for working with {@link ActionListener}s.
 */
public abstract class ActionListenerUtils {

    /**
     * Returns a Mockito matcher for any argument that is an {@link ActionListener}.
     * @param <T> the action listener type that the caller expects. Do not specify this, it will be inferred
     * @return an action listener matcher
     */
    @SuppressWarnings("unchecked")
    public static <T> ActionListener<T> anyActionListener() {
        return any(ActionListener.class);
    }
}
