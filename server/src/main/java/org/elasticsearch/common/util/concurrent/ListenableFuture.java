/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.FanOutListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;

/**
 * An {@link ActionListener} which allows for the result to fan out to a (dynamic) collection of other listeners, added using {@link
 * #addListener}. Listeners added before completion are retained until completion; listeners added after completion are completed
 * immediately.
 *
 * Similar to {@link ListenableActionFuture}, with the following differences:
 *
 * <ul>
 * <li>This listener must not be completed more than once, whereas {@link ListenableActionFuture} will silently ignore additional
 * completions.
 * <li>This listener optionally allows for the completions of any retained listeners to be dispatched to an executor rather than handled
 * directly by the completing thread, whilst still allowing listeners to be completed immediately on the subscribing thread. In contrast,
 * when using {@link ListenableActionFuture} you must use {@link ThreadedActionListener} if dispatching is needed, and this will always
 * dispatch.
 * <li>This listener optionally allows for the retained listeners to be completed in the thread context of the subscribing thread, captured
 * at subscription time. This is often useful when subscribing listeners from several different contexts. In contrast, when using {@link
 * ListenableActionFuture} you must remember to use {@link ContextPreservingActionListener} to capture the thread context yourself.
 * </ul>
 */
public final class ListenableFuture<V> extends FanOutListener<V> {
    public V result() {
        try {
            return super.rawResult();
        } catch (Exception e) {
            throw wrapAsExecutionException(e);
        }
    }
}
