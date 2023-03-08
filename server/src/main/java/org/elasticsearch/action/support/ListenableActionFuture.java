/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

/**
 * An {@link ActionListener} which allows for the result to fan out to a (dynamic) collection of other listeners, added using {@link
 * #addListener}. Listeners added before completion are retained until completion; listeners added after completion are completed
 * immediately.
 *
 * Similar to {@link ListenableFuture}, with the following differences:
 *
 * <ul>
 * <li>This listener will silently ignore additional completions, whereas {@link ListenableFuture} must not be completed more than once.
 * <li>This listener completes the retained listeners on directly the completing thread, so you must use {@link ThreadedActionListener} if
 * dispatching is needed. In contrast, {@link ListenableFuture} allows to dispatch only the retained listeners, while immediately-completed
 * listeners are completed on the subscribing thread.
 * <li>This listener completes the retained listeners in the context of the completing thread, so you must remember to use {@link
 * ContextPreservingActionListener} to capture the thread context yourself if needed. In contrast, {@link ListenableFuture} allows for the
 * thread context to be captured at subscription time.
 * </ul>
 */
// TODO replace with superclass
@Deprecated(forRemoval = true)
public class ListenableActionFuture<T> extends FanOutListener<T> {}
