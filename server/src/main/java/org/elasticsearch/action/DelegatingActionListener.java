/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import static org.elasticsearch.action.ActionListenerImplementations.safeOnFailure;

/**
 * A base class for creating wrappers around {@link ActionListener}s that delegate certain operations
 * to the wrapped listener. By default, this class delegates failure handling to the delegate listener's
 * {@link ActionListener#onFailure} method.
 *
 * <p>This is a useful base class for creating ActionListener wrappers that need to override
 * {@link #onResponse} handling with custom logic, while retaining the delegate's failure handling.
 * It can also be useful to override other methods to perform additional work with access to the
 * delegate listener.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create a custom delegating listener that transforms responses
 * public class TransformingListener<T, R> extends DelegatingActionListener<T, R> {
 *     private final Function<T, R> transformer;
 *
 *     public TransformingListener(ActionListener<R> delegate, Function<T, R> transformer) {
 *         super(delegate);
 *         this.transformer = transformer;
 *     }
 *
 *     @Override
 *     public void onResponse(T response) {
 *         try {
 *             R transformed = transformer.apply(response);
 *             delegate.onResponse(transformed);
 *         } catch (Exception e) {
 *             delegate.onFailure(e);
 *         }
 *     }
 * }
 *
 * // Use the custom listener
 * ActionListener<String> stringListener = ActionListener.wrap(
 *     result -> System.out.println("Result: " + result),
 *     e -> System.err.println("Error: " + e)
 * );
 * ActionListener<Integer> intListener = new TransformingListener<>(
 *     stringListener,
 *     num -> "Number: " + num
 * );
 * }</pre>
 *
 * @param <Response> the type of response this listener handles
 * @param <DelegateResponse> the type of response the delegate listener handles
 */
public abstract class DelegatingActionListener<Response, DelegateResponse> implements ActionListener<Response> {

    /**
     * The delegate listener to which operations are forwarded.
     */
    protected final ActionListener<DelegateResponse> delegate;

    /**
     * Constructs a new delegating action listener that wraps the given delegate listener.
     *
     * @param delegate the listener to delegate operations to
     */
    protected DelegatingActionListener(ActionListener<DelegateResponse> delegate) {
        this.delegate = delegate;
    }

    /**
     * Delegates failure handling to the delegate listener. This method safely invokes the
     * delegate's {@link ActionListener#onFailure} method, catching any exceptions it might throw.
     *
     * @param e the exception that occurred
     */
    @Override
    public void onFailure(Exception e) {
        safeOnFailure(delegate, e);
    }

    /**
     * Returns a string representation of this delegating listener, including the class name
     * and the delegate's string representation.
     *
     * @return a descriptive string for this listener
     */
    @Override
    public String toString() {
        return getClass().getName() + "/" + delegate;
    }
}
