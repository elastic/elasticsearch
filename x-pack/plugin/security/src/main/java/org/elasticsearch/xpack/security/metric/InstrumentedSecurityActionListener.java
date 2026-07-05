/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;

public class InstrumentedSecurityActionListener {

    /**
     * Wraps the provided {@code listener} and returns a new wrapped listener which handles authentication metrics collection.
     *
     * @param metrics The metrics to collect.
     * @param context The context object is used to collect and attach additional metric attributes.
     * @param listener The authentication result handling listener.
     * @return a new "wrapped" listener which overrides onResponse and onFailure methods in order to collect authentication metrics.
     * @param <R> The type of authentication result value.
     * @param <C> The type of context object which is used to attach additional attributes to collected authentication metrics.
     */
    public static <R, C> ActionListener<AuthenticationResult<R>> wrapForAuthc(
        final SecurityMetrics<C> metrics,
        final C context,
        final ActionListener<AuthenticationResult<R>> listener
    ) {
        assert metrics.type().group() == SecurityMetricGroup.AUTHC;
        final long startTimeNano = metrics.relativeTimeInNanos();
        return ActionListener.runBefore(ActionListener.wrap(result -> {
            if (result.isAuthenticated()) {
                metrics.recordSuccess(context);
            } else {
                metrics.recordFailure(context);
            }
            listener.onResponse(result);
        }, e -> {
            metrics.recordFailure(context);
            listener.onFailure(e);
        }), () -> metrics.recordTime(context, startTimeNano));
    }

    /**
     * A simpler variant that re-uses the Authentication Result as the context. This can be handy in situations where the attributes that
     * are of interest are available only after the authentication is completed and not before.
     * As a natural consequence, there will be no context available at the point of recording start time and in cases of exceptional failure
     */
    public static <R> ActionListener<AuthenticationResult<R>> wrapForAuthc(
        final SecurityMetrics<AuthenticationResult<R>> metrics,
        final ActionListener<AuthenticationResult<R>> listener
    ) {
        assert metrics.type().group() == SecurityMetricGroup.AUTHC;
        final long startTimeNano = metrics.relativeTimeInNanos();
        return ActionListener.runBefore(ActionListener.wrap(result -> {
            if (result.isAuthenticated()) {
                metrics.recordSuccess(result);
            } else {
                metrics.recordFailure(result);
            }
            listener.onResponse(result);
        }, e -> {
            metrics.recordFailure(null);
            listener.onFailure(e);
        }), () -> metrics.recordTime(null, startTimeNano));
    }

}
