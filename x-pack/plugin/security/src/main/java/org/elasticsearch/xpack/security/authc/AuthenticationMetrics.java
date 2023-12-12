/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Provides a common way of registering and collecting various authentication metrics.
 *
 * @param <C> The type of context object which is used to attach additional attributes to collected metrics.
 */
final class AuthenticationMetrics<C> {

    public static final String METRIC_SUCCESS_COUNT = "es.security.authc.%s.success.count";
    public static final String METRIC_FAILURES_COUNT = "es.security.authc.%s.failures.count";
    public static final String METRIC_AUTHENTICATION_TIME = "es.security.authc.%s.time";

    public static final String ATTRIBUTE_NAMESPACE_PREFIX = "es.security.";
    public static final String ATTRIBUTE_AUTHC_FAILURE_REASON = ATTRIBUTE_NAMESPACE_PREFIX + "%s_authc_failure_reason";

    private final LongCounter successCounter;
    private final LongCounter failuresCounter;
    private final LongHistogram timeHistogram;

    private final Function<C, Map<String, Object>> attributesBuilder;
    private final String failureReasonAttributeName;
    private final LongSupplier nanoTimeSupplier;

    /**
     * Constructor.
     *
     * @param meterRegistry The registry used to register authentication metrics.
     * @param authenticatorNamespace The namespace is used to construct unique metric names per authenticator.
     * @param attributesBuilder Used to build additional metric attributes from the given context object.
     * @param nanoTimeSupplier The relative time supplier in nano seconds.
     */
    AuthenticationMetrics(
        final MeterRegistry meterRegistry,
        final String authenticatorNamespace,
        final Function<C, Map<String, Object>> attributesBuilder,
        final LongSupplier nanoTimeSupplier
    ) {
        this.successCounter = meterRegistry.registerLongCounter(
            Strings.format(METRIC_SUCCESS_COUNT, authenticatorNamespace),
            Strings.format("Number of successful %s authentications.", authenticatorNamespace),
            "count"
        );
        this.failuresCounter = meterRegistry.registerLongCounter(
            Strings.format(METRIC_FAILURES_COUNT, authenticatorNamespace),
            Strings.format("Number of failed %s authentications.", authenticatorNamespace),
            "count"
        );
        this.timeHistogram = meterRegistry.registerLongHistogram(
            Strings.format(METRIC_AUTHENTICATION_TIME, authenticatorNamespace),
            Strings.format("Time it took (in nanoseconds) to execute %s authentication.", authenticatorNamespace),
            "ns"
        );
        this.failureReasonAttributeName = Strings.format(ATTRIBUTE_AUTHC_FAILURE_REASON, authenticatorNamespace);
        this.attributesBuilder = attributesBuilder;
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    /**
     * Wraps the provided {@code listener} and returns a new wrapped listener which handles authentication metrics collection.
     *
     * @param context The context object is used to collect and attach additional metric attributes.
     * @param listener The authentication result handling listener.
     * @return a new "wrapped" listener which overrides onResponse and onFailure methods in order to collect authentication metrics.
     * @param <R> The type of authentication result value.
     */
    public <R> ActionListener<AuthenticationResult<R>> wrap(final C context, final ActionListener<AuthenticationResult<R>> listener) {
        final long startTimeNano = nanoTimeSupplier.getAsLong();

        return ActionListener.runBefore(ActionListener.wrap(result -> {
            if (result.isAuthenticated()) {
                recordSuccessfulAuthentication(context);
            } else {
                recordFailedAuthentication(context, result.getMessage());
            }
            listener.onResponse(result);
        }, e -> {
            recordFailedAuthentication(context, e.getMessage());
            listener.onFailure(e);
        }), () -> recordAuthenticationTime(context, startTimeNano));
    }

    private void recordSuccessfulAuthentication(final C context) {
        this.successCounter.incrementBy(1L, attributesBuilder.apply(context));
    }

    private void recordFailedAuthentication(final C context, final String failureReason) {
        final Map<String, Object> attributes = new HashMap<>(attributesBuilder.apply(context));
        if (failureReason != null) {
            attributes.put(failureReasonAttributeName, failureReason);
        }
        this.failuresCounter.incrementBy(1L, attributes);
    }

    private void recordAuthenticationTime(final C context, final long startTimeNano) {
        final long timeInNanos = TimeValue.timeValueNanos(nanoTimeSupplier.getAsLong() - startTimeNano).getNanos();
        this.timeHistogram.record(timeInNanos, this.attributesBuilder.apply(context));
    }

}
