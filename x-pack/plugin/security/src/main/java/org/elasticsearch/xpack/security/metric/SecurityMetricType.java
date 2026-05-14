/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

/**
 * Defines a security metric type that can be collected.
 * Each metric type has a group, and metric info for tracking success, failure and total time.
 */
public record SecurityMetricType(
    SecurityMetricGroup group,
    SecurityMetricInfo successMetricInfo,
    SecurityMetricInfo failuresMetricInfo,
    SecurityMetricInfo timeMetricInfo
) {

    public static final SecurityMetricType AUTHC_API_KEY = new SecurityMetricType(
        SecurityMetricGroup.AUTHC,
        new SecurityMetricInfo("es.security.authc.api_key.success.total", "Number of successful API key authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.api_key.failures.total", "Number of failed API key authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.api_key.time", "Time it took (in milliseconds) to execute API key authentication.", "ms")
    );

    public static final SecurityMetricType AUTHC_SERVICE_ACCOUNT = new SecurityMetricType(
        SecurityMetricGroup.AUTHC,
        new SecurityMetricInfo(
            "es.security.authc.service_account.success.total",
            "Number of successful service account authentications.",
            "count"
        ),
        new SecurityMetricInfo(
            "es.security.authc.service_account.failures.total",
            "Number of failed service account authentications.",
            "count"
        ),
        new SecurityMetricInfo(
            "es.security.authc.service_account.time",
            "Time it took (in milliseconds) to execute service account authentication.",
            "ms"
        )
    );

    public static final SecurityMetricType AUTHC_OAUTH2_TOKEN = new SecurityMetricType(
        SecurityMetricGroup.AUTHC,
        new SecurityMetricInfo("es.security.authc.token.success.total", "Number of successful OAuth2 token authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.token.failures.total", "Number of failed OAuth2 token authentications.", "count"),
        new SecurityMetricInfo(
            "es.security.authc.token.time",
            "Time it took (in milliseconds) to execute OAuth2 token authentication.",
            "ms"
        )
    );

    public static final SecurityMetricType AUTHC_REALMS = new SecurityMetricType(
        SecurityMetricGroup.AUTHC,
        new SecurityMetricInfo("es.security.authc.realms.success.total", "Number of successful realm authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.realms.failures.total", "Number of failed realm authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.realms.time", "Time it took (in milliseconds) to execute realm authentication.", "ms")
    );
}
