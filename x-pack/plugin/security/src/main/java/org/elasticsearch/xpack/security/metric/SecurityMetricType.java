/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

/**
 * Defines all security metric types that can be collected.
 */
public enum SecurityMetricType {

    AUTHC_API_KEY(
        SecurityMetricGroup.AUTHC,
        new SecurityMetricInfo("es.security.authc.api_key.success.total", "Number of successful API key authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.api_key.failures.total", "Number of failed API key authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.api_key.time", "Time it took (in nanoseconds) to execute API key authentication.", "ns")
    ),

    AUTHC_SERVICE_ACCOUNT(
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
            "Time it took (in nanoseconds) to execute service account authentication.",
            "ns"
        )
    ),

    AUTHC_OAUTH2_TOKEN(
        SecurityMetricGroup.AUTHC,
        new SecurityMetricInfo("es.security.authc.token.success.total", "Number of successful OAuth2 token authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.token.failures.total", "Number of failed OAuth2 token authentications.", "count"),
        new SecurityMetricInfo(
            "es.security.authc.token.time",
            "Time it took (in nanoseconds) to execute OAuth2 token authentication.",
            "ns"
        )
    ),

    AUTHC_REALMS(
        SecurityMetricGroup.AUTHC,
        new SecurityMetricInfo("es.security.authc.realms.success.total", "Number of successful realm authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.realms.failures.total", "Number of failed realm authentications.", "count"),
        new SecurityMetricInfo("es.security.authc.realms.time", "Time it took (in nanoseconds) to execute realm authentication.", "ns")
    ),

    ;

    private final SecurityMetricGroup group;
    private final SecurityMetricInfo successMetricInfo;
    private final SecurityMetricInfo failuresMetricInfo;
    private final SecurityMetricInfo timeMetricInfo;

    SecurityMetricType(
        SecurityMetricGroup group,
        SecurityMetricInfo successMetricInfo,
        SecurityMetricInfo failuresMetricInfo,
        SecurityMetricInfo timeMetricInfo
    ) {
        this.group = group;
        this.successMetricInfo = successMetricInfo;
        this.failuresMetricInfo = failuresMetricInfo;
        this.timeMetricInfo = timeMetricInfo;
    }

    public SecurityMetricGroup group() {
        return this.group;
    }

    public SecurityMetricInfo successMetricInfo() {
        return successMetricInfo;
    }

    public SecurityMetricInfo failuresMetricInfo() {
        return failuresMetricInfo;
    }

    public SecurityMetricInfo timeMetricInfo() {
        return timeMetricInfo;
    }

}
