/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.workloadidentity {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.sslconfig;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.workloadidentity.spi;

    requires org.apache.httpcomponents.httpclient;
    requires org.apache.httpcomponents.httpcore;
    requires org.apache.httpcomponents.httpasyncclient;
    requires org.apache.httpcomponents.httpcore.nio;

    // HttpsWorkloadIdentityIssuerClient declares its logger as org.apache.logging.log4j.Logger
    // (rather than the org.elasticsearch.logging facade used elsewhere in this module) so it
    // can pass that logger directly to its RetryableAction-based token-request retrier, which
    // accepts only the log4j API. Matches the convention at every other RetryableAction host
    // class in the repo.
    requires org.apache.logging.log4j;

    // The public extension surface (WorkloadIdentityIssuerClient and WorkloadIdentityRegistry)
    // lives in the org.elasticsearch.workloadidentity.spi module, bundled under the plugin's
    // spi/ directory; the impl-side packages here are intentionally not exported.
}
