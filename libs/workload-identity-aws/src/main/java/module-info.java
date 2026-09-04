/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/** Async, non-blocking AWS workload-identity credential providers. */
module org.elasticsearch.workload.identity.aws {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.server;
    requires software.amazon.awssdk.auth;
    requires software.amazon.awssdk.awscore;
    requires software.amazon.awssdk.identity.spi;
    requires software.amazon.awssdk.core;
    requires software.amazon.awssdk.services.sts;
    requires software.amazon.awssdk.utils;

    exports org.elasticsearch.workload.identity.aws;
}
