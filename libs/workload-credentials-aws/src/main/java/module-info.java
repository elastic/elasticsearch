/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.workload.credentials.aws {
    requires org.elasticsearch.workload.identity;
    requires software.amazon.awssdk.auth;
    requires software.amazon.awssdk.identity.spi;
    requires software.amazon.awssdk.utils;
    requires software.amazon.awssdk.annotations;

    exports org.elasticsearch.workload.credentials.aws;
}
