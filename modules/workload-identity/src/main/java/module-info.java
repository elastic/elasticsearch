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

    requires org.apache.httpcomponents.httpclient;
    requires org.apache.httpcomponents.httpcore;
    requires org.apache.httpcomponents.httpasyncclient;
    requires org.apache.httpcomponents.httpcore.nio;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    // Only the central abstraction is exported; HTTP transport classes remain internal.
    exports org.elasticsearch.workloadidentity;
}
