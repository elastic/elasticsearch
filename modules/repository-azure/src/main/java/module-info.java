/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.repository.azure {
    requires org.elasticsearch.base;
    requires org.elasticsearch.transport.netty4;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;

    requires org.apache.lucene.core;

    requires org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;

    requires com.azure.http.netty;
    requires com.azure.identity;

    requires io.netty.buffer;
    requires io.netty.transport;
    requires io.netty.resolver;
    requires io.netty.common;

    requires reactor.netty.core;
    requires reactor.netty.http;
    requires com.azure.storage.blob.batch;
}
