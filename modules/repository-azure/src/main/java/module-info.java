/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.repository.azure {
    requires org.elasticsearch.base;
    requires org.elasticsearch.transport.netty4;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;

    requires org.apache.lucene.core;

    requires org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;

    requires com.azure.core;
    requires com.azure.http.netty;
    requires com.azure.storage.blob;
    requires com.azure.storage.common;
    requires com.azure.identity;

    requires io.netty.buffer;
    requires io.netty.transport;
    requires io.netty.resolver;
    requires io.netty.common;

    requires reactor.core;
    requires reactor.netty.core;
    requires reactor.netty.http;
}
