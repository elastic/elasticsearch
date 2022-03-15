/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.plugin.transport.nio {
    requires org.apache.logging.log4j;
    requires org.elasticsearch.server;
    requires org.elasticsearch.core;
    requires org.elasticsearch.nio;
    requires org.elasticsearch.xcontent;
    requires org.apache.lucene.core;
    requires io.netty.transport;
    requires io.netty.codec;
    requires io.netty.codec.http;
    requires io.netty.common;
    requires io.netty.buffer;

    exports org.elasticsearch.http.nio;
    exports org.elasticsearch.transport.nio;
}
