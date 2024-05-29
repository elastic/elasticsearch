/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.painless {
    requires org.elasticsearch.base;
    requires org.elasticsearch.geo;
    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;

    requires org.antlr.antlr4.runtime;
    requires org.apache.lucene.core;
    requires org.objectweb.asm;
    requires org.objectweb.asm.commons;
    requires org.objectweb.asm.util;

    exports org.elasticsearch.painless;
    exports org.elasticsearch.painless.api;
    exports org.elasticsearch.painless.action;

    opens org.elasticsearch.painless to org.elasticsearch.painless.spi;  // whitelist access
    opens org.elasticsearch.painless.action to org.elasticsearch.server; // guice
}
