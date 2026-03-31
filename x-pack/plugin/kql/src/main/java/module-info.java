/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.kql {
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.antlr.antlr4.runtime;
    requires org.elasticsearch.base;
    requires org.apache.lucene.queryparser;
    requires org.elasticsearch.logging;
    requires org.apache.lucene.core;
    requires org.apache.lucene.join;

    exports org.elasticsearch.xpack.kql;
    exports org.elasticsearch.xpack.kql.parser;
    exports org.elasticsearch.xpack.kql.query;
}
