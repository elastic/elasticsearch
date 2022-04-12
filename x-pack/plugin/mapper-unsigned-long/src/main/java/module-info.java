/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.mapper.unsigned_long {
    requires org.apache.lucene.core;
    requires org.elasticsearch.server;
    requires org.apache.lucene.sandbox;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.painless.spi;

    // TODO: Argh! this qualified export is dropped since painless in not in a parent layer
    exports org.elasticsearch.xpack.unsignedlong; // to org.elasticsearch.painless; // painless script engine

    opens org.elasticsearch.xpack.unsignedlong to org.elasticsearch.painless.spi; // whitelist resource access
}
