/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.mapper.extras {
    requires org.elasticsearch.server;
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcontent;
    requires org.apache.lucene.analysis.common;
    requires org.apache.lucene.core;
    requires org.apache.lucene.memory;
    requires org.apache.lucene.queries;
}
