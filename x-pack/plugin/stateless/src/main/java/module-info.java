/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.stateless {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;

    requires org.elasticsearch.blobcache;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.xcontent;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    exports co.elastic.elasticsearch.stateless.action to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.xpack to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.autoscaling.action to org.elasticsearch.server;
}
