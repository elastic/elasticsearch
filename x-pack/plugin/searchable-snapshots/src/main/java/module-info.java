/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.searchablesnapshots {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;

    requires org.elasticsearch.blobcache;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires org.apache.lucene.analysis.common;

    exports org.elasticsearch.xpack.searchablesnapshots.action.cache to org.elasticsearch.server;
    exports org.elasticsearch.xpack.searchablesnapshots.action to org.elasticsearch.server;
}
