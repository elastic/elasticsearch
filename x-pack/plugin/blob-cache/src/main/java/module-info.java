/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.blobcache {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.preallocate;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    exports org.elasticsearch.blobcache;
    exports org.elasticsearch.blobcache.common;
    exports org.elasticsearch.blobcache.shared;
}
