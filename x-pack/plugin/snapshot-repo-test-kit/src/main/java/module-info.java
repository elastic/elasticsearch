/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.repositories.blobstore.testkit {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires org.elasticsearch.logging;

    exports org.elasticsearch.repositories.blobstore.testkit.analyze;
    exports org.elasticsearch.repositories.blobstore.testkit.integrity;
}
