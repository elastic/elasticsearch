/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.xpack.stateless {
    requires org.elasticsearch.base;
    requires org.elasticsearch.blobcache;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    // TODO: remove "to" clauses ES-13786
    exports org.elasticsearch.xpack.stateless to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.action to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.cache to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.commits to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.engine to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.lucene to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.utils to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
}
