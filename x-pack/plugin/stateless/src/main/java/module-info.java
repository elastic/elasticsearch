/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

module org.elasticsearch.stateless {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;

    requires org.elasticsearch.blobcache;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.xcontent;

    requires org.elasticsearch.serverless.constants;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    exports co.elastic.elasticsearch.stateless to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.action to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.xpack to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.recovery to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.commits to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.objectstore to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.cache.action to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.metering.action to org.elasticsearch.server;

    exports co.elastic.elasticsearch.stateless.autoscaling;
    exports co.elastic.elasticsearch.stateless.autoscaling.indexing;
    exports co.elastic.elasticsearch.stateless.autoscaling.search;
    exports co.elastic.elasticsearch.stateless.autoscaling.memory;
    exports co.elastic.elasticsearch.stateless.lucene.stats to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.cluster.coordination to org.elasticsearch.server;
    exports co.elastic.elasticsearch.stateless.engine to org.elasticsearch.server; // For PrimaryTermAndGeneration
}
