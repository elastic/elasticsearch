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

import org.elasticsearch.cluster.EstimatedHeapUsageCollector;
import org.elasticsearch.cluster.metadata.TemplateDecoratorProvider;
import org.elasticsearch.repositories.SnapshotShardContextFactory;
import org.elasticsearch.xpack.stateless.memory.StatelessHeapUsageCollector;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotShardContextFactory;
import org.elasticsearch.xpack.stateless.templates.StatelessTemplateSettingsDecoratorProvider;

module org.elasticsearch.xpack.stateless {
    requires org.elasticsearch.base;
    requires org.elasticsearch.blobcache;
    requires org.elasticsearch.nativeaccess;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;

    requires org.apache.logging.log4j;

    requires org.apache.lucene.core;
    requires org.apache.log4j;

    // TODO: remove unnecessary "to" clauses ES-13786
    exports org.elasticsearch.xpack.stateless
        to
            org.elasticsearch.server,
            org.elasticsearch.serverless.autoscaling,
            org.elasticsearch.serverless.stateless,
            org.elasticsearch.metering;
    exports org.elasticsearch.xpack.stateless.action to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.xpack to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.recovery to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.commits to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.objectstore to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.cache to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.lucene to org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.memory to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.reshard to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.utils to org.elasticsearch.server, org.elasticsearch.serverless.stateless;

    exports org.elasticsearch.xpack.stateless.cluster.coordination to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.engine to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.recovery.shardinfo to org.elasticsearch.server; // For PrimaryTermAndGeneration
    exports org.elasticsearch.xpack.stateless.snapshots to org.elasticsearch.server; // for stateless snapshots
    exports org.elasticsearch.xpack.stateless.templates to org.elasticsearch.server;

    provides org.elasticsearch.action.search.OnlinePrewarmingServiceProvider
        with
            org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingServiceProvider;
    provides SnapshotShardContextFactory with StatelessSnapshotShardContextFactory;
    provides TemplateDecoratorProvider with StatelessTemplateSettingsDecoratorProvider;
    provides EstimatedHeapUsageCollector with StatelessHeapUsageCollector;
}
