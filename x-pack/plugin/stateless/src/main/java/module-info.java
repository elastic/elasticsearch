/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.elasticsearch.action.search.OnlinePrewarmingServiceProvider;
import org.elasticsearch.cluster.EstimatedHeapUsageCollector;
import org.elasticsearch.cluster.metadata.TemplateDecoratorProvider;
import org.elasticsearch.repositories.SnapshotShardContextFactory;
import org.elasticsearch.xpack.stateless.memory.StatelessHeapUsageCollector;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotShardContextFactory;
import org.elasticsearch.xpack.stateless.templates.StatelessTemplateSettingsDecoratorProvider;

module org.elasticsearch.xpack.stateless {
    requires org.elasticsearch.base;
    requires org.elasticsearch.blobcache;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires hppc;

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

    provides OnlinePrewarmingServiceProvider with org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingServiceProvider;
    provides SnapshotShardContextFactory with StatelessSnapshotShardContextFactory;
    provides TemplateDecoratorProvider with StatelessTemplateSettingsDecoratorProvider;
    provides EstimatedHeapUsageCollector with StatelessHeapUsageCollector;
}
