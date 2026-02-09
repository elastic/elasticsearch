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
import org.elasticsearch.index.mapper.RootObjectMapperNamespaceValidator;
import org.elasticsearch.repositories.SnapshotShardContextFactory;
import org.elasticsearch.snapshots.IndexMetadataRestoreTransformer;
import org.elasticsearch.xpack.stateless.allocation.StatelessHeapUsageCollector;
import org.elasticsearch.xpack.stateless.mapper.ServerlessRootObjectMapperNamespaceValidator;
import org.elasticsearch.xpack.stateless.recovery.StatelessRestoreTransformer;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotShardContextFactory;

module org.elasticsearch.xpack.stateless {
    requires org.elasticsearch.base;
    requires org.elasticsearch.blobcache;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;

    requires org.elasticsearch.serverless.constants;
    requires org.elasticsearch.serverless.stateless.api;

    requires org.apache.logging.log4j;

    requires org.apache.lucene.core;
    requires org.apache.log4j;

    // TODO: remove unnecessary "to" clauses ES-13786
    exports org.elasticsearch.xpack.stateless to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.action to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.mapper to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.xpack to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.recovery to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.commits to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.objectstore to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.cache to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.cache.action to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.metering.action to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.reshard to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.allocation to org.elasticsearch.server; // For StatelessHeapUsageCollector

    exports org.elasticsearch.xpack.stateless.autoscaling;
    exports org.elasticsearch.xpack.stateless.autoscaling.indexing;
    exports org.elasticsearch.xpack.stateless.autoscaling.search;
    exports org.elasticsearch.xpack.stateless.autoscaling.memory;
    exports org.elasticsearch.xpack.stateless.lucene.stats to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.cluster.coordination to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.autoscaling.search.load;
    exports org.elasticsearch.xpack.stateless.engine to org.elasticsearch.server, org.elasticsearch.serverless.stateless;
    exports org.elasticsearch.xpack.stateless.multiproject to org.elasticsearch.server, org.elasticsearch.serverless.stateless; // For
                                                                                                                                // PrimaryTermAndGeneration
    exports org.elasticsearch.xpack.stateless.recovery.shardinfo to org.elasticsearch.server; // For PrimaryTermAndGeneration
    exports org.elasticsearch.xpack.stateless.snapshots to org.elasticsearch.server; // for stateless snapshots

    provides org.elasticsearch.action.search.OnlinePrewarmingServiceProvider
        with
            org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingServiceProvider;
    provides EstimatedHeapUsageCollector with StatelessHeapUsageCollector;
    provides IndexMetadataRestoreTransformer with StatelessRestoreTransformer;
    provides RootObjectMapperNamespaceValidator with ServerlessRootObjectMapperNamespaceValidator;
    provides SnapshotShardContextFactory with StatelessSnapshotShardContextFactory;
}
