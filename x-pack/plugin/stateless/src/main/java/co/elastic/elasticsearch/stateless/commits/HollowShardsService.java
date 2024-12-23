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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Functionality around the hollowing of inactive shards to reduce their memory footprint.
 */
public class HollowShardsService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(HollowShardsService.class);

    /**
     * Enables the super thin indexing shards feature in order to hollow inactive indexing shards and decrease their memory footprint.
     */
    public static final Setting<Boolean> STATELESS_HOLLOW_INDEX_SHARDS_ENABLED = Setting.boolSetting(
        "stateless.hollow_index_shards.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * How long a data stream non-write index should not have received ingestion for, before being considered for hollowing.
     */
    public static final Setting<TimeValue> SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL = Setting.positiveTimeSetting(
        "stateless.hollow_index_shards.ingestion.ds_non_write_ttl",
        TimeValue.timeValueMinutes(15),
        Setting.Property.NodeScope
    );

    /**
     * How long a regular index, or the write index of a data stream, should not have received ingestion for, before being
     * considered for hollowing.
     */
    public static final Setting<TimeValue> SETTING_HOLLOW_INGESTION_TTL = Setting.positiveTimeSetting(
        "stateless.hollow_index_shards.ingestion.ttl",
        TimeValue.timeValueDays(3),
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final LongSupplier relativeTimeSupplierInMillis;
    private final boolean featureEnabled;
    private final TimeValue ingestionDataStreamNonWriteTtl;
    private final TimeValue ingestionTtl;

    public HollowShardsService(Settings settings, ClusterService clusterService) {
        this(settings, clusterService, clusterService.threadPool()::relativeTimeInMillis);
    }

    public HollowShardsService(Settings settings, ClusterService clusterService, LongSupplier relativeTimeSupplierInMillis) {
        this.clusterService = clusterService;
        this.relativeTimeSupplierInMillis = relativeTimeSupplierInMillis;
        assert DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE) : "HollowShardsService should exist only on index nodes";
        this.featureEnabled = HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.get(settings);
        this.ingestionDataStreamNonWriteTtl = HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.get(settings);
        this.ingestionTtl = HollowShardsService.SETTING_HOLLOW_INGESTION_TTL.get(settings);
        if (featureEnabled) {
            logger.info("Hollow index shards enabled with TTL {} and DS non-write TTL", ingestionTtl, ingestionDataStreamNonWriteTtl);
        } else {
            logger.debug(() -> "Hollow index shards disabled");
        }
    }

    public boolean isFeatureEnabled() {
        return featureEnabled;
    }

    public boolean isHollowableIndexShard(IndexShard indexShard) {
        // TODO: for ES-10258 we may need a variation of this function that does not check the primary permits (which may be held already)
        if (featureEnabled && indexShard.isSystem() == false && indexShard.getActiveOperationsCount() == 0) {
            final var engine = indexShard.getEngineOrNull();
            if (engine instanceof IndexEngine indexEngine) {
                final var index = indexShard.shardId().getIndex();
                final var indexAbstraction = clusterService.state().metadata().getIndicesLookup().get(index.getName());
                if (indexAbstraction != null) {
                    final var dataStream = indexAbstraction.getParentDataStream();
                    final boolean dsNonWrite = dataStream != null
                        && Objects.equals(dataStream.getWriteIndex(), index) == false
                        && Objects.equals(dataStream.getFailureStoreWriteIndex(), index) == false;
                    final TimeValue ttl = dsNonWrite ? ingestionDataStreamNonWriteTtl : ingestionTtl;
                    return engineHasNoIngestion(indexEngine, ttl);
                }
            }
        }
        return false;
    }

    protected boolean engineHasNoIngestion(Engine engine, TimeValue ttl) {
        final long lastWriteMillis = TimeValue.nsecToMSec(engine.getLastWriteNanos());
        final long now = relativeTimeSupplierInMillis.getAsLong();
        return now - lastWriteMillis > ttl.millis();
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}
}
