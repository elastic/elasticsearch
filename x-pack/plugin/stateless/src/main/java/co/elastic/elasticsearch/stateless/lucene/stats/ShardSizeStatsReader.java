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

package co.elastic.elasticsearch.stateless.lucene.stats;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

public class ShardSizeStatsReader {

    private static final Logger logger = LogManager.getLogger(ShardSizeStatsReader.class);

    private final IndicesService indicesService;
    private final LongSupplier currentTimeMillsSupplier;

    private long interactiveDataAge = TimeValue.timeValueDays(7).millis();// TODO ES-6224 make it a cluster setting

    public ShardSizeStatsReader(ClusterSettings clusterSettings, LongSupplier currentTimeMillsSupplier, IndicesService indicesService) {
        // TODO ES-6224 listen for a changes to interactive data age via clusterSettings
        // Make sure all sizes are pushed to elected master when this value changes
        this.indicesService = indicesService;
        this.currentTimeMillsSupplier = currentTimeMillsSupplier;
    }

    public ShardSizeStatsReader(ClusterService clusterService, ThreadPool threadPool, IndicesService indicesService) {
        this(clusterService.getClusterSettings(), threadPool::absoluteTimeInMillis, indicesService);
    }

    public Map<ShardId, ShardSize> getAllShardSizes() {
        var sizes = new HashMap<ShardId, ShardSize>();
        for (var indexService : indicesService) {
            for (var indexShard : indexService) {
                var shardSize = getShardSize(indexShard);
                if (shardSize != null) {
                    sizes.put(indexShard.shardId(), shardSize);
                }
            }
        }
        return sizes;
    }

    /**
     * @return the ShardSize of the shard or {@code null} if operation could not be performed
     */
    @Nullable
    public ShardSize getShardSize(ShardId shardId) {
        return getShardSize(indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id()));
    }

    @Nullable
    public ShardSize getShardSize(IndexShard indexShard) {
        if (isSizeAvailable(indexShard.state()) == false) {
            return null;
        }

        try {
            try (var searcher = indexShard.acquireSearcher("shard_stats")) {
                var interactiveSize = 0L;
                var nonInteractiveSize = 0L;

                final long currentTimeMillis = currentTimeMillsSupplier.getAsLong();
                final long currentInteractiveDataAge = interactiveDataAge;
                for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                    var segmentSize = Lucene.segmentReader(ctx.reader()).getSegmentInfo().sizeInBytes();
                    if (isInteractive(ctx.reader(), currentTimeMillis, currentInteractiveDataAge)) {
                        interactiveSize += segmentSize;
                    } else {
                        nonInteractiveSize += segmentSize;
                    }
                }
                return new ShardSize(interactiveSize, nonInteractiveSize);
            } catch (IOException e) {
                logger.warn("Failed to read shard size stats for {}", indexShard.shardId(), e);
                return null;
            }
        } catch (Exception e) {
            logger.warn("Failed to acquire searcher for {}", indexShard.shardId(), e);
            return null;
        }
    }

    private static boolean isInteractive(LeafReader segmentReader, long currentTimeMillis, long interactiveDataAge) throws IOException {
        PointValues values = segmentReader.getPointValues(DataStream.TIMESTAMP_FIELD_NAME);
        if (values == null || values.getMaxPackedValue() == null) {
            // no timestamp field, entire segment is considered interactive
            return true;
        }
        var maxTimestamp = LongPoint.decodeDimension(values.getMaxPackedValue(), 0);
        return currentTimeMillis - maxTimestamp <= interactiveDataAge;
    }

    private static boolean isSizeAvailable(IndexShardState state) {
        return state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY;
    }
}
