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

import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
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

public class ShardSizeStatsReaderImpl implements ShardSizeStatsReader {

    private static final Logger logger = LogManager.getLogger(ShardSizeStatsReader.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final LongSupplier currentTimeMillisSupplier;

    public ShardSizeStatsReaderImpl(ClusterService clusterService, IndicesService indicesService, LongSupplier currentTimeMillisSupplier) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
    }

    public ShardSizeStatsReaderImpl(ClusterService clusterService, IndicesService indicesService) {
        this(clusterService, indicesService, () -> clusterService.threadPool().absoluteTimeInMillis());
    }

    public Map<ShardId, ShardSize> getAllShardSizes(TimeValue boostWindowInterval) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        var sizes = new HashMap<ShardId, ShardSize>();
        for (var indexService : indicesService) {
            for (var indexShard : indexService) {
                var shardSize = getShardSize(indexShard, boostWindowInterval);
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
    public ShardSize getShardSize(ShardId shardId, TimeValue boostWindowInterval) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        var indexService = indicesService.indexService(shardId.getIndex());
        if (indexService != null) {
            var indexShard = indexService.getShardOrNull(shardId.id());
            if (indexShard != null) {
                return getShardSize(indexShard, boostWindowInterval);
            }
        }
        return null;
    }

    @Nullable
    public ShardSize getShardSize(IndexShard indexShard, TimeValue boostWindowInterval) {
        if (isSizeAvailable(indexShard.state()) == false) {
            return null;
        }

        try {
            try (var searcher = indexShard.acquireSearcher("shard_stats")) {
                var interactiveSize = 0L;
                var nonInteractiveSize = 0L;

                var indexMetadata = clusterService.state().metadata().getProject().index(indexShard.shardId().getIndex());
                if (indexMetadata == null) {
                    return null;
                }

                final MappedFieldType fieldType;
                if (indexShard.mapperService() != null) {
                    fieldType = indexShard.mapperService().fieldType(DataStream.TIMESTAMP_FIELD_NAME);
                } else {
                    fieldType = null;
                }
                if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
                    final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();
                    for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                        var segmentSize = Lucene.segmentReader(ctx.reader()).getSegmentInfo().sizeInBytes();
                        if (isInteractive(ctx.reader(), fieldType, currentTimeMillis, boostWindowInterval)) {
                            interactiveSize += segmentSize;
                        } else {
                            nonInteractiveSize += segmentSize;
                        }
                    }
                } else {
                    // Closed indices are considered as non-interactive. We can use
                    // doc stats here because it computes accurate segment sizes.
                    nonInteractiveSize += indexShard.docStats().getTotalSizeInBytes();
                }
                long primaryTerm = indexShard.getOperationPrimaryTerm();
                final var engine = indexShard.getEngineOrNull();
                long generation = engine != null ? engine.getLastCommittedSegmentInfos().getGeneration() : 0L;
                return new ShardSize(interactiveSize, nonInteractiveSize, primaryTerm, generation);
            } catch (IOException e) {
                logger.warn("Failed to read shard size stats for {}", indexShard.shardId(), e);
                return null;
            }
        } catch (AlreadyClosedException | IllegalIndexShardStateException e) {
            logger.debug("{} failed to acquire searcher for shard size stats (shard is closing)", indexShard.shardId(), e);
            return null;
        } catch (Exception e) {
            logger.warn("Failed to acquire searcher for {}", indexShard.shardId(), e);
            return null;
        }
    }

    private static boolean isInteractive(
        LeafReader segmentReader,
        MappedFieldType fieldType,
        long currentTimeMillis,
        TimeValue interactiveDataAge
    ) throws IOException {
        PointValues values = segmentReader.getPointValues(DataStream.TIMESTAMP_FIELD_NAME);
        if (values == null || values.getMaxPackedValue() == null) {
            // no timestamp value, entire segment is considered interactive
            return true;
        }
        if (fieldType instanceof DateFieldMapper.DateFieldType dateFieldType) {
            var maxTimestampMillis = dateFieldType.resolution().parsePointAsMillis(values.getMaxPackedValue());
            return currentTimeMillis - maxTimestampMillis <= interactiveDataAge.millis();
        } else if (fieldType instanceof NumberFieldMapper.NumberFieldType numberFieldType) {
            var maxTimestampMillis = numberFieldType.parsePoint(values.getMaxPackedValue()).longValue();
            return currentTimeMillis - maxTimestampMillis <= interactiveDataAge.millis();
        } else {
            // @timestamp field is not representing a date nor number, entire segment is considered interactive
            return true;
        }
    }

    private static boolean isSizeAvailable(IndexShardState state) {
        return state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY;
    }
}
