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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
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

    public ShardSizeStatsReader(LongSupplier currentTimeMillsSupplier, IndicesService indicesService) {
        this.indicesService = indicesService;
        this.currentTimeMillsSupplier = currentTimeMillsSupplier;
    }

    public ShardSizeStatsReader(ThreadPool threadPool, IndicesService indicesService) {
        this(threadPool::absoluteTimeInMillis, indicesService);
    }

    public Map<ShardId, ShardSize> getAllShardSizes(TimeValue interactiveDataAge) {
        var sizes = new HashMap<ShardId, ShardSize>();
        for (var indexService : indicesService) {
            for (var indexShard : indexService) {
                var shardSize = getShardSize(indexShard, interactiveDataAge);
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
    public ShardSize getShardSize(ShardId shardId, TimeValue interactiveDataAge) {
        return getShardSize(indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id()), interactiveDataAge);
    }

    @Nullable
    public ShardSize getShardSize(IndexShard indexShard, TimeValue interactiveDataAge) {
        if (isSizeAvailable(indexShard.state()) == false) {
            return null;
        }

        try {
            try (var searcher = indexShard.acquireSearcher("shard_stats")) {
                var interactiveSize = 0L;
                var nonInteractiveSize = 0L;

                final MappedFieldType fieldType = indexShard.mapperService().fieldType(DataStream.TIMESTAMP_FIELD_NAME);
                final long currentTimeMillis = currentTimeMillsSupplier.getAsLong();
                for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                    var segmentSize = Lucene.segmentReader(ctx.reader()).getSegmentInfo().sizeInBytes();
                    if (isInteractive(ctx.reader(), fieldType, currentTimeMillis, interactiveDataAge)) {
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
        } catch (ElasticsearchSecurityException e) {
            // it is currently impossible to obtain a searcher for the system indices such as .security or .kibana
            // this temporarily assigns 10Mb weight for such indices until the issue is fixed
            return new ShardSize(ByteSizeValue.ofMb(10).getBytes(), 0);
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
