/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Generates the shard id for {@code (id, routing)} pairs.
 */
public abstract class IndexRouting {

    static final NodeFeature LOGSB_ROUTE_ON_SORT_FIELDS = new NodeFeature("routing.logsb_route_on_sort_fields");

    /**
     * Build the routing from {@link IndexMetadata}.
     */
    public static IndexRouting fromIndexMetadata(IndexMetadata metadata) {
        if (metadata.getIndexMode() == IndexMode.TIME_SERIES
            && metadata.getTimeSeriesDimensions().isEmpty() == false
            && metadata.getCreationVersion().onOrAfter(IndexVersions.TSID_CREATED_DURING_ROUTING)) {
            return new ExtractFromSource.ForIndexDimensions(metadata);
        }
        if (metadata.getRoutingPaths().isEmpty() == false) {
            return new ExtractFromSource.ForRoutingPath(metadata);
        }
        if (metadata.isRoutingPartitionedIndex()) {
            return new Partitioned(metadata);
        }
        return new Unpartitioned(metadata);
    }

    protected final String indexName;
    private final int routingNumShards;
    private final int routingFactor;
    @Nullable
    private final IndexReshardingMetadata indexReshardingMetadata;

    private IndexRouting(IndexMetadata metadata) {
        this.indexName = metadata.getIndex().getName();
        this.routingNumShards = metadata.getRoutingNumShards();
        this.routingFactor = metadata.getRoutingFactor();
        this.indexReshardingMetadata = metadata.getReshardingMetadata();
    }

    /**
     * Finalize the request before routing, with data needed for routing decisions.
     */
    public void preProcess(IndexRequest indexRequest) {}

    /**
     * Finalize the request after routing, incorporating data produced by the routing logic.
     */
    public void postProcess(IndexRequest indexRequest) {}

    /**
     * Called when indexing a document to generate the shard id that should contain
     * a document with the provided parameters.
     */
    public abstract int indexShard(IndexRequest indexRequest);

    /**
     * Called when indexing a document must be rerouted from the source shard to the target
     * during resharding. Should be similar to {@link #indexShard(IndexRequest)} while avoiding
     * the initial expense of having to calculate the routing parameters.
     */
    public abstract int rerouteToTarget(IndexRequest indexRequest);

    /**
     * Called when updating a document to generate the shard id that should contain
     * a document with the provided {@code _id} and (optional) {@code _routing}.
     */
    public abstract int updateShard(String id, @Nullable String routing);

    /**
     * Called when deleting a document to generate the shard id that should contain
     * a document with the provided {@code _id} and (optional) {@code _routing}.
     */
    public abstract int deleteShard(String id, @Nullable String routing);

    /**
     * Called when getting a document to generate the shard id that should contain
     * a document with the provided {@code _id} and (optional) {@code _routing}.
     */
    public abstract int getShard(String id, @Nullable String routing);

    /**
     * Collect all of the shard ids that *may* contain documents with the
     * provided {@code routing}. Indices with a {@code routing_partition}
     * will collect more than one shard. Indices without a partition
     * will collect the same shard id as would be returned
     * by {@link #getShard}.
     * <p>
     * Note: This is called for any search-like requests that have a
     * routing specified but <strong>only</strong> if they have a routing
     * specified. If they do not have a routing they just use all shards
     * in the index.
     */
    public abstract void collectSearchShards(String routing, IntConsumer consumer);

    /**
     * Convert a hash generated from an {@code (id, routing}) pair into a
     * shard id.
     */
    protected final int hashToShardId(int hash) {
        return Math.floorMod(hash, routingNumShards) / routingFactor;
    }

    /**
     * Convert a routing value into a hash.
     */
    private static int effectiveRoutingToHash(String effectiveRouting) {
        return Murmur3HashFunction.hash(effectiveRouting);
    }

    /**
     * Check if the _split index operation is allowed for an index
     * @throws IllegalArgumentException if the operation is not allowed
     */
    public void checkIndexSplitAllowed() {}

    /**
     * If this index is in the process of resharding, and the shard to which this request is being routed,
     * is a target shard that is not yet in HANDOFF state, then route it to the source shard.
     * @param shardId  shardId to which the current document is routed based on hashing
     * @return Updated shardId
     */
    protected final int rerouteWritesIfResharding(int shardId) {
        return rerouteFromSplitTargetShard(shardId, IndexReshardingState.Split.TargetShardState.HANDOFF);
    }

    protected final int rerouteSearchIfResharding(int shardId) {
        return rerouteFromSplitTargetShard(shardId, IndexReshardingState.Split.TargetShardState.SPLIT);
    }

    private int rerouteFromSplitTargetShard(int shardId, IndexReshardingState.Split.TargetShardState minimumRequiredState) {
        assert indexReshardingMetadata == null || indexReshardingMetadata.isSplit() : "Index resharding state is not a split";
        if (indexReshardingMetadata != null && indexReshardingMetadata.getSplit().isTargetShard(shardId)) {
            if (indexReshardingMetadata.getSplit().targetStateAtLeast(shardId, minimumRequiredState) == false) {
                return indexReshardingMetadata.getSplit().sourceShard(shardId);
            }
        }
        return shardId;
    }

    private abstract static class IdAndRoutingOnly extends IndexRouting {
        private final boolean routingRequired;
        private final IndexVersion creationVersion;
        private final IndexMode indexMode;

        IdAndRoutingOnly(IndexMetadata metadata) {
            super(metadata);
            this.creationVersion = metadata.getCreationVersion();
            MappingMetadata mapping = metadata.mapping();
            this.routingRequired = mapping == null ? false : mapping.routingRequired();
            this.indexMode = metadata.getIndexMode();
        }

        protected abstract int shardId(String id, @Nullable String routing);

        @Override
        public void preProcess(IndexRequest indexRequest) {
            // Generate id if not already provided.
            // This is needed for routing, so it has to happen in pre-processing.
            final String id = indexRequest.id();
            if (id == null) {
                if (shouldUseTimeBasedId(indexMode, creationVersion)) {
                    indexRequest.autoGenerateTimeBasedId();
                } else {
                    indexRequest.autoGenerateId();
                }
            } else if (id.isEmpty()) {
                throw new IllegalArgumentException("if _id is specified it must not be empty");
            }
        }

        private static boolean shouldUseTimeBasedId(final IndexMode indexMode, final IndexVersion creationVersion) {
            return indexMode == IndexMode.LOGSDB && isNewIndexVersion(creationVersion);
        }

        private static boolean isNewIndexVersion(final IndexVersion creationVersion) {
            return creationVersion.between(IndexVersions.TIME_BASED_K_ORDERED_DOC_ID_BACKPORT, IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
                || creationVersion.onOrAfter(IndexVersions.TIME_BASED_K_ORDERED_DOC_ID);
        }

        @Override
        public int indexShard(IndexRequest indexRequest) {
            String id = indexRequest.id();
            String routing = indexRequest.routing();
            if (id == null) {
                throw new IllegalStateException("id is required and should have been set by process");
            }
            checkRoutingRequired(id, routing);
            int shardId = shardId(id, routing);
            return rerouteWritesIfResharding(shardId);
        }

        @Override
        public int rerouteToTarget(IndexRequest indexRequest) {
            return indexShard(indexRequest);
        }

        @Override
        public int updateShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            int shardId = shardId(id, routing);
            return rerouteWritesIfResharding(shardId);
        }

        @Override
        public int deleteShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            int shardId = shardId(id, routing);
            return rerouteWritesIfResharding(shardId);
        }

        @Override
        public int getShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            return shardId(id, routing);
        }

        private void checkRoutingRequired(String id, @Nullable String routing) {
            if (routingRequired && routing == null) {
                throw new RoutingMissingException(indexName, id);
            }
        }
    }

    /**
     * Strategy for indices that are not partitioned.
     */
    private static class Unpartitioned extends IdAndRoutingOnly {
        Unpartitioned(IndexMetadata metadata) {
            super(metadata);
        }

        @Override
        protected int shardId(String id, @Nullable String routing) {
            return hashToShardId(effectiveRoutingToHash(routing == null ? id : routing));
        }

        @Override
        public void collectSearchShards(String routing, IntConsumer consumer) {
            consumer.accept(rerouteSearchIfResharding(hashToShardId(effectiveRoutingToHash(routing))));
        }
    }

    /**
     * Strategy for partitioned indices.
     */
    private static class Partitioned extends IdAndRoutingOnly {
        private final int routingPartitionSize;

        Partitioned(IndexMetadata metadata) {
            super(metadata);
            this.routingPartitionSize = metadata.getRoutingPartitionSize();
        }

        @Override
        protected int shardId(String id, @Nullable String routing) {
            if (routing == null) {
                throw new IllegalArgumentException("A routing value is required for gets from a partitioned index");
            }
            int offset = Math.floorMod(effectiveRoutingToHash(id), routingPartitionSize);
            return hashToShardId(effectiveRoutingToHash(routing) + offset);
        }

        @Override
        public void collectSearchShards(String routing, IntConsumer consumer) {
            int hash = effectiveRoutingToHash(routing);
            for (int i = 0; i < routingPartitionSize; i++) {
                consumer.accept(rerouteSearchIfResharding(hashToShardId(hash + i)));
            }
        }
    }

    /**
     * Base class for strategies that determine the shard by extracting and hashing fields from the document source.
     */
    public abstract static class ExtractFromSource extends IndexRouting {
        protected final XContentParserConfiguration parserConfig;
        private final IndexMode indexMode;
        private final boolean trackTimeSeriesRoutingHash;
        private final boolean useTimeSeriesSyntheticId;
        private final boolean addIdWithRoutingHash;
        private int hash = Integer.MAX_VALUE;

        ExtractFromSource(IndexMetadata metadata, List<String> includePaths) {
            super(metadata);
            if (metadata.isRoutingPartitionedIndex()) {
                throw new IllegalArgumentException("routing_partition_size is incompatible with routing_path");
            }
            indexMode = metadata.getIndexMode();
            assert indexMode != null : "Index mode must be set for ExtractFromSource routing";
            this.trackTimeSeriesRoutingHash = indexMode == IndexMode.TIME_SERIES
                && metadata.getCreationVersion().onOrAfter(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID);
            this.useTimeSeriesSyntheticId = metadata.useTimeSeriesSyntheticId();
            addIdWithRoutingHash = indexMode == IndexMode.LOGSDB;
            this.parserConfig = XContentParserConfiguration.EMPTY.withFiltering(null, Set.copyOf(includePaths), null, true);
        }

        @Override
        public void postProcess(IndexRequest indexRequest) {
            if (trackTimeSeriesRoutingHash) {
                indexRequest.routing(TimeSeriesRoutingHashFieldMapper.encode(hash));
            } else if (addIdWithRoutingHash) {
                assert hash != Integer.MAX_VALUE;
                indexRequest.autoGenerateTimeBasedId(OptionalInt.of(hash));
            }
        }

        @Override
        public int indexShard(IndexRequest indexRequest) {
            assert Transports.assertNotTransportThread("parsing the _source can get slow");
            checkNoRouting(indexRequest.routing());
            hash = hashSource(indexRequest);
            int shardId = hashToShardId(hash);
            return rerouteWritesIfResharding(shardId);
        }

        @Override
        public int rerouteToTarget(IndexRequest indexRequest) {
            if (trackTimeSeriesRoutingHash) {
                String routing = indexRequest.routing();
                if (routing == null) {
                    throw new IllegalStateException("Routing should be set by the coordinator");
                }
                return hashToShardId(TimeSeriesRoutingHashFieldMapper.decode(indexRequest.routing()));
            } else if (addIdWithRoutingHash) {
                return hashToShardId(idToHash(indexRequest.id()));
            } else {
                checkNoRouting(indexRequest.routing());
                return indexShard(indexRequest);
            }
        }

        protected abstract int hashSource(IndexRequest indexRequest);

        private static int defaultOnEmpty() {
            throw new IllegalArgumentException("Error extracting routing: source didn't contain any routing fields");
        }

        protected static int hash(BytesRef ref) {
            return StringHelper.murmurhash3_x86_32(ref, 0);
        }

        @Override
        public int updateShard(String id, @Nullable String routing) {
            throw new IllegalArgumentException(error("update"));
        }

        @Override
        public int deleteShard(String id, @Nullable String routing) {
            checkNoRouting(routing);
            int shardId = idToHash(id);
            return rerouteWritesIfResharding(shardId);
        }

        @Override
        public int getShard(String id, @Nullable String routing) {
            checkNoRouting(routing);
            int shardId = idToHash(id);
            return (rerouteWritesIfResharding(shardId));
        }

        private void checkNoRouting(@Nullable String routing) {
            if (routing != null) {
                throw new IllegalArgumentException(error("specifying routing"));
            }
        }

        private int idToHash(String id) {
            byte[] idBytes;
            try {
                idBytes = Base64.getUrlDecoder().decode(id);
            } catch (IllegalArgumentException e) {
                throw new ResourceNotFoundException("invalid id [{}] for index [{}] in " + indexMode.getName() + " mode", id, indexName);
            }
            if (idBytes.length < 4) {
                throw new ResourceNotFoundException("invalid id [{}] for index [{}] in " + indexMode.getName() + " mode", id, indexName);
            }
            int hash;
            if (addIdWithRoutingHash) {
                // For LogsDB with routing on sort fields, the routing hash is stored in the range[id.length - 9, id.length - 5] of the id,
                // see IndexRequest#autoGenerateTimeBasedId.
                hash = ByteUtils.readIntLE(idBytes, idBytes.length - 9);
            } else if (useTimeSeriesSyntheticId) {
                // For TSDB with synthetic ids, the hash is stored as the id suffix.
                hash = TsidExtractingIdFieldMapper.extractRoutingHashFromSyntheticId(new BytesRef(idBytes));
            } else {
                // For TSDB, the hash is stored as the id prefix.
                hash = ByteUtils.readIntLE(idBytes, 0);
            }
            return hashToShardId(hash);
        }

        @Override
        public void checkIndexSplitAllowed() {
            throw new IllegalArgumentException(error("index-split"));
        }

        @Override
        public void collectSearchShards(String routing, IntConsumer consumer) {
            throw new IllegalArgumentException(error("searching with a specified routing"));
        }

        private String error(String operation) {
            return operation + " is not supported because the destination index [" + indexName + "] is in " + indexMode.getName() + " mode";
        }

        /**
         * Strategy for indices that use {@link IndexMetadata#INDEX_ROUTING_PATH} to extract the routing value from the source.
         * This is used primarily for time-series indices created before {@link IndexVersions#TSID_CREATED_DURING_ROUTING}
         * and for LogsDB indices that route on specific fields.
         * For time-series indices this strategy will result in dimensions to be extracted and hashed twice during indexing:
         * once in the coordinating node during shard routing and then again in the data node to create the tsid during document parsing.
         * The {@link ForIndexDimensions} strategy avoids this double hashing.
         */
        public static class ForRoutingPath extends ExtractFromSource {
            private final Predicate<String> isRoutingPath;

            ForRoutingPath(IndexMetadata metadata) {
                super(metadata, metadata.getRoutingPaths());
                isRoutingPath = Regex.simpleMatcher(metadata.getRoutingPaths().toArray(String[]::new));
            }

            @Override
            protected int hashSource(IndexRequest indexRequest) {
                return hashRoutingFields(indexRequest.getContentType(), indexRequest.source()).buildHash(
                    IndexRouting.ExtractFromSource::defaultOnEmpty
                );
            }

            public String createId(XContentType sourceType, BytesReference source, byte[] suffix) {
                return hashRoutingFields(sourceType, source).createId(suffix, IndexRouting.ExtractFromSource::defaultOnEmpty);
            }

            public RoutingHashBuilder builder() {
                return new RoutingHashBuilder(isRoutingPath);
            }

            private RoutingHashBuilder hashRoutingFields(XContentType sourceType, BytesReference source) {
                RoutingHashBuilder b = builder();
                try (XContentParser parser = XContentHelper.createParserNotCompressed(parserConfig, source, sourceType)) {
                    parser.nextToken(); // Move to first token
                    if (parser.currentToken() == null) {
                        throw new IllegalArgumentException("Error extracting routing: source didn't contain any routing fields");
                    }
                    parser.nextToken();
                    b.extractObject(null, parser);
                    ensureExpectedToken(null, parser.nextToken(), parser);
                } catch (IOException | ParsingException e) {
                    throw new IllegalArgumentException("Error extracting routing: " + e.getMessage(), e);
                }
                return b;
            }

            public boolean matchesField(String fieldName) {
                return isRoutingPath.test(fieldName);
            }
        }

        /**
         * Strategy for time-series indices that use {@link IndexMetadata#INDEX_DIMENSIONS} to extract the tsid from the source.
         * This strategy avoids double hashing of dimensions during indexing.
         * It requires that the index was created with {@link IndexVersions#TSID_CREATED_DURING_ROUTING} or later.
         * It creates the tsid during routing and makes the routing decision based on the tsid.
         * The tsid gets attached to the index request so that the data node can reuse it instead of rebuilding it.
         */
        public static class ForIndexDimensions extends ExtractFromSource {

            ForIndexDimensions(IndexMetadata metadata) {
                super(metadata, metadata.getTimeSeriesDimensions());
                assert metadata.getIndexMode() == IndexMode.TIME_SERIES : "Index mode must be time_series for ForIndexDimensions routing";
                assert metadata.getCreationVersion().onOrAfter(IndexVersions.TSID_CREATED_DURING_ROUTING)
                    : "Index version must be at least "
                        + IndexVersions.TSID_CREATED_DURING_ROUTING
                        + " for ForIndexDimensions routing but was "
                        + metadata.getCreationVersion();
            }

            @Override
            protected int hashSource(IndexRequest indexRequest) {
                BytesRef tsid = indexRequest.tsid();
                if (tsid == null) {
                    tsid = buildTsid(indexRequest.getContentType(), indexRequest.indexSource().bytes());
                    indexRequest.tsid(tsid);
                }
                return hash(tsid);
            }

            public BytesRef buildTsid(XContentType sourceType, BytesReference source) {
                TsidBuilder b = new TsidBuilder();
                try (XContentParser parser = XContentHelper.createParserNotCompressed(parserConfig, source, sourceType)) {
                    b.add(parser, XContentParserTsidFunnel.get());
                } catch (IOException | ParsingException e) {
                    throw new IllegalArgumentException("Error extracting tsid: " + e.getMessage(), e);
                }
                return b.buildTsid();
            }
        }
    }
}
