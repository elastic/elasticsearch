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
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.expectValueToken;

/**
 * Generates the shard id for {@code (id, routing)} pairs.
 */
public abstract class IndexRouting {

    static final NodeFeature LOGSB_ROUTE_ON_SORT_FIELDS = new NodeFeature("routing.logsb_route_on_sort_fields");

    /**
     * Build the routing from {@link IndexMetadata}.
     */
    public static IndexRouting fromIndexMetadata(IndexMetadata metadata) {
        if (false == metadata.getRoutingPaths().isEmpty()) {
            return new ExtractFromSource(metadata);
        }
        if (metadata.isRoutingPartitionedIndex()) {
            return new Partitioned(metadata);
        }
        return new Unpartitioned(metadata);
    }

    protected final String indexName;
    private final int routingNumShards;
    private final int routingFactor;

    private IndexRouting(IndexMetadata metadata) {
        this.indexName = metadata.getIndex().getName();
        this.routingNumShards = metadata.getRoutingNumShards();
        this.routingFactor = metadata.getRoutingFactor();
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
    public abstract int indexShard(String id, @Nullable String routing, XContentType sourceType, BytesReference source);

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
        public int indexShard(String id, @Nullable String routing, XContentType sourceType, BytesReference source) {
            if (id == null) {
                throw new IllegalStateException("id is required and should have been set by process");
            }
            checkRoutingRequired(id, routing);
            return shardId(id, routing);
        }

        @Override
        public int updateShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            return shardId(id, routing);
        }

        @Override
        public int deleteShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            return shardId(id, routing);
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
            consumer.accept(hashToShardId(effectiveRoutingToHash(routing)));
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
                consumer.accept(hashToShardId(hash + i));
            }
        }
    }

    public static class ExtractFromSource extends IndexRouting {
        private final Predicate<String> isRoutingPath;
        private final XContentParserConfiguration parserConfig;
        private final IndexMode indexMode;
        private final boolean trackTimeSeriesRoutingHash;
        private final boolean addIdWithRoutingHash;
        private int hash = Integer.MAX_VALUE;

        ExtractFromSource(IndexMetadata metadata) {
            super(metadata);
            if (metadata.isRoutingPartitionedIndex()) {
                throw new IllegalArgumentException("routing_partition_size is incompatible with routing_path");
            }
            indexMode = metadata.getIndexMode();
            trackTimeSeriesRoutingHash = indexMode == IndexMode.TIME_SERIES
                && metadata.getCreationVersion().onOrAfter(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID);
            addIdWithRoutingHash = indexMode == IndexMode.LOGSDB;
            List<String> routingPaths = metadata.getRoutingPaths();
            isRoutingPath = Regex.simpleMatcher(routingPaths.toArray(String[]::new));
            this.parserConfig = XContentParserConfiguration.EMPTY.withFiltering(null, Set.copyOf(routingPaths), null, true);
        }

        public boolean matchesField(String fieldName) {
            return isRoutingPath.test(fieldName);
        }

        @Override
        public void postProcess(IndexRequest indexRequest) {
            // Update the request with the routing hash, if needed.
            // This needs to happen in post-processing, after the routing hash is calculated.
            if (trackTimeSeriesRoutingHash) {
                indexRequest.routing(TimeSeriesRoutingHashFieldMapper.encode(hash));
            } else if (addIdWithRoutingHash) {
                assert hash != Integer.MAX_VALUE;
                indexRequest.autoGenerateTimeBasedId(OptionalInt.of(hash));
            }
        }

        @Override
        public int indexShard(String id, @Nullable String routing, XContentType sourceType, BytesReference source) {
            assert Transports.assertNotTransportThread("parsing the _source can get slow");
            checkNoRouting(routing);
            hash = hashSource(sourceType, source).buildHash(IndexRouting.ExtractFromSource::defaultOnEmpty);
            return hashToShardId(hash);
        }

        public String createId(XContentType sourceType, BytesReference source, byte[] suffix) {
            return hashSource(sourceType, source).createId(suffix, IndexRouting.ExtractFromSource::defaultOnEmpty);
        }

        public String createId(Map<String, Object> flat, byte[] suffix) {
            Builder b = builder();
            for (Map.Entry<String, Object> e : flat.entrySet()) {
                if (isRoutingPath.test(e.getKey())) {
                    if (e.getValue() instanceof List<?> listValue) {
                        for (Object v : listValue) {
                            b.addHash(e.getKey(), new BytesRef(v.toString()));
                        }
                    } else {
                        b.addHash(e.getKey(), new BytesRef(e.getValue().toString()));
                    }
                }
            }
            return b.createId(suffix, IndexRouting.ExtractFromSource::defaultOnEmpty);
        }

        private static int defaultOnEmpty() {
            throw new IllegalArgumentException("Error extracting routing: source didn't contain any routing fields");
        }

        public Builder builder() {
            return new Builder();
        }

        private Builder hashSource(XContentType sourceType, BytesReference source) {
            Builder b = builder();
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

        public class Builder {
            private final List<NameAndHash> hashes = new ArrayList<>();

            public void addMatching(String fieldName, BytesRef string) {
                if (isRoutingPath.test(fieldName)) {
                    addHash(fieldName, string);
                }
            }

            public String createId(byte[] suffix, IntSupplier onEmpty) {
                byte[] idBytes = new byte[4 + suffix.length];
                ByteUtils.writeIntLE(buildHash(onEmpty), idBytes, 0);
                System.arraycopy(suffix, 0, idBytes, 4, suffix.length);
                return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(idBytes);
            }

            private void extractObject(@Nullable String path, XContentParser source) throws IOException {
                while (source.currentToken() != Token.END_OBJECT) {
                    ensureExpectedToken(Token.FIELD_NAME, source.currentToken(), source);
                    String fieldName = source.currentName();
                    String subPath = path == null ? fieldName : path + "." + fieldName;
                    source.nextToken();
                    extractItem(subPath, source);
                }
            }

            private void extractArray(@Nullable String path, XContentParser source) throws IOException {
                while (source.currentToken() != Token.END_ARRAY) {
                    expectValueToken(source.currentToken(), source);
                    extractItem(path, source);
                }
            }

            private void extractItem(String path, XContentParser source) throws IOException {
                switch (source.currentToken()) {
                    case START_OBJECT:
                        source.nextToken();
                        extractObject(path, source);
                        source.nextToken();
                        break;
                    case VALUE_STRING:
                    case VALUE_NUMBER:
                    case VALUE_BOOLEAN:
                        addHash(path, new BytesRef(source.text()));
                        source.nextToken();
                        break;
                    case START_ARRAY:
                        source.nextToken();
                        extractArray(path, source);
                        source.nextToken();
                        break;
                    case VALUE_NULL:
                        source.nextToken();
                        break;
                    default:
                        throw new ParsingException(
                            source.getTokenLocation(),
                            "Cannot extract routing path due to unexpected token [{}]",
                            source.currentToken()
                        );
                }
            }

            private void addHash(String path, BytesRef value) {
                hashes.add(new NameAndHash(new BytesRef(path), hash(value), hashes.size()));
            }

            private int buildHash(IntSupplier onEmpty) {
                if (hashes.isEmpty()) {
                    return onEmpty.getAsInt();
                }
                Collections.sort(hashes);
                int hash = 0;
                for (NameAndHash nah : hashes) {
                    hash = 31 * hash + (hash(nah.name) ^ nah.hash);
                }
                return hash;
            }
        }

        private static int hash(BytesRef ref) {
            return StringHelper.murmurhash3_x86_32(ref, 0);
        }

        @Override
        public int updateShard(String id, @Nullable String routing) {
            throw new IllegalArgumentException(error("update"));
        }

        @Override
        public int deleteShard(String id, @Nullable String routing) {
            checkNoRouting(routing);
            return idToHash(id);
        }

        @Override
        public int getShard(String id, @Nullable String routing) {
            checkNoRouting(routing);
            return idToHash(id);
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
            // For TSDB, the hash is stored as the id prefix.
            // For LogsDB with routing on sort fields, the routing hash is stored in the range[id.length - 9, id.length - 5] of the id,
            // see IndexRequest#autoGenerateTimeBasedId.
            return hashToShardId(ByteUtils.readIntLE(idBytes, addIdWithRoutingHash ? idBytes.length - 9 : 0));
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
    }

    private record NameAndHash(BytesRef name, int hash, int order) implements Comparable<NameAndHash> {
        @Override
        public int compareTo(NameAndHash o) {
            int i = name.compareTo(o.name);
            if (i != 0) return i;
            // ensures array values are in the order as they appear in the source
            return Integer.compare(order, o.order);
        }
    }
}
