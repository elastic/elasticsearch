/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Generates the shard id for {@code (id, routing)} pairs.
 */
public abstract class IndexRouting {
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

        IdAndRoutingOnly(IndexMetadata metadata) {
            super(metadata);
            MappingMetadata mapping = metadata.mapping();
            this.routingRequired = mapping == null ? false : mapping.routingRequired();
        }

        protected abstract int shardId(String id, @Nullable String routing);

        @Override
        public int indexShard(String id, @Nullable String routing, XContentType sourceType, BytesReference source) {
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

    private static class ExtractFromSource extends IndexRouting {
        private final XContentParserConfiguration parserConfig;

        ExtractFromSource(IndexMetadata metadata) {
            super(metadata);
            if (metadata.isRoutingPartitionedIndex()) {
                throw new IllegalArgumentException("routing_partition_size is incompatible with routing_path");
            }
            this.parserConfig = XContentParserConfiguration.EMPTY.withFiltering(Set.copyOf(metadata.getRoutingPaths()), null, true);
        }

        @Override
        public int indexShard(String id, @Nullable String routing, XContentType sourceType, BytesReference source) {
            if (routing != null) {
                throw new IllegalArgumentException(error("indexing with a specified routing"));
            }
            assert Transports.assertNotTransportThread("parsing the _source can get slow");

            List<NameAndHash> hashes = new ArrayList<>();
            try {
                try (XContentParser parser = sourceType.xContent().createParser(parserConfig, source.streamInput())) {
                    parser.nextToken(); // Move to first token
                    if (parser.currentToken() == null) {
                        throw new IllegalArgumentException("Error extracting routing: source didn't contain any routing fields");
                    }
                    parser.nextToken();
                    extractObject(hashes, null, parser);
                    ensureExpectedToken(null, parser.nextToken(), parser);
                }
            } catch (IOException | ParsingException e) {
                throw new IllegalArgumentException("Error extracting routing: " + e.getMessage(), e);
            }
            return hashToShardId(hashesToHash(hashes));
        }

        private static void extractObject(List<NameAndHash> hashes, @Nullable String path, XContentParser source) throws IOException {
            while (source.currentToken() != Token.END_OBJECT) {
                ensureExpectedToken(Token.FIELD_NAME, source.currentToken(), source);
                String fieldName = source.currentName();
                String subPath = path == null ? fieldName : path + "." + fieldName;
                source.nextToken();
                extractItem(hashes, subPath, source);
            }
        }

        private static void extractItem(List<NameAndHash> hashes, String path, XContentParser source) throws IOException {
            switch (source.currentToken()) {
                case START_OBJECT:
                    source.nextToken();
                    extractObject(hashes, path, source);
                    source.nextToken();
                    break;
                case VALUE_STRING:
                    hashes.add(new NameAndHash(new BytesRef(path), hash(new BytesRef(source.text()))));
                    source.nextToken();
                    break;
                case VALUE_NULL:
                    source.nextToken();
                    break;
                default:
                    throw new ParsingException(
                        source.getTokenLocation(),
                        "Routing values must be strings but found [{}]",
                        source.currentToken()
                    );
            }
        }

        private static int hash(BytesRef ref) {
            return StringHelper.murmurhash3_x86_32(ref, 0);
        }

        private static int hashesToHash(List<NameAndHash> hashes) {
            Collections.sort(hashes);
            Iterator<NameAndHash> itr = hashes.iterator();
            if (itr.hasNext() == false) {
                throw new IllegalArgumentException("Error extracting routing: source didn't contain any routing fields");
            }
            NameAndHash prev = itr.next();
            int hash = hash(prev.name) ^ prev.hash;
            while (itr.hasNext()) {
                NameAndHash next = itr.next();
                if (prev.name.equals(next.name)) {
                    throw new IllegalArgumentException("Duplicate routing dimension for [" + next.name + "]");
                }
                int thisHash = hash(next.name) ^ next.hash;
                hash = 31 * hash + thisHash;
                prev = next;
            }
            return hash;
        }

        @Override
        public int updateShard(String id, @Nullable String routing) {
            throw new IllegalArgumentException(error("update"));
        }

        @Override
        public int deleteShard(String id, @Nullable String routing) {
            throw new IllegalArgumentException(error("delete"));
        }

        @Override
        public int getShard(String id, @Nullable String routing) {
            throw new IllegalArgumentException(error("get"));
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
            return operation + " is not supported because the destination index [" + indexName + "] is in time series mode";
        }
    }

    private static record NameAndHash(BytesRef name, int hash) implements Comparable<NameAndHash> {
        @Override
        public int compareTo(NameAndHash o) {
            return name.compareTo(o.name);
        }
    }
}
