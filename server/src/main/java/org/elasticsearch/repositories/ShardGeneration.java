/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.util.Objects;
import java.util.Random;

/**
 * The generation ID of a shard, used to name the shard-level {@code index-$SHARD_GEN} file that represents a {@link
 * BlobStoreIndexShardSnapshots} instance. Before 7.6 ({@link SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION}) these generations were
 * numeric, but recent versions use a UUID instead.
 */
public final class ShardGeneration implements Writeable, ToXContentFragment {

    private final String rawGeneration;

    /**
     * @return a new (fresh) shard generation.
     */
    public static ShardGeneration newGeneration() {
        return new ShardGeneration(UUIDs.randomBase64UUID());
    }

    /**
     * @return a new (fresh) shard generation generated using the given {@link Random} for repeatability in tests.
     */
    public static ShardGeneration newGeneration(Random random) {
        return new ShardGeneration(UUIDs.randomBase64UUID(random));
    }

    /**
     * Construct a specific {@link ShardGeneration}. Doing this is generally a mistake, you should either create a new fresh one with {@link
     * #newGeneration} or else read one from the wire with {@link #ShardGeneration(StreamInput)} or {@link #fromXContent(XContentParser)}.
     */
    public ShardGeneration(String rawGeneration) {
        this.rawGeneration = Objects.requireNonNull(rawGeneration);
    }

    /**
     * Construct a specific {@link ShardGeneration} for a repository using the legacy numeric format.
     */
    public ShardGeneration(long legacyGeneration) {
        rawGeneration = String.valueOf(legacyGeneration);
    }

    public ShardGeneration(StreamInput in) throws IOException {
        rawGeneration = in.readString();
    }

    public static ShardGeneration fromXContent(XContentParser parser) throws IOException {
        final String generationString = parser.textOrNull();
        // it's null iff using legacy generations that aren't tracked in the RepositoryData
        return generationString == null ? null : new ShardGeneration(generationString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rawGeneration);
    }

    /**
     * Convert to a {@link String} for use in naming the {@code index-$SHARD_GEN} blob containing a {@link BlobStoreIndexShardSnapshots}.
     */
    public String toBlobNamePart() {
        return rawGeneration;
    }

    @Override
    public String toString() {
        return rawGeneration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardGeneration that = (ShardGeneration) o;
        return rawGeneration.equals(that.rawGeneration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawGeneration);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(rawGeneration);
        return builder;
    }
}
