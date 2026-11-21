/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.FeatureFlag;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A {@link FilterDirectory} that holds state a codec shares across the segments of one shard, for codecs whose segments are
 * better off agreeing on something they would otherwise each derive for themselves.
 *
 * <p>Lucene builds the codec that reads a segment by name, so a reader has no handle on the shard it belongs to other than
 * its {@code SegmentInfo.dir}, which is the shard's {@code Store} directory chain for every segment written and read by that
 * shard. A codec reaches this state through {@link #getOrCreate} on that directory, from both its writers (flush and
 * merge) and its readers.
 *
 * <p>Like {@link FieldInfoCachingDirectory}, this wrapper sits inside the {@code Store}'s Directory chain, between the
 * underlying {@code ByteSizeDirectory} and the outer {@code StoreDirectory}, so that {@code store.directory()} continues to
 * return the same {@code StoreDirectory}. The two are separate layers as they are separate features, each behind its own
 * feature flag.
 *
 * <p>The state is only held in memory, for the lifetime of the shard's {@code Store}, and is never persisted: it starts empty
 * whenever a new {@code Store} is created, for instance after a node restart, on a relocation target or on recovery. It must
 * therefore only ever be a hint: segments stay self-describing, and a codec must behave correctly, if less efficiently, when
 * the directory is not wrapped (the feature flag is off, tests, tools reading a shard outside of a {@code Store}) or its
 * state is empty. {@link #getOrCreate} handles a directory that is not wrapped by returning a state of its own to each
 * caller.
 *
 * <p>Each state reports the heap it holds, as {@link Accountable}, and this directory reports their sum.
 */
public final class ShardCodecStateDirectory extends DelegatingByteSizeDirectory implements Accountable {

    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("shard_codec_state_directory");

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ShardCodecStateDirectory.class)
        + RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);

    private final ConcurrentHashMap<Class<?>, Accountable> states = new ConcurrentHashMap<>();

    ShardCodecStateDirectory(Directory delegate) {
        super(delegate);
    }

    /**
     * Wraps the given directory with a {@link ShardCodecStateDirectory} if the feature flag is enabled and the directory is not
     * already wrapped. Otherwise returns the directory unchanged. Idempotent.
     */
    public static Directory wrapIfEnabled(Directory directory) {
        if (FEATURE_FLAG.isEnabled() == false) {
            return directory;
        }
        if (unwrap(directory, ShardCodecStateDirectory.class) != null) {
            return directory;
        }
        return new ShardCodecStateDirectory(directory);
    }

    /**
     * Returns the state of the given type of the shard that {@code directory}, typically a segment's
     * {@code SegmentInfo.dir}, belongs to, creating it with {@code factory} on first use. If the directory is not wrapped,
     * returns a new state from {@code factory}, shared with nobody, so a codec works the same, without sharing.
     * <p>
     * Callers should get the state once per writer or reader and keep it, rather than look it up on each use: on a directory
     * that is not wrapped, each call returns a new state.
     * <p>
     * On a wrapped directory, the factory is called once per type unless it throws. It must return a non-null instance, must
     * not call this method itself, and must be cheap, an allocation with no training or I/O, as other callers can wait on
     * it. The state is shared by all threads using the shard, so it must be thread-safe.
     */
    public static <T extends Accountable> T getOrCreate(Directory directory, Class<T> type, Supplier<T> factory) {
        ShardCodecStateDirectory stateDirectory = unwrap(directory, ShardCodecStateDirectory.class);
        if (stateDirectory == null) {
            return Objects.requireNonNull(factory.get());
        }
        return type.cast(stateDirectory.states.computeIfAbsent(type, k -> Objects.requireNonNull(factory.get())));
    }

    @Override
    public long ramBytesUsed() {
        long ramBytesUsed = BASE_RAM_BYTES_USED;
        for (Accountable state : states.values()) {
            ramBytesUsed += RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + state.ramBytesUsed();
        }
        return ramBytesUsed;
    }
}
