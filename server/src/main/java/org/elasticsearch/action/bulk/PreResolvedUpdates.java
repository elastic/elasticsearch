/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFields;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.core.Strings.format;

/**
 * Best-effort pre-resolution of the documents targeted by a {@link BulkShardRequest}'s update operations, allowing
 * their stored-fields reads to be prefetched before execution.
 *
 * <p>Slots are indexed by position in {@link BulkShardRequest#items()} and consumed at most once via {@link #take};
 * empty or already-taken slots resolve at execution time instead. Closing releases the acquired searcher of every
 * slot that was never consumed.
 */
public final class PreResolvedUpdates implements Releasable {

    public static final Setting<Boolean> PRE_RESOLVE_BULK_UPDATES = boolSetting(
        "indices.pre_resolve_bulk_updates",
        false,
        Setting.Property.NodeScope
    );

    static final PreResolvedUpdates EMPTY = new PreResolvedUpdates(null);

    private static final Logger logger = LogManager.getLogger(PreResolvedUpdates.class);

    @Nullable
    private UpdateHelper.PreResolvedUpdate[] slots;

    private PreResolvedUpdates(@Nullable UpdateHelper.PreResolvedUpdate[] slots) {
        this.slots = slots;
    }

    /**
     * Pre-resolves the document of every update operation in the request. Skips operations that
     * {@link UpdateHelper#preResolve} declines or that target the same id as an earlier op of any type in the bulk.
     * Never throws: on failure everything acquired so far is released and {@link #EMPTY} is returned.
     */
    static PreResolvedUpdates resolve(
        BulkShardRequest request,
        IndexShard primary,
        UpdateHelper updateHelper,
        LongSupplier nowInMillis,
        FetchSourceContext fetchSourceContext
    ) {
        // Updates are not supported in indices with sequence numbers disabled
        if (primary.indexSettings().sequenceNumbersDisabled()) {
            return EMPTY;
        }
        final BulkItemRequest[] items = request.items();
        UpdateHelper.PreResolvedUpdate[] slots = null;
        try {
            Set<String> seenIds = Sets.newHashSetWithExpectedSize(items.length);
            // Shared per segment so Lucene's CompressingStoredFieldsReader can reuse its
            // per-chunk decompression state across docs in the same leaf.
            IdentityHashMap<LeafReader, StoredFields> storedFieldsCache = null;
            for (int i = 0; i < items.length; i++) {
                final DocWriteRequest<?> itemRequest = items[i].request();
                // ops without an id (not yet auto-generated) cannot clash with an update's target, and aborted
                // items never execute: nothing to resolve, and no write for a later update to miss
                if (itemRequest == null || itemRequest.id() == null || isAborted(items[i].getPrimaryResponse())) {
                    continue;
                }
                if (seenIds.add(itemRequest.id()) == false || itemRequest.opType() != DocWriteRequest.OpType.UPDATE) {
                    // an earlier op of any type may write this doc; a pre-bulk get would miss that write, so
                    // such updates resolve at execution time
                    continue;
                }
                final var preResolved = updateHelper.preResolve(
                    (UpdateRequest) itemRequest,
                    primary,
                    nowInMillis,
                    fetchSourceContext,
                    request.splitShardCountSummary()
                );
                if (preResolved != null) {
                    if (slots == null) {
                        slots = new UpdateHelper.PreResolvedUpdate[items.length];
                    }
                    slots[i] = preResolved;
                    if (storedFieldsCache == null) {
                        storedFieldsCache = new IdentityHashMap<>();
                    }
                    try {
                        preResolved.prefetch(storedFieldsCache);
                    } catch (IOException e) {
                        logger.debug("prefetch stored fields failed for [{}]", preResolved.id(), e);
                    }
                }
            }
        } catch (Exception e) {
            logger.debug(() -> format("%s failed to pre-resolve updates", primary.shardId()), e);
            if (slots != null) {
                Releasables.closeWhileHandlingException(slots);
            }
            return EMPTY;
        }
        return slots == null ? EMPTY : new PreResolvedUpdates(slots);
    }

    private static boolean isAborted(@Nullable BulkItemResponse response) {
        return response != null && response.isFailed() && response.getFailure().isAborted();
    }

    int size() {
        return slots == null ? 0 : slots.length;
    }

    /** Returns the slot's pre-resolved update without consuming it, or {@code null}. */
    @Nullable
    UpdateHelper.PreResolvedUpdate get(int slot) {
        return slots == null ? null : slots[slot];
    }

    /** Returns and clears the slot's pre-resolved update, or {@code null}. The caller owns releasing it. */
    @Nullable
    UpdateHelper.PreResolvedUpdate take(int slot) {
        if (slots == null) {
            return null;
        }
        final UpdateHelper.PreResolvedUpdate preResolved = slots[slot];
        slots[slot] = null;
        return preResolved;
    }

    @Override
    public void close() {
        final UpdateHelper.PreResolvedUpdate[] toRelease = slots;
        slots = null;
        if (toRelease != null) {
            Releasables.close(toRelease);
        }
    }
}
