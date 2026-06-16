/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Streaming K-way sorted merge on the coordinator. Reads pages arriving from K data-node sinks
 * (tagged with their sink ID by {@link ExchangeSourceHandler}), routes them to per-source deques,
 * and merges them element-by-element using a min-heap so that output is globally sorted.
 *
 * <p>Unlike {@link TopNOperator}, this operator emits output while still receiving input: it
 * starts merging as soon as it has heard from every registered sink (either received at least one
 * page or the sink is recorded in {@link ExchangeSourceHandler#completedSinks()}). This enables a
 * downstream {@code LIMIT} to terminate the pipeline early — the data nodes receive the
 * cancellation signal and stop sending pages, keeping memory bounded to O(K × pageSize)
 * regardless of the total index size.
 *
 * <p>Correctness relies on the practical guarantee that all
 * {@link ExchangeSourceHandler#addRemoteSink} calls complete before any data page can arrive from
 * a data node, because the page flow requires a network round-trip that takes significantly longer
 * than the coordinator's synchronous sink-registration loop.
 */
public final class SortedMergeSourceOperator extends SourceOperator {

    public static final class Factory implements SourceOperatorFactory {
        private final ExchangeSourceHandler exchangeSourceHandler;
        private final List<TopNOperator.SortOrder> sortOrders;
        private final ElementType[] elementTypes;
        private final int maxPageSize;
        /**
         * Channel index of the {@link DocBlock} in incoming pages, or {@code -1} if absent.
         * When present, the DocVector's (shard, segment, doc) triple is used as a tiebreaker
         * after all sort-key comparisons return 0, making the per-node merge deterministic.
         */
        private final int docChannel;

        public Factory(
            ExchangeSourceHandler exchangeSourceHandler,
            List<TopNOperator.SortOrder> sortOrders,
            ElementType[] elementTypes,
            int maxPageSize,
            int docChannel
        ) {
            this.exchangeSourceHandler = exchangeSourceHandler;
            this.sortOrders = sortOrders;
            this.elementTypes = elementTypes;
            this.maxPageSize = maxPageSize;
            this.docChannel = docChannel;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            ExchangeSource source = exchangeSourceHandler.createExchangeSource();
            return new SortedMergeSourceOperator(
                exchangeSourceHandler,
                source,
                sortOrders,
                elementTypes,
                driverContext.blockFactory(),
                maxPageSize,
                docChannel
            );
        }

        @Override
        public String describe() {
            return "SortedMergeSourceOperator[sortOrders=" + sortOrders + "]";
        }
    }

    private final ExchangeSourceHandler exchangeSourceHandler;
    private final ExchangeSource exchangeSource;
    private final List<TopNOperator.SortOrder> sortOrders;
    private final ElementType[] elementTypes;
    private final BlockFactory blockFactory;
    private final int maxPageSize;
    /**
     * Channel index of the {@link DocBlock} in incoming pages, or {@code -1} if absent.
     * @see Factory#docChannel
     */
    private final int docChannel;

    /** Per-source page queues: sinkId → ordered deque of pages still to merge. */
    private final Map<Integer, ArrayDeque<Page>> sourceQueues = new HashMap<>();
    /** Per-source cursor: sinkId → current position within the queue's front page. */
    private final Map<Integer, Integer> sourceCursors = new HashMap<>();
    /**
     * Source IDs currently present in the K-way heap. Maintained separately because
     * {@link PriorityQueue} does not expose membership queries.
     */
    private final Set<Integer> inHeap = new HashSet<>();

    /** K-way min-heap ordered by the front element of each source. */
    private PriorityQueue<Integer> heap;
    private boolean heapInitialized = false;

    private boolean finished = false;

    /** Scratch space for BytesRef comparisons — avoids allocating on every compare call. */
    private final BytesRef scratch1 = new BytesRef();
    private final BytesRef scratch2 = new BytesRef();

    SortedMergeSourceOperator(
        ExchangeSourceHandler exchangeSourceHandler,
        ExchangeSource exchangeSource,
        List<TopNOperator.SortOrder> sortOrders,
        ElementType[] elementTypes,
        BlockFactory blockFactory,
        int maxPageSize,
        int docChannel
    ) {
        this.exchangeSourceHandler = exchangeSourceHandler;
        this.exchangeSource = exchangeSource;
        this.sortOrders = sortOrders;
        this.elementTypes = elementTypes;
        this.blockFactory = blockFactory;
        this.maxPageSize = maxPageSize;
        this.docChannel = docChannel;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void finish() {
        if (finished == false) {
            finished = true;
            releaseBufferedPages();
            exchangeSource.finish();
        }
    }

    @Override
    public void close() {
        if (finished == false) {
            finished = true;
            releaseBufferedPages();
        }
        exchangeSource.finish();
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (finished) {
            return NOT_BLOCKED;
        }
        if (heapInitialized && heap.isEmpty() == false) {
            return NOT_BLOCKED;
        }
        if (heapInitialized && exchangeSource.isFinished()) {
            return NOT_BLOCKED;
        }
        return exchangeSource.waitForReading();
    }

    @Override
    public Page getOutput() {
        if (finished) {
            return null;
        }

        drainExchange();

        if (heapInitialized == false && canStartMerge()) {
            initializeHeap();
        }

        if (heapInitialized == false) {
            return null;
        }

        replenishHeap();

        if (heap.isEmpty()) {
            if (exchangeSource.isFinished() || allSourcesExhausted()) {
                finished = true;
            }
            return null;
        }

        return emitPage();
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /**
     * Drain all immediately available pages from the exchange buffer, routing them to per-source
     * queues.
     */
    private void drainExchange() {
        while (true) {
            Page page = exchangeSource.pollPage();
            if (page == null) {
                break;
            }
            int sid = page.sourceId();
            if (sid >= 1) {
                if (page.getPositionCount() > 0) {
                    sourceQueues.computeIfAbsent(sid, k -> new ArrayDeque<>()).add(page);
                } else {
                    page.releaseBlocks();
                }
            } else {
                // Untagged page (sid == -1): defensive discard.
                page.releaseBlocks();
            }
        }
    }

    /**
     * Returns {@code true} when it is safe to start the K-way merge: either the exchange has
     * fully finished (all sources done) or we have heard from every registered remote sink
     * (received at least one page from it or it has completed with no pages).
     */
    private boolean canStartMerge() {
        if (exchangeSource.isFinished()) {
            return true;
        }
        int k = exchangeSourceHandler.numRegisteredRemoteSinks();
        if (k == 0) {
            return false;
        }
        // "Heard from" = have a page queue for this sink OR it already completed.
        int heard = sourceQueues.size();
        for (int sinkId = 1; sinkId <= k; sinkId++) {
            if (sourceQueues.containsKey(sinkId) == false && exchangeSourceHandler.completedSinks().contains(sinkId)) {
                heard++;
            }
        }
        return heard >= k;
    }

    private void initializeHeap() {
        heap = new PriorityQueue<>(Math.max(1, sourceQueues.size()), this::compareSourceFronts);
        for (Integer sid : sourceQueues.keySet()) {
            if (sourceQueues.get(sid).isEmpty() == false) {
                heap.add(sid);
                inHeap.add(sid);
            }
        }
        heapInitialized = true;
    }

    /**
     * After draining the exchange, re-add any sources that have new pages but are not yet in the
     * heap.
     */
    private void replenishHeap() {
        for (Map.Entry<Integer, ArrayDeque<Page>> entry : sourceQueues.entrySet()) {
            int sid = entry.getKey();
            if (entry.getValue().isEmpty() == false && inHeap.contains(sid) == false) {
                heap.add(sid);
                inHeap.add(sid);
            }
        }
    }

    private boolean isSinkDone(int sinkId) {
        return exchangeSourceHandler.completedSinks().contains(sinkId) || exchangeSource.isFinished();
    }

    private boolean allSourcesExhausted() {
        for (Map.Entry<Integer, ArrayDeque<Page>> entry : sourceQueues.entrySet()) {
            if (entry.getValue().isEmpty() == false) {
                return false;
            }
            if (isSinkDone(entry.getKey()) == false) {
                return false;
            }
        }
        return true;
    }

    private int compareSourceFronts(int sourceA, int sourceB) {
        Page pageA = sourceQueues.get(sourceA).peek();
        int posA = sourceCursors.getOrDefault(sourceA, 0);
        Page pageB = sourceQueues.get(sourceB).peek();
        int posB = sourceCursors.getOrDefault(sourceB, 0);
        int cmp = compareRows(pageA, posA, pageB, posB);
        if (cmp != 0) {
            return cmp;
        }
        // Final tiebreaker: order by source ID so that equal-key rows from different sources are
        // always emitted in a consistent order. This makes the global merge deterministic even when
        // the DocVector tiebreaker is unavailable (e.g. at the coordinator level where DocVectors
        // are not serializable). The ordering is by source-assignment order — stable per query.
        return Integer.compare(sourceA, sourceB);
    }

    private int compareRows(Page pageA, int posA, Page pageB, int posB) {
        for (TopNOperator.SortOrder order : sortOrders) {
            int channel = order.channel();
            Block blockA = pageA.getBlock(channel);
            Block blockB = pageB.getBlock(channel);
            boolean nullA = blockA.isNull(posA);
            boolean nullB = blockB.isNull(posB);

            if (nullA && nullB) {
                continue;
            }
            if (nullA) {
                return order.nullsFirst() ? -1 : 1;
            }
            if (nullB) {
                return order.nullsFirst() ? 1 : -1;
            }

            int cmp = compareNonNull(blockA, posA, blockB, posB, elementTypes[channel], order.asc());
            if (cmp != 0) {
                return order.asc() ? cmp : -cmp;
            }
        }
        // Sort-key tiebreaker: when all sort keys are equal, use the DocVector's (shard, segment, doc)
        // triple, which is globally unique within a node. Only available when docChannel >= 0 (i.e. the
        // late-materialization path where _doc is present in the page).
        if (docChannel >= 0) {
            DocVector dvA = ((DocBlock) pageA.getBlock(docChannel)).asVector();
            DocVector dvB = ((DocBlock) pageB.getBlock(docChannel)).asVector();
            int cmpShard = Integer.compare(dvA.shards().getInt(posA), dvB.shards().getInt(posB));
            if (cmpShard != 0) {
                return cmpShard;
            }
            int cmpSeg = Integer.compare(dvA.segments().getInt(posA), dvB.segments().getInt(posB));
            if (cmpSeg != 0) {
                return cmpSeg;
            }
            return Integer.compare(dvA.docs().getInt(posA), dvB.docs().getInt(posB));
        }
        return 0;
    }

    /**
     * Compares the values at the given positions for non-null fields. For multi-valued positions
     * uses the minimum value when ascending or the maximum when descending, consistent with how
     * data nodes select their sort key.
     */
    private int compareNonNull(Block a, int posA, Block b, int posB, ElementType type, boolean asc) {
        return switch (type) {
            case LONG -> Long.compare(longSortKey((LongBlock) a, posA, asc), longSortKey((LongBlock) b, posB, asc));
            case INT -> Integer.compare(intSortKey((IntBlock) a, posA, asc), intSortKey((IntBlock) b, posB, asc));
            case DOUBLE -> Double.compare(doubleSortKey((DoubleBlock) a, posA, asc), doubleSortKey((DoubleBlock) b, posB, asc));
            case FLOAT -> Float.compare(floatSortKey((FloatBlock) a, posA, asc), floatSortKey((FloatBlock) b, posB, asc));
            case BOOLEAN -> Boolean.compare(boolSortKey((BooleanBlock) a, posA, asc), boolSortKey((BooleanBlock) b, posB, asc));
            case BYTES_REF -> {
                BytesRef ra = bytesSortKey((BytesRefBlock) a, posA, asc, scratch1);
                BytesRef rb = bytesSortKey((BytesRefBlock) b, posB, asc, scratch2);
                yield ra.compareTo(rb);
            }
            default -> throw new IllegalArgumentException("SortedMergeSourceOperator: unsupported sort type " + type);
        };
    }

    // ---- per-type sort-key extractors (min for asc, max for desc) ----

    private static long longSortKey(LongBlock block, int pos, boolean asc) {
        int count = block.getValueCount(pos);
        int first = block.getFirstValueIndex(pos);
        long val = block.getLong(first);
        for (int i = 1; i < count; i++) {
            long v = block.getLong(first + i);
            val = asc ? Math.min(val, v) : Math.max(val, v);
        }
        return val;
    }

    private static int intSortKey(IntBlock block, int pos, boolean asc) {
        int count = block.getValueCount(pos);
        int first = block.getFirstValueIndex(pos);
        int val = block.getInt(first);
        for (int i = 1; i < count; i++) {
            int v = block.getInt(first + i);
            val = asc ? Math.min(val, v) : Math.max(val, v);
        }
        return val;
    }

    private static double doubleSortKey(DoubleBlock block, int pos, boolean asc) {
        int count = block.getValueCount(pos);
        int first = block.getFirstValueIndex(pos);
        double val = block.getDouble(first);
        for (int i = 1; i < count; i++) {
            double v = block.getDouble(first + i);
            val = asc ? Math.min(val, v) : Math.max(val, v);
        }
        return val;
    }

    private static float floatSortKey(FloatBlock block, int pos, boolean asc) {
        int count = block.getValueCount(pos);
        int first = block.getFirstValueIndex(pos);
        float val = block.getFloat(first);
        for (int i = 1; i < count; i++) {
            float v = block.getFloat(first + i);
            val = asc ? Math.min(val, v) : Math.max(val, v);
        }
        return val;
    }

    private static boolean boolSortKey(BooleanBlock block, int pos, boolean asc) {
        int count = block.getValueCount(pos);
        int first = block.getFirstValueIndex(pos);
        boolean val = block.getBoolean(first);
        for (int i = 1; i < count; i++) {
            boolean v = block.getBoolean(first + i);
            // false < true; for asc: pick false (min); for desc: pick true (max)
            val = asc ? (val && v) : (val || v);
        }
        return val;
    }

    private static BytesRef bytesSortKey(BytesRefBlock block, int pos, boolean asc, BytesRef scratch) {
        int count = block.getValueCount(pos);
        int first = block.getFirstValueIndex(pos);
        // candidate is a private copy so we can mutate scratch freely in the loop.
        BytesRef tmp = block.getBytesRef(first, scratch);
        BytesRef candidate = BytesRef.deepCopyOf(tmp);
        BytesRef loop = new BytesRef();
        for (int i = 1; i < count; i++) {
            BytesRef v = block.getBytesRef(first + i, loop);
            int cmp = candidate.compareTo(v);
            if (asc ? cmp > 0 : cmp < 0) {
                candidate = BytesRef.deepCopyOf(v);
            }
        }
        scratch.bytes = candidate.bytes;
        scratch.offset = candidate.offset;
        scratch.length = candidate.length;
        return scratch;
    }

    // ---- output page builder ----

    /**
     * Builds one output page by draining up to {@code maxPageSize} elements from the K-way heap.
     * If a source's page is exhausted mid-page and no further pages are queued for it yet, the
     * page is returned early so the driver can retry once more data arrives.
     */
    private Page emitPage() {
        Block.Builder[] builders = new Block.Builder[elementTypes.length];
        boolean success = false;
        try {
            for (int i = 0; i < elementTypes.length; i++) {
                builders[i] = elementTypes[i].newBlockBuilder(maxPageSize, blockFactory);
            }

            int count = 0;
            while (count < maxPageSize && heap.isEmpty() == false) {
                int sourceId = heap.peek();
                ArrayDeque<Page> queue = sourceQueues.get(sourceId);
                Page page = queue.peek();
                int pos = sourceCursors.getOrDefault(sourceId, 0);

                for (int ch = 0; ch < elementTypes.length; ch++) {
                    builders[ch].copyFrom(page.getBlock(ch), pos, pos + 1);
                }
                count++;

                // Remove source from heap before changing cursor — must re-insert with updated key.
                heap.poll();
                inHeap.remove(sourceId);

                pos++;
                if (pos >= page.getPositionCount()) {
                    // Current page exhausted.
                    page.releaseBlocks();
                    queue.poll();

                    Page nextPage = queue.peek();
                    if (nextPage != null) {
                        sourceCursors.put(sourceId, 0);
                        heap.add(sourceId);
                        inHeap.add(sourceId);
                    } else {
                        sourceCursors.remove(sourceId);
                        if (isSinkDone(sourceId) == false) {
                            // More pages may arrive later; stop here and return partial page.
                            break;
                        }
                        // Source is truly done — leave it out of the heap.
                    }
                } else {
                    sourceCursors.put(sourceId, pos);
                    heap.add(sourceId);
                    inHeap.add(sourceId);
                }
            }

            if (count == 0) {
                if (exchangeSource.isFinished() || allSourcesExhausted()) {
                    finished = true;
                }
                return null;
            }

            Block[] outputBlocks = new Block[elementTypes.length];
            for (int i = 0; i < elementTypes.length; i++) {
                outputBlocks[i] = builders[i].build();
                builders[i] = null;
            }
            success = true;
            return new Page(count, outputBlocks);
        } finally {
            if (success == false) {
                for (Block.Builder b : builders) {
                    Releasables.closeExpectNoException(b);
                }
            }
        }
    }

    private void releaseBufferedPages() {
        for (ArrayDeque<Page> queue : sourceQueues.values()) {
            for (Page page : queue) {
                page.releaseBlocks();
            }
            queue.clear();
        }
        sourceQueues.clear();
    }
}
