/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Sink operator that accumulates rows to a fixed page size and stores each output page
 * to the cursor index incrementally. Page stores run concurrently up to
 * {@link #MAX_IN_FLIGHT_STORES}; the Driver is only blocked when that limit is reached
 * or during the final metadata update at {@link #finish()}.
 * <p>
 * Page 0 is captured via {@link Page#shallowCopy()} into the {@link PaginationStoreProvider}
 * immediately when produced, firing the {@link PaginationStoreProvider#firstPageListener()}.
 * This allows the query response to be sent without waiting for any index operation.
 * All stores (metadata, pages, finalization) happen entirely in the background.
 * <p>
 * Storage failures fail the entire query by resolving the blocked listener with an exception,
 * which the Driver then propagates.
 */
public class StorePageOperator extends SinkOperator {

    private static final Logger logger = LogManager.getLogger(StorePageOperator.class);

    static final int MAX_IN_FLIGHT_STORES = 3;

    private final PaginationStoreProvider provider;
    private final PaginationContext paginationContext;
    private final EsqlCursorIndexService cursorIndexService;
    private final Function<Page, Page> mapper;
    private final int pageSize;

    private final List<Page> accumulator = new ArrayList<>();
    private int accumulatedRows = 0;
    private int nextPageIndex = 0;
    private boolean finishing = false;
    private volatile boolean finished = false;
    private boolean metadataCreated = false;

    private final AtomicInteger inFlightStores = new AtomicInteger();
    private volatile Exception storageFailure;

    private final Object blockLock = new Object();
    /** Backpressure listener created when in-flight stores reach {@link #MAX_IN_FLIGHT_STORES}. Guarded by {@link #blockLock}. */
    private SubscribableListener<Void> blockedFuture;

    /**
     * Listener that is resolved when all in-flight stores have completed.
     * Set by {@link #finish()} when it needs to wait for background stores.
     */
    private volatile SubscribableListener<Void> drainListener;

    /** Listener that is resolved when the metadata finalize operation completes. */
    private volatile SubscribableListener<Void> finalizeListener;

    public StorePageOperator(PaginationStoreProvider provider, Function<Page, Page> mapper) {
        this.provider = provider;
        this.paginationContext = provider.paginationContext();
        this.cursorIndexService = provider.cursorIndexService();
        this.mapper = mapper;
        this.pageSize = paginationContext.pageSize();
    }

    @Override
    public boolean isFinished() {
        if (finished) {
            return true;
        }
        if (storageFailure != null) {
            finished = true;
            return true;
        }
        return false;
    }

    @Override
    public void finish() {
        if (finishing || finished) {
            return;
        }
        finishing = true;
        while (accumulatedRows >= pageSize) {
            List<Page> outputPage = sliceNextOutputPage();
            dispatchStore(outputPage, false);
        }
        if (accumulatedRows > 0) {
            flushAccumulator();
        }
        if (nextPageIndex == 0) {
            provider.signalNoPagesProduced();
        }
        provider.setTotalPages(nextPageIndex);
        waitForInFlightAndFinalize();
    }

    @Override
    public boolean needsInput() {
        return finishing == false && finished == false && isBlocked().listener().isDone();
    }

    @Override
    public IsBlockedResult isBlocked() {
        Exception failure = storageFailure;
        if (failure != null) {
            SubscribableListener<Void> failListener = new SubscribableListener<>();
            failListener.onFailure(failure);
            return new IsBlockedResult(failListener, "storage failure");
        }
        SubscribableListener<Void> fl = finalizeListener;
        if (fl != null) {
            return new IsBlockedResult(fl, "finalizing metadata");
        }
        SubscribableListener<Void> dl = drainListener;
        if (dl != null) {
            return new IsBlockedResult(dl, "waiting for in-flight stores");
        }
        if (finished || inFlightStores.get() < MAX_IN_FLIGHT_STORES) {
            return Operator.NOT_BLOCKED;
        }
        synchronized (blockLock) {
            if (finished || inFlightStores.get() < MAX_IN_FLIGHT_STORES) {
                return Operator.NOT_BLOCKED;
            }
            if (blockedFuture == null) {
                blockedFuture = new SubscribableListener<>();
            }
            return new IsBlockedResult(blockedFuture, "backpressure");
        }
    }

    @Override
    protected void doAddInput(Page page) {
        Page mapped = mapper.apply(page);
        provider.addTotalRows(mapped.getPositionCount());
        accumulator.add(mapped);
        accumulatedRows += mapped.getPositionCount();

        while (accumulatedRows >= pageSize) {
            if (inFlightStores.get() >= MAX_IN_FLIGHT_STORES || storageFailure != null) {
                break;
            }
            List<Page> outputPage = sliceNextOutputPage();
            dispatchStore(outputPage, false);
        }
    }

    private List<Page> sliceNextOutputPage() {
        List<Page> outputPage = PageUtils.slicePages(accumulator, 0, pageSize);
        dropAccumulatedRows(pageSize);
        return outputPage;
    }

    /**
     * Drops the first {@code rowsToDrop} rows from the accumulator, releasing pages
     * that are fully consumed.
     */
    private void dropAccumulatedRows(int rowsToDrop) {
        int dropped = 0;
        while (dropped < rowsToDrop && accumulator.isEmpty() == false) {
            Page first = accumulator.get(0);
            int pageRows = first.getPositionCount();
            if (dropped + pageRows <= rowsToDrop) {
                accumulator.remove(0);
                first.releaseBlocks();
                dropped += pageRows;
            } else {
                int remaining = pageRows - (rowsToDrop - dropped);
                int startPos = pageRows - remaining;
                int[] positions = new int[remaining];
                for (int i = 0; i < remaining; i++) {
                    positions[i] = startPos + i;
                }
                Page filtered = first.shallowCopy().filter(false, positions);
                accumulator.set(0, filtered);
                first.releaseBlocks();
                dropped = rowsToDrop;
            }
        }
        accumulatedRows -= rowsToDrop;
    }

    private void flushAccumulator() {
        List<Page> outputPage = new ArrayList<>(accumulator);
        accumulator.clear();
        accumulatedRows = 0;
        dispatchStore(outputPage, true);
    }

    /**
     * Dispatches a page store. The first page also triggers metadata creation.
     * Stores run concurrently; backpressure is applied via {@link #isBlocked()}.
     *
     * @param needsDeepCopy {@code true} for raw accumulator pages that may contain block types
     *        requiring deep copy before serialization; {@code false} for pages already
     *        processed through {@link Page#filter} which produce standard array-backed blocks.
     */
    private void dispatchStore(List<Page> outputPage, boolean needsDeepCopy) {
        int pageIndex = nextPageIndex++;

        if (pageIndex == 0) {
            List<Page> copy = outputPage.stream().map(Page::shallowCopy).toList();
            provider.setFirstPage(copy);
        }

        inFlightStores.incrementAndGet();

        if (metadataCreated == false) {
            metadataCreated = true;
            storeMetadataAndPage(pageIndex, outputPage, needsDeepCopy);
        } else {
            storePage(pageIndex, outputPage, needsDeepCopy);
        }
    }

    /**
     * Creates the incomplete metadata doc, then stores the page. Both are sequential
     * to guarantee that metadata exists before any page is visible.
     */
    private void storeMetadataAndPage(int pageIndex, List<Page> outputPage, boolean needsDeepCopy) {
        cursorIndexService.storeMetadataIncomplete(
            paginationContext.cursorId(),
            provider.columns(),
            paginationContext.expirationMillis(),
            paginationContext.zoneId(),
            paginationContext.columnar(),
            provider.backgroundTaskId(),
            paginationContext.securityHeaders(),
            ActionListener.wrap(cursorId -> {
                storePage(pageIndex, outputPage, needsDeepCopy);
            }, e -> {
                releasePages(outputPage);
                onStoreFailure(e, "metadata", -1);
            })
        );
    }

    private void storePage(int pageIndex, List<Page> outputPage, boolean needsDeepCopy) {
        cursorIndexService.storePage(
            paginationContext.cursorId(),
            pageIndex,
            outputPage,
            needsDeepCopy,
            ActionListener.wrap(v -> onStoreComplete(), e -> onStoreFailure(e, "page", pageIndex))
        );
        releasePages(outputPage);
    }

    private void onStoreComplete() {
        int remaining = inFlightStores.decrementAndGet();
        SubscribableListener<Void> drain = drainListener;
        if (drain != null && remaining == 0) {
            drain.onResponse(null);
        }
        notifyUnblocked();
    }

    private void onStoreFailure(Exception e, String what, int pageIndex) {
        if (pageIndex >= 0) {
            logger.warn("failed to store {} [{}] for cursor [{}]", what, pageIndex, paginationContext.cursorId(), e);
        } else {
            logger.warn("failed to store {} for cursor [{}]", what, paginationContext.cursorId(), e);
        }
        storageFailure = e;
        int remaining = inFlightStores.decrementAndGet();
        SubscribableListener<Void> drain = drainListener;
        if (drain != null && remaining == 0) {
            drain.onResponse(null);
        }
        notifyUnblocked();
    }

    /**
     * Wakes the Driver if it is blocked on backpressure. Uses the same synchronized
     * read-and-null pattern as ExchangeBuffer to avoid racing with {@link #isBlocked()}.
     */
    private void notifyUnblocked() {
        final SubscribableListener<Void> toNotify;
        synchronized (blockLock) {
            toNotify = blockedFuture;
            blockedFuture = null;
        }
        if (toNotify != null) {
            toNotify.onResponse(null);
        }
    }

    /**
     * Waits for all in-flight stores to complete, then finalizes metadata.
     * If there are no in-flight stores, proceeds immediately.
     */
    private void waitForInFlightAndFinalize() {
        if (inFlightStores.get() == 0) {
            doFinalize();
            return;
        }
        SubscribableListener<Void> drain = new SubscribableListener<>();
        drainListener = drain;
        if (inFlightStores.get() == 0) {
            drain.onResponse(null);
        }
        drain.addListener(ActionListener.wrap(v -> doFinalize(), e -> doFinalize()));
    }

    private void doFinalize() {
        drainListener = null;
        if (storageFailure != null) {
            finished = true;
            return;
        }
        if (metadataCreated) {
            finalizeMetadata();
        } else {
            finished = true;
        }
    }

    private void finalizeMetadata() {
        SubscribableListener<Void> listener = new SubscribableListener<>();
        finalizeListener = listener;

        cursorIndexService.updateMetadataComplete(
            paginationContext.cursorId(),
            provider.totalRows(),
            provider.totalPages(),
            ActionListener.wrap(v -> {
                finished = true;
                listener.onResponse(null);
            }, e -> {
                logger.warn("failed to finalize metadata for cursor [{}]", paginationContext.cursorId(), e);
                finished = true;
                listener.onFailure(e);
            })
        );
    }

    private static void releasePages(List<Page> pages) {
        for (Page p : pages) {
            p.releaseBlocks();
        }
    }

    @Override
    public void close() {
        releasePages(accumulator);
        accumulator.clear();
    }

    @Override
    public String toString() {
        return "StorePageOperator[pageSize=" + pageSize + "]";
    }

    public record StorePageOperatorFactory(PaginationStoreProvider provider, Function<Page, Page> mapper) implements SinkOperatorFactory {

        @Override
        public SinkOperator get(DriverContext driverContext) {
            return new StorePageOperator(provider, mapper);
        }

        @Override
        public String describe() {
            return "StorePageOperator[pageSize=" + provider.paginationContext().pageSize() + "]";
        }
    }
}
