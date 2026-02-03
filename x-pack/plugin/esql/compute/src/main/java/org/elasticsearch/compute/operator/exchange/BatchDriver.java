/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * Driver that processes batches on the server side of a BidirectionalBatchExchange.
 * Extends Driver to handle batch-specific logic.
 *
 * <p>This driver:
 * <ul>
 *   <li>Reads pages from the client-to-server exchange</li>
 *   <li>Executes operators (from factories) on those pages</li>
 *   <li>Writes results to the server-to-client exchange</li>
 *   <li>Detects batch boundaries from BatchPage</li>
 *   <li>Tracks batch state via {@link BatchContext}</li>
 * </ul>
 */
public final class BatchDriver extends Driver {
    private static final Logger logger = LogManager.getLogger(BatchDriver.class);

    private final BatchContext batchContext;
    private final PageToBatchPageOperator wrappedSink;
    private final BatchDoneNotifier batchDoneNotifier = new BatchDoneNotifier();

    public BatchDriver(
        String sessionId,
        String shortDescription,
        String clusterName,
        String nodeName,
        long startTime,
        long startNanos,
        DriverContext driverContext,
        Supplier<String> description,
        ExchangeSourceOperator source,
        List<Operator> intermediateOperators,
        SinkOperator sink,
        TimeValue statusInterval,
        Releasable releasable
    ) {
        super(
            sessionId,
            shortDescription,
            clusterName,
            nodeName,
            startTime,
            startNanos,
            driverContext,
            description,
            wrapSource(source),
            intermediateOperators,
            sink,
            statusInterval,
            releasable
        );

        // Create the batch context
        this.batchContext = new BatchContext();

        // Set up the wrapped source operator
        if (activeOperators.isEmpty()) {
            throw new IllegalStateException("BatchDriver requires at least one operator (source operator)");
        }
        Operator firstOperator = activeOperators.get(0);
        if (firstOperator instanceof WrappedSourceOperator wrapped) {
            wrapped.setDriver(this);
        } else {
            throw new IllegalStateException(
                "BatchDriver requires the first operator to be a WrappedSourceOperator, but got: " + firstOperator.getClass().getName()
            );
        }

        // Set up the wrapped sink operator
        if (sink instanceof PageToBatchPageOperator sinkOp) {
            this.wrappedSink = sinkOp;
            sinkOp.setBatchContext(batchContext);
        } else {
            throw new IllegalStateException(
                "BatchDriver requires the sink to be a PageToBatchPageOperator (use BatchDriver.wrapSink()), but got: "
                    + sink.getClass().getName()
            );
        }
    }

    private static SourceOperator wrapSource(ExchangeSourceOperator source) {
        return new WrappedSourceOperator(source);
    }

    /**
     * Wraps a sink operator to convert Pages to BatchPages.
     */
    public static PageToBatchPageOperator wrapSink(SinkOperator sink) {
        return new PageToBatchPageOperator(sink);
    }

    /**
     * Get the batch context.
     */
    public BatchContext getBatchContext() {
        return batchContext;
    }

    /**
     * Get the notifier for batch completion events.
     */
    public BatchDoneNotifier onBatchDone() {
        return batchDoneNotifier;
    }

    /**
     * Get the current batch ID.
     */
    public long getBatchId() {
        return batchContext.getBatchId();
    }

    @Override
    protected void onNoPagesMoved() {
        // logger.trace(
        // "[BatchDriver] onNoPagesMoved called: state={}, activeOperators={}, hasSinkBuffer={}",
        // batchContext.getState(),
        // activeOperators.size(),
        // wrappedSink != null
        // );

        // Only complete batch when in DRAINING state
        if (batchContext.getState() != BatchContext.BatchState.DRAINING) {
            // logger.trace("[BatchDriver] Not in DRAINING state, returning early");
            return;
        }

        // Check if any operator can still produce data
        for (Operator operator : activeOperators) {
            if (operator.canProduceMoreDataWithoutExtraInput()) {
                // logger.trace("[BatchDriver] Operator {} can produce more data - waiting", operator);
                return;
            }
        }

        // Pipeline is drained - complete the batch
        if (activeOperators.isEmpty()) {
            // logger.debug("[BatchDriver] activeOperators is empty, completing batch");
            completeBatch("driver finished");
            return;
        }

        Operator sourceOp = activeOperators.get(0);
        if (sourceOp instanceof SourceOperator source) {
            boolean isFinished = source.isFinished();
            boolean isBlocked = source.isBlocked().listener().isDone() == false;
            // logger.trace("[BatchDriver] Source state: isFinished={}, isBlocked={}", isFinished, isBlocked);
            if (isFinished || isBlocked) {
                completeBatch("source blocked/finished");
            } else {
                completeBatch("pipeline drained, normal completion");
            }
        } else {
            // logger.debug("[BatchDriver] First operator is not a SourceOperator: {}", sourceOp.getClass().getSimpleName());
        }
    }

    private void completeBatch(String reason) {
        long batchId = batchContext.getBatchId();
        // logger.debug("[BatchDriver] Completing batch {} ({})", batchId, reason);

        // Flush the sink buffer
        wrappedSink.flushBatch();

        // Transition to IDLE
        batchContext.endBatch();

        // Notify listeners
        batchDoneNotifier.notifyBatchDone(batchId);

        // logger.debug("[BatchDriver] Batch {} complete, state is now {}", batchId, batchContext.getState());
    }

    Page unwrapBatchPage(BatchPage batchPage) {
        if (batchPage.isBatchMarkerOnly()) {
            batchPage.releaseBlocks();
            return null;
        }
        if (batchPage.getBlockCount() > 0) {
            Block[] blocks = new Block[batchPage.getBlockCount()];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = batchPage.getBlock(i);
                blocks[i].incRef();
            }
            Page unwrappedPage = new Page(blocks);
            batchPage.releaseBlocks();
            return unwrappedPage;
        }
        batchPage.releaseBlocks();
        return null;
    }

    private static class WrappedSourceOperator extends SourceOperator {
        private final ExchangeSourceOperator delegate;
        private BatchDriver driver;

        WrappedSourceOperator(ExchangeSourceOperator delegate) {
            this.delegate = delegate;
        }

        void setDriver(BatchDriver driver) {
            this.driver = driver;
        }

        @Override
        public Page getOutput() {
            // Don't poll new pages while draining
            if (driver.batchContext.getState() == BatchContext.BatchState.DRAINING) {
                return null;
            }

            Page page = delegate.getOutput();
            if (page == null) {
                return null;
            }

            if ((page instanceof BatchPage) == false) {
                page.releaseBlocks();
                throw new IllegalArgumentException(
                    Strings.format("BatchDriver only accepts BatchPage, but received: %s", page.getClass().getName())
                );
            }

            BatchPage batchPage = (BatchPage) page;
            long pageBatchId = batchPage.batchId();
            BatchContext ctx = driver.batchContext;

            // Handle state transitions based on current state
            switch (ctx.getState()) {
                case NOT_STARTED, IDLE -> {
                    // First page of a new batch
                    ctx.startBatch(pageBatchId);
                }
                case ACTIVE -> {
                    // Verify batch ID matches
                    if (pageBatchId != ctx.getBatchId()) {
                        batchPage.releaseBlocks();
                        throw new IllegalStateException(
                            Strings.format("Received page for batch %d but currently processing batch %d", pageBatchId, ctx.getBatchId())
                        );
                    }
                }
                case DRAINING -> {
                    // Should not reach here - we return null above
                    batchPage.releaseBlocks();
                    throw new IllegalStateException("Received page while in DRAINING state");
                }
            }

            // Check if this is the last page in the batch
            if (batchPage.isLastPageInBatch()) {
                ctx.startDraining();
            }

            return driver.unwrapBatchPage(batchPage);
        }

        @Override
        public boolean isFinished() {
            // Don't report finished while we're still processing a batch.
            // This prevents the driver from finishing early before the batch marker is sent.
            if (driver.batchContext.isBatchActive()) {
                return false;
            }
            return delegate.isFinished();
        }

        @Override
        public void finish() {
            delegate.finish();
        }

        @Override
        public IsBlockedResult isBlocked() {
            return delegate.isBlocked();
        }

        @Override
        public boolean canProduceMoreDataWithoutExtraInput() {
            // Can't produce more if we're draining
            if (driver.batchContext.getState() == BatchContext.BatchState.DRAINING) {
                return false;
            }
            return delegate.canProduceMoreDataWithoutExtraInput();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * Notifier for batch completion events.
     */
    public static class BatchDoneNotifier {
        private final List<ActionListener<Long>> listeners = new CopyOnWriteArrayList<>();

        public void addListener(ActionListener<Long> listener) {
            listeners.add(listener);
        }

        void notifyBatchDone(long batchId) {
            for (ActionListener<Long> listener : listeners) {
                try {
                    listener.onResponse(batchId);
                } catch (Exception e) {
                    logger.error("[BatchDriver] Error notifying batch done listener", e);
                    throw new RuntimeException("Failed to notify batch done listener", e);
                }
            }
        }
    }
}
