/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * A driver-local context that is shared across operators.
 *
 * Operators in the same driver pipeline are executed in a single threaded fashion. A driver context
 * has a set of mutating methods that can be used to store and share values across these operators,
 * or even outside the Driver. When the Driver is finished, it finishes the context. Finishing the
 * context effectively takes a snapshot of the driver context values so that they can be exposed
 * outside the Driver. The net result of this is that the driver context can be mutated freely,
 * without contention, by the thread executing the pipeline of operators until it is finished.
 * The context must be finished by the thread running the Driver, when the Driver is finished.
 *
 * Releasables can be added and removed to the context by operators in the same driver pipeline.
 * This allows to "transfer ownership" of a shared resource across operators (and even across
 * Drivers), while ensuring that the resource can be correctly released when no longer needed.
 *
 * DriverContext can also be used to track async actions. The driver may close an operator while
 * some of its async actions are still running. To prevent the driver from finishing in this case,
 * methods {@link #addAsyncAction()} and {@link #removeAsyncAction()} are provided for tracking
 * such actions. Subsequently, the driver uses {@link #waitForAsyncActions(ActionListener)} to
 * await the completion of all async actions before finalizing the Driver.
 */
public class DriverContext {

    private static final Logger logger = LogManager.getLogger(DriverContext.class);

    // Working set. Only the thread executing the driver will update this set.
    Set<Releasable> workingSet = Collections.newSetFromMap(new IdentityHashMap<>());

    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>();

    private final BigArrays bigArrays;

    private final BlockFactory blockFactory;

    private final AsyncActions asyncActions = new AsyncActions();

    private final WarningsMode warningsMode;

    private final @Nullable String driverDescription;

    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;

    private Runnable earlyTerminationChecker = () -> {};

    /**
     * Hooks fired when the controlling task asks this driver to wind down cleanly while keeping
     * already-buffered work. Each hook should return {@code true} only when it actually
     * transitioned a source operator from "still accepting input" to "drain and stop" — that signal
     * propagates back through {@link Driver#runStopHooks()} and ultimately gates {@code is_partial}
     * on the response, so hooks that are no-ops at call time (e.g. already-finished buffers) must
     * report {@code false}. {@link CopyOnWriteArrayList} keeps registration cheap during operator
     * construction and tolerates late additions interleaved with concurrent {@link #runStopHooks()}.
     */
    private final List<BooleanSupplier> stopHooks = new CopyOnWriteArrayList<>();

    public DriverContext(BigArrays bigArrays, BlockFactory blockFactory, @Nullable LocalCircuitBreaker.SizeSettings localBreakerSettings) {
        this(bigArrays, blockFactory, localBreakerSettings, null, WarningsMode.COLLECT);
    }

    public DriverContext(
        BigArrays bigArrays,
        BlockFactory blockFactory,
        @Nullable LocalCircuitBreaker.SizeSettings localBreakerSettings,
        String description
    ) {
        this(bigArrays, blockFactory, localBreakerSettings, description, WarningsMode.COLLECT);
    }

    private DriverContext(
        BigArrays bigArrays,
        BlockFactory blockFactory,
        @Nullable LocalCircuitBreaker.SizeSettings localBreakerSettings,
        @Nullable String description,
        WarningsMode warningsMode
    ) {
        Objects.requireNonNull(bigArrays);
        Objects.requireNonNull(blockFactory);
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.localBreakerSettings = localBreakerSettings == null ? LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS : localBreakerSettings;
        this.driverDescription = description;
        this.warningsMode = warningsMode;
    }

    public BigArrays bigArrays() {
        return bigArrays;
    }

    /**
     * The {@link CircuitBreaker} to use to track memory.
     */
    public CircuitBreaker breaker() {
        return blockFactory.breaker();
    }

    public LocalCircuitBreaker.SizeSettings localBreakerSettings() {
        return localBreakerSettings;
    }

    public BlockFactory blockFactory() {
        return blockFactory;
    }

    public BlockFactory createChildBlockFactory() {
        BlockFactory parent = blockFactory.parent();
        final var childBreaker = new LocalCircuitBreaker(
            parent.breaker(),
            localBreakerSettings.overReservedBytes(),
            localBreakerSettings.maxOverReservedBytes()
        );
        return parent.newChildFactory(childBreaker);
    }

    public void releaseChildBlockFactory(BlockFactory childFactory) {
        if (childFactory.breaker() instanceof LocalCircuitBreaker local) {
            local.close();
        }
    }

    /** See {@link Driver#shortDescription}. */
    @Nullable
    public String driverDescription() {
        return driverDescription;
    }

    /** A snapshot of the driver context. */
    public record Snapshot(Set<Releasable> releasables) implements Releasable {
        @Override
        public void close() {
            Releasables.close(releasables);
        }
    }

    /**
     * Adds a releasable to this context. Releasables are identified by Object identity.
     * @return true if the releasable was added, otherwise false (if already present)
     */
    public boolean addReleasable(Releasable releasable) {
        return workingSet.add(releasable);
    }

    /**
     * Removes a releasable from this context. Releasables are identified by Object identity.
     * @return true if the releasable was removed, otherwise false (if not present)
     */
    public boolean removeReleasable(Releasable releasable) {
        return workingSet.remove(releasable);
    }

    /**
     * Retrieves the snapshot of the driver context after it has been finished.
     * @return the snapshot
     */
    public Snapshot getSnapshot() {
        ensureFinished();
        // should be called by the DriverRunner
        return snapshot.get();
    }

    /**
     * Tells whether this context is finished. Can be invoked from any thread.
     */
    public boolean isFinished() {
        return snapshot.get() != null;
    }

    /**
     * Finishes this context. Further mutating operations should not be performed.
     */
    public void finish() {
        if (isFinished()) {
            return;
        }
        // must be called by the thread executing the driver.
        // no more updates to this context.
        asyncActions.finish();
        var itr = workingSet.iterator();
        workingSet = null;
        Set<Releasable> releasableSet = Collections.newSetFromMap(new IdentityHashMap<>());
        while (itr.hasNext()) {
            var r = itr.next();
            releasableSet.add(r);
            itr.remove();
        }
        snapshot.compareAndSet(null, new Snapshot(releasableSet));
    }

    private void ensureFinished() {
        if (isFinished() == false) {
            throw new IllegalStateException("not finished");
        }
    }

    public void waitForAsyncActions(ActionListener<Void> listener) {
        asyncActions.addListener(listener);
    }

    public void addAsyncAction() {
        asyncActions.addInstance();
    }

    public void removeAsyncAction() {
        asyncActions.removeInstance();
    }

    /**
     * Returns true if there are pending async actions registered via {@link #addAsyncAction()}.
     */
    public boolean hasPendingAsyncActions() {
        return asyncActions.instances.get() > 1;
    }

    /**
     * Checks if the Driver associated with this DriverContext has been cancelled or early terminated.
     */
    public void checkForEarlyTermination() {
        earlyTerminationChecker.run();
    }

    /**
     * Initializes the early termination or cancellation checker for this DriverContext.
     * This method should be called when associating this DriverContext with a driver.
     */
    public void initializeEarlyTerminationChecker(Runnable checker) {
        this.earlyTerminationChecker = checker;
    }

    /**
     * Registers a stop hook to be fired when the controlling task requests a clean wind-down of
     * this driver (e.g. async STOP). Used by source operators that hold a buffer between the
     * producer thread and the driver loop — registering a hook that closes the buffer's input side
     * lets STOP cut off the producer without discarding pages already accepted into the buffer.
     * Idempotent: the same hook may be registered multiple times and {@link #runStopHooks()} will
     * fire each registration.
     */
    public void addStopHook(BooleanSupplier hook) {
        stopHooks.add(hook);
    }

    /**
     * Fires all registered stop hooks. Returns {@code true} if at least one hook reported that it
     * cut a still-running unit of work. See {@link #addStopHook(BooleanSupplier)} for the contract
     * each hook must honor.
     */
    public boolean runStopHooks() {
        boolean anyCut = false;
        for (BooleanSupplier hook : stopHooks) {
            try {
                anyCut |= hook.getAsBoolean();
            } catch (Exception e) {
                // Hooks are best-effort signals; a faulty hook must not break the STOP response path
                // or stop the rest of the chain from firing. The only consequence of swallowing here
                // is a slightly weaker partial-marking signal — never lost results. Log at debug so a
                // misbehaving hook is diagnosable in a support bundle rather than silently invisible.
                logger.debug("stop hook threw during runStopHooks; skipping hook", e);
            }
        }
        return anyCut;
    }

    /**
     * Evaluators should use this function to decide their warning behavior.
     * @return an appropriate {@link WarningsMode}
     */
    public WarningsMode warningsMode() {
        return warningsMode;
    }

    /**
     * Create a new {@link Warnings} collector using this context's {@link #warningsMode()}.
     * @see Warnings#createWarnings(WarningsMode, WarningSourceLocation)
     */
    public Warnings createWarnings(WarningSourceLocation source) {
        return Warnings.createWarnings(warningsMode, source);
    }

    /**
     * Create a new {@link Warnings} collector, using this context's {@link #warningsMode()}, that warns
     * that it treats the result as {@code false}.
     * @see Warnings#createWarningsTreatedAsFalse(WarningsMode, WarningSourceLocation)
     */
    public Warnings createWarningsTreatedAsFalse(WarningSourceLocation source) {
        return Warnings.createWarningsTreatedAsFalse(warningsMode, source);
    }

    /**
     * Create a new {@link Warnings} collector, using this context's {@link #warningsMode()}, that warns
     * that evaluation resulted in warnings.
     * @see Warnings#createOnlyWarnings(WarningsMode, WarningSourceLocation)
     */
    public Warnings createOnlyWarnings(WarningSourceLocation source) {
        return Warnings.createOnlyWarnings(warningsMode, source);
    }

    /**
     * Indicates the behavior Evaluators of this context should use for reporting warnings
     */
    public enum WarningsMode {
        COLLECT,
        IGNORE
    }

    /**
     * Marks the beginning of a run loop for assertion purposes.
     */
    public boolean assertBeginRunLoop() {
        if (blockFactory.breaker() instanceof LocalCircuitBreaker localBreaker) {
            assert localBreaker.assertBeginRunLoop();
        }
        return true;
    }

    /**
     * Marks the end of a run loop for assertion purposes.
     */
    public boolean assertEndRunLoop() {
        if (blockFactory.breaker() instanceof LocalCircuitBreaker localBreaker) {
            assert localBreaker.assertEndRunLoop();
        }
        return true;
    }

    private static class AsyncActions {
        private final SubscribableListener<Void> completion = new SubscribableListener<>();
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicInteger instances = new AtomicInteger(1);

        void addInstance() {
            if (finished.get()) {
                throw new IllegalStateException("DriverContext was finished already");
            }
            instances.incrementAndGet();
        }

        void removeInstance() {
            if (instances.decrementAndGet() == 0) {
                completion.onResponse(null);
            }
        }

        void addListener(ActionListener<Void> listener) {
            completion.addListener(listener);
        }

        void finish() {
            if (finished.compareAndSet(false, true)) {
                removeInstance();
            }
        }
    }
}
