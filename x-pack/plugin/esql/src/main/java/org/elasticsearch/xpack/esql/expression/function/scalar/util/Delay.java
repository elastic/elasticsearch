/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.util;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Build;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Slowdown function - for debug purposes only.
 * Syntax: WAIT(ms) - will sleep for ms milliseconds.
 */
public class Delay extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Delay", Delay::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Delay.class).unary(Delay::new).name("delay");

    @FunctionInfo(
        returnType = { "boolean" },
        briefSummary = "Sleeps for a duration for every row, for debug purposes only.",
        description = "Sleeps for a duration for every row. For debug purposes only."
    )
    public Delay(Source source, @Param(name = "ms", type = { "time_duration" }, description = "For how long") Expression ms) {
        super(source, ms);
    }

    private Delay(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Delay(source(), newChildren.getFirst());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isType(field(), t -> t == DataType.TIME_DURATION, sourceText(), FIRST, "time_duration");
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Delay::new, field());
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public Object fold(FoldContext ctx) {
        return null;
    }

    private long msValue(FoldContext ctx) {
        if (field().foldable() == false) {
            throw new IllegalArgumentException("function [" + sourceText() + "] has invalid argument [" + field().sourceText() + "]");
        }
        var ms = field().fold(ctx);
        if (ms instanceof Duration duration) {
            return duration.toMillis();
        }
        return ((Number) ms).longValue();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        return context -> new DelayEvaluator(context, msValue(toEvaluator.foldCtx()));
    }

    static final class DelayEvaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DelayEvaluator.class);

        /**
         * Upper bound on how long the sleep can ignore a cancelled/stopped query. The requested delay is
         * slept in slices no longer than this so that {@link DriverContext#checkForEarlyTermination()} is
         * polled at least this often; without it a single long {@link Thread#sleep} would pin the driver's
         * worker thread for the whole duration and cancellation could only be observed once it returned.
         */
        static final long CANCELLATION_CHECK_INTERVAL_MS = 100;

        private final DriverContext driverContext;
        private final long ms;

        /**
         * Flipped by the {@link DriverContext#addStopHook stop hook} below when the user requests async STOP.
         * Hard cancel and the exchange-sink-close early-finish are observed through
         * {@link DriverContext#checkForEarlyTermination()}, but async STOP winds a query down by firing stop hooks
         * (and closing the exchange source) without setting the driver's cancel/early-finished flag on a coordinator
         * pipeline driver — its last operator is an {@code OutputOperator}, not an exchange sink, so {@code finishEarly}
         * cannot be used to wind it down cleanly. Instead the sleep stops early (see {@link #delay(long)}), letting the
         * in-flight row flow through and the pipeline drain to natural completion, matching how EXTERNAL STOP behaves.
         */
        private final AtomicBoolean stopRequested = new AtomicBoolean();

        DelayEvaluator(DriverContext driverContext, long ms) {
            if (Build.current().isSnapshot() == false) {
                throw new IllegalArgumentException("Delay function is only available in snapshot builds");
            }
            this.driverContext = driverContext;
            this.ms = ms;
            // Returning true the first time reports that STOP cut a running unit of work, which marks the response partial.
            driverContext.addStopHook(() -> stopRequested.compareAndSet(false, true));
        }

        @Override
        public Block eval(Page page) {
            int positionCount = page.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                delay(ms);
            }
            return driverContext.blockFactory().newConstantBooleanBlockWith(true, positionCount);
        }

        private void delay(long ms) {
            long remaining = ms;
            while (remaining > 0) {
                // Cancellation is cooperative and the sleep is not interruptible, so poll between slices.
                driverContext.checkForEarlyTermination(); // hard cancel + exchange-sink-close early finish
                if (stopRequested.get()) {
                    // Async STOP: stop waiting and let the in-flight row flow through so the pipeline drains normally.
                    return;
                }
                long slice = Math.min(remaining, CANCELLATION_CHECK_INTERVAL_MS);
                try {
                    Thread.sleep(slice);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                remaining -= slice;
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }

        @Override
        public void close() {

        }
    }
}
