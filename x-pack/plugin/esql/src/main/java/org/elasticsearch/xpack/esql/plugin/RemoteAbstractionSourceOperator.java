/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

/**
 * The per-leaf source operator a {@code RemoteViewExec}/{@code RemoteDatasetExec} lowers to. A remote-abstraction leaf is
 * a data <em>source</em> sitting where a scan would be, but its rows live on another cluster; this operator owns a private
 * {@link ExchangeSourceHandler}, dispatches the abstraction to its home cluster through {@link FederationExecutionService},
 * and behaves as an exchange-source operator draining the pages the home cluster sinks back.
 *
 * <p>It does <b>not</b> reuse the plan's single main exchange source: {@code LocalExecutionPlanner} owns exactly one main
 * exchange source (the plan's result funnel), so a leaf that drained it would corrupt the coordinator's result stream. The
 * leaf owns its own handler instead — the engine-question conclusion that makes N remote-abstraction leaves coexist in one
 * plan, each with its own child session.
 *
 * <p><b>Once-per-leaf dispatch.</b> The single {@link Factory} for a leaf creates the one {@link ExchangeSourceHandler} and
 * fires the one {@link FederationExecutionService#fetchAbstraction} lazily, on first {@link Factory#get} — not per operator
 * instance. Under {@code DriverParallelism > 1} the planner asks the factory for several operators; every one of them only
 * {@link ExchangeSourceHandler#createExchangeSource}s from the already-dispatched handler. A second dispatch would be a
 * second full remote execution, so the factory — not the operator — owns the single call, exactly as
 * {@code ExchangeSourceOperator}'s shared handler tolerates N instances off one external source.
 *
 * <p>Each operator delegates {@code getOutput}/{@code isFinished}/{@code isBlocked}/{@code finish} to an inner
 * {@link ExchangeSourceOperator} over its own {@link ExchangeSourceHandler#createExchangeSource}, so the exchange-draining
 * behaviour is reused verbatim rather than reinvented.
 */
public final class RemoteAbstractionSourceOperator {

    private RemoteAbstractionSourceOperator() {}

    /**
     * The leaf's operator factory. Owns the leaf-scoped {@link ExchangeSourceHandler} and the single dispatch; each
     * {@link #get} hands out an {@link ExchangeSourceOperator} over a fresh source view of that one handler.
     */
    public static final class Factory implements SourceOperator.SourceOperatorFactory {

        private final FederationExecutionService federationExecutionService;
        private final String sessionId;
        private final CancellableTask parentTask;
        private final Configuration configuration;
        private final String handle;
        private final String abstractionName;
        private final List<Attribute> expectedAttributes;
        private final int bufferSize;

        // The one leaf-scoped source handler + one dispatch, created lazily and shared by every operator instance. Guarded
        // by the factory monitor so the first get() from any driver thread wins the single dispatch; subsequent gets see
        // the already-dispatched handler. Never dispatched more than once (a second dispatch = a second remote execution).
        private ExchangeSourceHandler leafExchangeSource;

        public Factory(
            FederationExecutionService federationExecutionService,
            String sessionId,
            CancellableTask parentTask,
            Configuration configuration,
            String handle,
            String abstractionName,
            List<Attribute> expectedAttributes,
            int bufferSize
        ) {
            this.federationExecutionService = federationExecutionService;
            this.sessionId = sessionId;
            this.parentTask = parentTask;
            this.configuration = configuration;
            this.handle = handle;
            this.abstractionName = abstractionName;
            this.expectedAttributes = expectedAttributes;
            this.bufferSize = bufferSize;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new ExchangeSourceOperator(dispatchedSource().createExchangeSource());
        }

        /**
         * Returns the one leaf-scoped {@link ExchangeSourceHandler}, dispatching the abstraction into it exactly once on
         * first call. The dispatch failure — if any — is surfaced through the exchange source itself: a home-side failure
         * finishes the handler with the failure (see {@link FederationExecutionService#fetchAbstraction}), which the
         * draining {@link ExchangeSourceOperator} observes as the drain terminating exceptionally.
         */
        private synchronized ExchangeSourceHandler dispatchedSource() {
            if (leafExchangeSource == null) {
                ExchangeSourceHandler source = federationExecutionService.newLeafExchangeSource(bufferSize);
                federationExecutionService.fetchAbstraction(
                    sessionId,
                    parentTask,
                    configuration,
                    handle,
                    abstractionName,
                    expectedAttributes,
                    source,
                    // The leaf's correctness is the drained pages; the completion is the drain-finished signal. On failure
                    // the service has already finished the source with the failure, so the operator's drain terminates
                    // loud — nothing extra to do here.
                    ActionListener.noop()
                );
                leafExchangeSource = source;
            }
            return leafExchangeSource;
        }

        @Override
        public String describe() {
            return "RemoteAbstractionSourceOperator[name=" + abstractionName + ", handle=" + handle + "]";
        }
    }
}
