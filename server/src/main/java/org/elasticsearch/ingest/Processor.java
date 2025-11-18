/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.Scheduler;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A processor implementation may modify the data belonging to a document.
 * Whether changes are made and what exactly is modified is up to the implementation.
 *
 * Processors may get called concurrently and thus need to be thread-safe.
 */
public interface Processor {

    /**
     * Introspect and potentially modify the incoming data.
     *
     * Expert method: only override this method if a processor implementation needs to make an asynchronous call,
     * otherwise just overwrite {@link #execute(IngestDocument)}.
     */
    default void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        if (isAsync() == false) {
            handler.accept(
                null,
                new UnsupportedOperationException("asynchronous execute method should not be executed for sync processors")
            );
        }
        handler.accept(ingestDocument, null);
    }

    /**
     * Introspect and potentially modify the incoming data.
     *
     * @return If <code>null</code> is returned then the current document will be dropped and not be indexed,
     *         otherwise this document will be kept and indexed
     */
    default IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        if (isAsync()) {
            throw new UnsupportedOperationException("synchronous execute method should not be executed for async processors");
        }
        return ingestDocument;
    }

    /**
     * Gets the type of a processor
     */
    String getType();

    /**
     * Gets the tag of a processor.
     */
    String getTag();

    /**
     * Gets the description of a processor.
     */
    String getDescription();

    default boolean isAsync() {
        return false;
    }

    /**
     * Validate a processor after it has been constructed by a factory.
     *
     * Override this method to perform additional post-construction validation that should be performed at the rest/transport level.
     * If there's an issue with the processor, then indicate that by throwing an exception. See
     * {@link IngestService#validatePipeline(Map, ProjectId, String, Map)}} for the call site where there is invoked in a try/catch.
     *
     * An example of where this would be needed is a processor that interacts with external state like the license state -- it may
     * be okay to create that processor on day 1 with license state A, but later illegal to create a similar processor on day 2 with
     * state B. We want to reject put requests on day 2 (at the rest/transport level), but still allow for restarting nodes in the
     * cluster (so we can't throw exceptions from {@link Factory#create(Map, String, String, Map, ProjectId)}).
     */
    default void extraValidation() throws Exception {}

    /**
     * A factory that knows how to construct a processor based on a map of maps.
     */
    interface Factory {

        /**
         * Creates a processor based on the specified map of maps config.
         *
         * @param processorFactories Other processors which may be created inside this processor
         * @param tag The tag for the processor
         * @param description A short description of what this processor does
         * @param config The configuration for the processor
         * @param projectId The project for which the processor is created
         *
         * <b>Note:</b> Implementations are responsible for removing the used configuration keys, so that after
         */
        Processor create(
            Map<String, Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception;
    }

    /**
     * Infrastructure class that holds services that can be used by processor factories to create processor instances
     * and that gets passed around to all {@link org.elasticsearch.plugins.IngestPlugin}s.
     */
    class Parameters {

        /**
         * Useful to provide access to the node's environment like config directory to processor factories.
         */
        public final Environment env;

        /**
         * Provides processors script support.
         */
        public final ScriptService scriptService;

        /**
         * Provide analyzer support
         */
        public final AnalysisRegistry analysisRegistry;

        /**
         * Allows processors to read headers set by {@link org.elasticsearch.action.support.ActionFilter}
         * instances that have run prior to in ingest.
         */
        public final ThreadContext threadContext;

        public final LongSupplier relativeTimeSupplier;

        public final IngestService ingestService;

        public final Consumer<Runnable> genericExecutor;

        /**
         * Provides scheduler support
         */
        public final BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler;

        /**
         * Provides access to the node client
         */
        public final Client client;

        public final MatcherWatchdog matcherWatchdog;

        public Parameters(
            Environment env,
            ScriptService scriptService,
            AnalysisRegistry analysisRegistry,
            ThreadContext threadContext,
            LongSupplier relativeTimeSupplier,
            BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler,
            IngestService ingestService,
            Client client,
            Consumer<Runnable> genericExecutor,
            MatcherWatchdog matcherWatchdog
        ) {
            this.env = env;
            this.scriptService = scriptService;
            this.threadContext = threadContext;
            this.analysisRegistry = analysisRegistry;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.scheduler = scheduler;
            this.ingestService = ingestService;
            this.client = client;
            this.genericExecutor = genericExecutor;
            this.matcherWatchdog = matcherWatchdog;
        }
    }
}
