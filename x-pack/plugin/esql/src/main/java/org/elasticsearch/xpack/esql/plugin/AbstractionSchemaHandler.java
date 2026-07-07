/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * The home-cluster receiver of the SCHEMA half of federation execution — the sibling of {@link AbstractionComputeHandler}
 * that runs on the same node but returns only the resolved output schema, no rows. The coordinator dispatches a
 * {@link ResolveAbstractionSchemaRequest} during resolution (before it can build the {@code Boundary.REMOTE} leaf), the
 * home cluster resolves the name through its OWN kind-blind umbrella and returns the real output attributes, and the
 * coordinator uses them for the leaf's {@code output()}.
 *
 * <p>Registers the {@code indices:data/read/esql/resolve_abstraction_schema} transport handler in its constructor, exactly
 * as {@link AbstractionComputeHandler} registers {@code execute_abstraction}. The resolution itself is delegated to
 * {@code TransportEsqlQueryAction.resolveAbstractionSchema} (passed as {@code resolver}) so the full
 * {@code planExecutor.esql(...)} assembly lives in one place — the same reason the execution handler delegates.
 */
final class AbstractionSchemaHandler implements TransportRequestHandler<ResolveAbstractionSchemaRequest> {

    /** {@code "indices:data/read/esql/resolve_abstraction_schema"} — {@code indices:}-scoped, sibling of {@code .../execute_abstraction}. */
    static final String RESOLVE_ABSTRACTION_SCHEMA_ACTION_NAME = EsqlQueryAction.NAME + "/resolve_abstraction_schema";

    /** Resolves an abstraction name to its real output attributes without executing (schema-capturing runner). */
    private final BiConsumer<AbstractionSchemaResolution, ActionListener<List<Attribute>>> resolver;

    /** The (name, parentTask) pair a resolution needs. */
    record AbstractionSchemaResolution(String abstractionName, CancellableTask parentTask) {}

    AbstractionSchemaHandler(
        TransportService transportService,
        Executor searchExecutor,
        BiConsumer<AbstractionSchemaResolution, ActionListener<List<Attribute>>> resolver
    ) {
        this.resolver = resolver;
        transportService.registerRequestHandler(
            RESOLVE_ABSTRACTION_SCHEMA_ACTION_NAME,
            searchExecutor,
            ResolveAbstractionSchemaRequest::new,
            this
        );
    }

    @Override
    public void messageReceived(ResolveAbstractionSchemaRequest request, TransportChannel channel, Task task) {
        ChannelActionListener<ResolveAbstractionSchemaResponse> listener = new ChannelActionListener<>(channel);
        resolver.accept(
            new AbstractionSchemaResolution(request.abstractionName(), (CancellableTask) task),
            listener.map(ResolveAbstractionSchemaResponse::new)
        );
    }
}
