/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.compute.operator.PageStreamPublisher;

import java.util.List;

/**
 * Action type for the streaming ES|QL query endpoint ({@code POST /_query/stream}).
 * Unlike {@link EsqlQueryAction}, the response type is {@link ActionResponse.Empty}: the action
 * completes only when compute finishes, so the transport task stays registered and cancellable for
 * the full duration of the query.
 *
 * <p>The schema and publisher are delivered out-of-band through
 * {@link EsqlStreamQueryRequest#streamStartListener()}, which the REST layer sets before dispatching.
 * This keeps {@link org.elasticsearch.rest.action.RestCancellableNodeClient} working unmodified:
 * its close-set entry for the task survives until {@link ActionResponse.Empty} is returned, so a
 * client disconnect issues a task cancellation and {@code isCancelled()} flips correctly.
 */
public class EsqlStreamQueryAction extends ActionType<ActionResponse.Empty> {

    public static final EsqlStreamQueryAction INSTANCE = new EsqlStreamQueryAction();
    public static final String NAME = "indices:data/read/esql/stream";

    private EsqlStreamQueryAction() {
        super(NAME);
    }

    /**
     * The out-of-band payload delivered to the REST layer once analysis is complete and compute
     * is about to start. Carries the same three values that the old {@code Response} carried, but
     * is signalled through {@link EsqlStreamQueryRequest#streamStartListener()} rather than through
     * the transport action's response path.
     */
    public record StreamStart(List<ColumnInfoImpl> columns, PageStreamPublisher publisher, boolean[] nullColumns) {}
}
