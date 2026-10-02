/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.PageStreamPublisher;

import java.io.IOException;
import java.util.function.Consumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A local-only wrapper around {@link EsqlQueryRequest} that carries a
 * {@link EsqlStreamQueryAction.ResultStream} listener for the streaming endpoint.
 *
 * The listener is called once analysis is complete and compute is about to start, delivering
 * the schema and publisher to the REST layer before the transport action's own response arrives.
 * This keeps the transport task alive for the full duration of compute, so
 * {@link org.elasticsearch.rest.action.RestCancellableNodeClient} and task cancellation work correctly.
 */
public class EsqlStreamQueryRequest extends EsqlQueryRequest {

    private final ActionListener<EsqlStreamQueryAction.ResultStream> resultStreamListener;
    private final boolean dropNullColumns;
    private final int batchSize;
    private final Consumer<PageStreamPublisher.StreamFooter> preHeaderFailureFooterConsumer;

    EsqlStreamQueryRequest(
        EsqlQueryRequest source,
        ActionListener<EsqlStreamQueryAction.ResultStream> resultStreamListener,
        boolean dropNullColumns,
        int batchSize
    ) {
        this(source, resultStreamListener, dropNullColumns, batchSize, footer -> {});
    }

    EsqlStreamQueryRequest(
        EsqlQueryRequest source,
        ActionListener<EsqlStreamQueryAction.ResultStream> resultStreamListener,
        boolean dropNullColumns,
        int batchSize,
        Consumer<PageStreamPublisher.StreamFooter> preHeaderFailureFooterConsumer
    ) {
        super(source);
        this.resultStreamListener = resultStreamListener;
        this.dropNullColumns = dropNullColumns;
        this.batchSize = batchSize;
        this.preHeaderFailureFooterConsumer = preHeaderFailureFooterConsumer;
    }

    public ActionListener<EsqlStreamQueryAction.ResultStream> resultStreamListener() {
        return resultStreamListener;
    }

    public Consumer<PageStreamPublisher.StreamFooter> preHeaderFailureFooterConsumer() {
        return preHeaderFailureFooterConsumer;
    }

    public boolean dropNullColumns() {
        return dropNullColumns;
    }

    public int batchSize() {
        return batchSize;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        if (batchSize < 1) {
            e = addValidationError("[batch_size] must be greater than or equal to 1", e);
        }
        return e;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}
