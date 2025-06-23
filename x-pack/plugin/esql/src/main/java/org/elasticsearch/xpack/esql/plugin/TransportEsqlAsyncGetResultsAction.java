/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlAsyncGetResultAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlQueryTask;
import org.elasticsearch.xpack.esql.core.plugin.AbstractTransportQlAsyncGetResultsAction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;

public class TransportEsqlAsyncGetResultsAction extends AbstractTransportQlAsyncGetResultsAction<EsqlQueryResponse, EsqlQueryTask> {

    private final BlockFactory blockFactory;

    @Inject
    public TransportEsqlAsyncGetResultsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays,
        BlockFactoryProvider blockFactoryProvider
    ) {
        super(
            EsqlAsyncGetResultAction.NAME,
            transportService,
            actionFilters,
            clusterService,
            registry,
            client,
            threadPool,
            bigArrays,
            EsqlQueryTask.class
        );
        this.blockFactory = blockFactoryProvider.blockFactory();
    }

    @Override
    protected void doExecute(Task task, GetAsyncResultRequest request, ActionListener<EsqlQueryResponse> listener) {
        super.doExecute(task, request, unwrapListener(listener));
    }

    @Override
    public Writeable.Reader<EsqlQueryResponse> responseReader() {
        return EsqlQueryResponse.reader(blockFactory);
    }

    static final String PARSE_EX_NAME = ElasticsearchException.getExceptionName(new ParsingException(Source.EMPTY, ""));
    static final String VERIFY_EX_NAME = ElasticsearchException.getExceptionName(new VerificationException(""));

    /**
     * Unwraps the exception in the case of failure. This keeps the exception types
     * the same as the sync API, namely ParsingException and VerificationException.
     */
    static <R> ActionListener<R> unwrapListener(ActionListener<R> listener) {
        return new ActionListener<>() {
            @Override
            public void onResponse(R o) {
                listener.onResponse(o);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ElasticsearchWrapperException && e instanceof ElasticsearchException ee) {
                    e = unwrapEsException(ee);
                }
                if (e instanceof NotSerializableExceptionWrapper wrapper) {
                    String name = wrapper.getExceptionName();
                    if (PARSE_EX_NAME.equals(name)) {
                        e = new ParsingException(Source.EMPTY, e.getMessage());
                        e.setStackTrace(wrapper.getStackTrace());
                        e.addSuppressed(wrapper);
                    } else if (VERIFY_EX_NAME.contains(name)) {
                        e = new VerificationException(e.getMessage());
                        e.setStackTrace(wrapper.getStackTrace());
                        e.addSuppressed(wrapper);
                    }
                }
                listener.onFailure(e);
            }
        };
    }

    static RuntimeException unwrapEsException(ElasticsearchException esEx) {
        Throwable root = esEx.unwrapCause();
        if (root instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return esEx;
    }
}
