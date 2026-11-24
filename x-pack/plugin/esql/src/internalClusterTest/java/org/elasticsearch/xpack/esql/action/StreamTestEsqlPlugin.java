/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlStreamDeleteAction;
import org.elasticsearch.xpack.esql.streaming.InMemoryStreamStore;
import org.elasticsearch.xpack.esql.streaming.StreamStorage;

import java.util.ArrayList;
import java.util.List;

/**
 * The ES|QL plugin with stream actions that use an {@link InMemoryStreamStore}, until the real storage exists. The storage is
 * created by the transport action, so the test replaces that action, the way a plugin subclass already replaces the license state.
 * <p>
 * The storage is static, so it is shared by the nodes of the test cluster, which run in one JVM. That is what the real storage
 * will be: one storage every node can reach. Streams are deleted by the tests.
 */
public class StreamTestEsqlPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {

    /** The storage the stream actions use. */
    public static final InMemoryStreamStore STORE = new InMemoryStreamStore();

    @Override
    public List<ActionHandler> getActions() {
        List<ActionHandler> actions = new ArrayList<>();
        for (ActionHandler handler : super.getActions()) {
            if (handler.getAction() == EsqlStreamDeleteAction.INSTANCE) {
                actions.add(new ActionHandler(EsqlStreamDeleteAction.INSTANCE, InMemoryTransportEsqlStreamDeleteAction.class));
            } else {
                actions.add(handler);
            }
        }
        return actions;
    }

    /** The stream delete action, deleting from {@link #STORE}. */
    public static class InMemoryTransportEsqlStreamDeleteAction extends TransportEsqlStreamDeleteAction {
        @Inject
        public InMemoryTransportEsqlStreamDeleteAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client
        ) {
            super(transportService, actionFilters, clusterService, threadPool, client);
        }

        @Override
        protected StreamStorage createStreamStorage(ClusterService clusterService, ThreadPool threadPool, Client client) {
            return STORE;
        }
    }
}
