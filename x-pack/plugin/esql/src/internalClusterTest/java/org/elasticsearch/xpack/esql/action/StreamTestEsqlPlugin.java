/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlStreamGetResultsAction;
import org.elasticsearch.xpack.esql.streaming.InMemoryStreamStore;
import org.elasticsearch.xpack.esql.streaming.StreamStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * The ES|QL plugin with a stream GET that reads from an {@link InMemoryStreamStore}, until the real storage exists. The storage is
 * created by the transport action, so the test replaces that action, the way a plugin subclass already replaces the license state.
 * <p>
 * The storage is shared by the nodes of the test cluster, which has a single node, and streams are deleted by the tests.
 */
public class StreamTestEsqlPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {

    /** The storage the stream GET reads from. */
    public static final ObservedStore STORE = new ObservedStore();

    /**
     * The in-memory storage, which also tells a test when a GET is waiting. A GET that waits for rows polls the storage, so a read that
     * found no rows is the sign that the GET is waiting, and a test that waits for that sign does not have to sleep and hope.
     */
    public static final class ObservedStore extends InMemoryStreamStore {
        private final Map<String, Semaphore> emptyReads = new ConcurrentHashMap<>();

        private Semaphore emptyReads(String streamId) {
            return emptyReads.computeIfAbsent(streamId, id -> new Semaphore(0));
        }

        @Override
        public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
            super.getPages(streamId, offset, rows, listener.map(pages -> {
                if (pages.isEmpty()) {
                    emptyReads(streamId).release();
                }
                return pages;
            }));
        }

        /** Blocks until one more read of the stream has found no rows. */
        public void awaitEmptyRead(String streamId) {
            ESTestCase.safeAcquire(emptyReads(streamId));
        }

        /** Forgets the reads of earlier tests. */
        public void resetEmptyReads() {
            emptyReads.clear();
        }
    }

    @Override
    public List<ActionHandler> getActions() {
        List<ActionHandler> actions = new ArrayList<>();
        for (ActionHandler handler : super.getActions()) {
            if (handler.getAction() == EsqlStreamGetResultAction.INSTANCE) {
                actions.add(new ActionHandler(EsqlStreamGetResultAction.INSTANCE, InMemoryTransportEsqlStreamGetResultsAction.class));
            } else {
                actions.add(handler);
            }
        }
        return actions;
    }

    /** The stream GET action, reading from {@link #STORE}. */
    public static class InMemoryTransportEsqlStreamGetResultsAction extends TransportEsqlStreamGetResultsAction {
        @Inject
        public InMemoryTransportEsqlStreamGetResultsAction(
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
