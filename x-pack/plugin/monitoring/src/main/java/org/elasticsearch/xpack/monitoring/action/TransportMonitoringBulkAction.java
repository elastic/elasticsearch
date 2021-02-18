/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkDoc;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequest;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkResponse;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.MonitoringService;
import org.elasticsearch.xpack.monitoring.exporter.BytesReferenceMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TransportMonitoringBulkAction extends HandledTransportAction<MonitoringBulkRequest, MonitoringBulkResponse> {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Exporters exportService;
    private final MonitoringService monitoringService;

    @Inject
    public TransportMonitoringBulkAction(ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService, ActionFilters actionFilters, Exporters exportService,
                                         MonitoringService monitoringService) {
        super(MonitoringBulkAction.NAME, transportService, actionFilters, MonitoringBulkRequest::new);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.exportService = exportService;
        this.monitoringService = monitoringService;
    }

    @Override
    protected void doExecute(Task task, MonitoringBulkRequest request, ActionListener<MonitoringBulkResponse> listener) {
        clusterService.state().blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);

        // ignore incoming bulk requests when collection is disabled in ES
        if (monitoringService.isMonitoringActive() == false) {
            listener.onResponse(new MonitoringBulkResponse(0, true));
            return;
        }

        final long timestamp = System.currentTimeMillis();
        final String cluster = clusterService.state().metadata().clusterUUID();

        final DiscoveryNode discoveryNode = clusterService.localNode();
        final MonitoringDoc.Node node = new MonitoringDoc.Node(discoveryNode.getId(),
                                                               discoveryNode.getHostName(),
                                                               discoveryNode.getAddress().toString(),
                                                               discoveryNode.getHostAddress(),
                                                               discoveryNode.getName(), timestamp);

        new AsyncAction(threadPool, request, listener, exportService, cluster, timestamp, node).start();
    }

    static class AsyncAction {

        private final ThreadPool threadPool;
        private final MonitoringBulkRequest request;
        private final ActionListener<MonitoringBulkResponse> listener;
        private final Exporters exportService;
        private final String defaultClusterUUID;
        private final long defaultTimestamp;
        private final MonitoringDoc.Node defaultNode;

        AsyncAction(ThreadPool threadPool,
                    MonitoringBulkRequest request, ActionListener<MonitoringBulkResponse> listener, Exporters exportService,
                    String defaultClusterUUID, long defaultTimestamp, MonitoringDoc.Node defaultNode) {
            this.threadPool = threadPool;
            this.request = request;
            this.listener = listener;
            this.exportService = exportService;
            this.defaultClusterUUID = defaultClusterUUID;
            this.defaultTimestamp = defaultTimestamp;
            this.defaultNode = defaultNode;
        }

        void start() {
            executeExport(createMonitoringDocs(request.getDocs()), System.nanoTime(), listener);
        }

        /**
         * Iterate over the list of {@link MonitoringBulkDoc} to create the corresponding
         * list of {@link MonitoringDoc}.
         */
        Collection<MonitoringDoc> createMonitoringDocs(Collection<MonitoringBulkDoc> bulkDocs) {
            return bulkDocs.stream()
                           .filter(bulkDoc -> bulkDoc.getSystem() != MonitoredSystem.UNKNOWN)
                           .map(this::createMonitoringDoc)
                           .collect(Collectors.toList());
        }

        /**
         * Create a {@link MonitoringDoc} from a {@link MonitoringBulkDoc}.
         *
         * @param bulkDoc the {@link MonitoringBulkDoc}
         * @return the {@link MonitoringDoc} to export
         */
        MonitoringDoc createMonitoringDoc(final MonitoringBulkDoc bulkDoc) {
            final MonitoredSystem system = bulkDoc.getSystem();
            final String type = bulkDoc.getType();
            final String id = bulkDoc.getId();
            final long intervalMillis = bulkDoc.getIntervalMillis();
            final XContentType xContentType = bulkDoc.getXContentType();
            final BytesReference source = bulkDoc.getSource();

            final long timestamp;
            if (bulkDoc.getTimestamp() != 0L) {
                timestamp = bulkDoc.getTimestamp();
            } else {
                timestamp = defaultTimestamp;
            }

            return new BytesReferenceMonitoringDoc(defaultClusterUUID, timestamp, intervalMillis,
                                                   defaultNode, system, type, id, xContentType, source);
        }

        /**
         * Exports the documents
         */
        void executeExport(final Collection<MonitoringDoc> docs, final long startTimeNanos,
                           final ActionListener<MonitoringBulkResponse> delegate) {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new ActionRunnable<MonitoringBulkResponse>(delegate) {
                @Override
                protected void doRun() {
                    exportService.export(
                        docs,
                        ActionListener.wrap(
                            r -> listener.onResponse(response(startTimeNanos)),
                            this::onFailure
                        )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onResponse(response(startTimeNanos, e));
                }
            });
        }
    }

    private static MonitoringBulkResponse response(final long start) {
        return new MonitoringBulkResponse(took(start), false);
    }

    private static MonitoringBulkResponse response(final long start, final Exception e) {
        return new MonitoringBulkResponse(took(start), new MonitoringBulkResponse.Error(e));
    }

    private static long took(final long start) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

}
