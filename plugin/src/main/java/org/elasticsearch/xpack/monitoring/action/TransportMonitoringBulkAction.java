/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.exporter.BytesReferenceMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TransportMonitoringBulkAction extends HandledTransportAction<MonitoringBulkRequest, MonitoringBulkResponse> {

    private final ClusterService clusterService;
    private final Exporters exportService;

    @Inject
    public TransportMonitoringBulkAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver, Exporters exportService) {
        super(settings, MonitoringBulkAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                MonitoringBulkRequest::new);
        this.clusterService = clusterService;
        this.exportService = exportService;
    }

    @Override
    protected void doExecute(MonitoringBulkRequest request, ActionListener<MonitoringBulkResponse> listener) {
        clusterService.state().blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);

        final long timestamp = System.currentTimeMillis();
        final String cluster = clusterService.state().metaData().clusterUUID();

        final DiscoveryNode discoveryNode = clusterService.localNode();
        final MonitoringDoc.Node node = new MonitoringDoc.Node(discoveryNode.getId(),
                                                               discoveryNode.getHostName(),
                                                               discoveryNode.getAddress().toString(),
                                                               discoveryNode.getHostAddress(),
                                                               discoveryNode.getName(), timestamp);

        new AsyncAction(request, listener, exportService, cluster, timestamp, node).start();
    }

    static class AsyncAction {

        private final MonitoringBulkRequest request;
        private final ActionListener<MonitoringBulkResponse> listener;
        private final Exporters exportService;
        private final String defaultClusterUUID;
        private final long defaultTimestamp;
        private final MonitoringDoc.Node defaultNode;

        AsyncAction(MonitoringBulkRequest request, ActionListener<MonitoringBulkResponse> listener, Exporters exportService,
                    String defaultClusterUUID, long defaultTimestamp, MonitoringDoc.Node defaultNode) {
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
                           final ActionListener<MonitoringBulkResponse> listener) {
            try {
                exportService.export(docs, ActionListener.wrap(
                        r -> listener.onResponse(response(startTimeNanos)),
                        e -> listener.onResponse(response(startTimeNanos, e))));
            } catch (Exception e) {
                listener.onResponse(response(startTimeNanos, e));
            }
        }
    }

    private static MonitoringBulkResponse response(final long start) {
        return new MonitoringBulkResponse(took(start));
    }

    private static MonitoringBulkResponse response(final long start, final Exception e) {
        return new MonitoringBulkResponse(took(start), new MonitoringBulkResponse.Error(e));
    }

    private static long took(final long start) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

}
