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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        new AsyncAction(request, listener, exportService, clusterService).start();
    }

    class AsyncAction {

        private final MonitoringBulkRequest request;
        private final ActionListener<MonitoringBulkResponse> listener;
        private final Exporters exportService;
        private final ClusterService clusterService;

        AsyncAction(MonitoringBulkRequest request, ActionListener<MonitoringBulkResponse> listener,
                           Exporters exportService, ClusterService clusterService) {
            this.request = request;
            this.listener = listener;
            this.exportService = exportService;
            this.clusterService = clusterService;
        }

        void start() {
            executeExport(prepareForExport(request.getDocs()), System.nanoTime(), listener);
        }

        /**
         * Iterate over the documents and set the values of common fields if needed:
         * - cluster UUID
         * - timestamp
         * - source node
         */
        Collection<MonitoringDoc> prepareForExport(Collection<MonitoringBulkDoc> bulkDocs) {
            final long defaultTimestamp = System.currentTimeMillis();
            final String defaultClusterUUID = clusterService.state().metaData().clusterUUID();

            DiscoveryNode discoveryNode = clusterService.localNode();
            final MonitoringDoc.Node defaultNode = new MonitoringDoc.Node(discoveryNode.getId(),
                    discoveryNode.getHostName(), discoveryNode.getAddress().toString(),
                    discoveryNode.getHostAddress(), discoveryNode.getName(),
                    discoveryNode.getAttributes());

            List<MonitoringDoc> docs = new ArrayList<>();
            for (MonitoringBulkDoc bulkDoc : bulkDocs) {
                String clusterUUID;
                if (Strings.hasLength(bulkDoc.getClusterUUID())) {
                    clusterUUID = bulkDoc.getClusterUUID();
                } else {
                    clusterUUID = defaultClusterUUID;
                }

                long timestamp;
                if (bulkDoc.getTimestamp() != 0L) {
                    timestamp = bulkDoc.getTimestamp();
                } else {
                    timestamp = defaultTimestamp;
                }

                MonitoringDoc.Node node;
                if (bulkDoc.getSourceNode() != null) {
                    node = bulkDoc.getSourceNode();
                } else {
                    node = defaultNode;
                }

                // TODO Convert MonitoringBulkDoc to a simple MonitoringDoc when all resolvers are
                // removed and MonitoringBulkDoc does not inherit from MonitoringDoc anymore.
                // Monitoring indices will be resolved here instead of being resolved at export
                // time.
                docs.add(new MonitoringBulkDoc(bulkDoc.getMonitoringId(),
                        bulkDoc.getMonitoringVersion(), bulkDoc.getIndex(), bulkDoc.getType(),
                        bulkDoc.getId(), clusterUUID, timestamp, node, bulkDoc.getSource(),
                        bulkDoc.getXContentType()));
            }
            return docs;
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

    private MonitoringBulkResponse response(final long start) {
        return new MonitoringBulkResponse(took(start));
    }

    private MonitoringBulkResponse response(final long start, final Exception e) {
        return new MonitoringBulkResponse(took(start), new MonitoringBulkResponse.Error(e));
    }

    private long took(final long start) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

}
