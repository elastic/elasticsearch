/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.node.action.HealthNodeRequest;
import org.elasticsearch.health.node.action.TransportHealthNodeAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * This action allows a node to send their health info to the selected health node.
 * When the health node receives the health info, it will update the internal cache
 * regarding this node.
 */
public class UpdateHealthInfoCacheAction extends ActionType<AcknowledgedResponse> {

    public static class Request extends HealthNodeRequest {
        private final String nodeId;
        @Nullable
        private final DiskHealthInfo diskHealthInfo;
        @Nullable
        private final DataStreamLifecycleHealthInfo dslHealthInfo;

        public Request(String nodeId, DiskHealthInfo diskHealthInfo) {
            this.nodeId = nodeId;
            this.diskHealthInfo = diskHealthInfo;
            this.dslHealthInfo = null;
        }

        public Request(String nodeId, DataStreamLifecycleHealthInfo dslHealthInfo) {
            this.nodeId = nodeId;
            this.diskHealthInfo = null;
            this.dslHealthInfo = dslHealthInfo;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.nodeId = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.HEALTH_INFO_ENRICHED_WITH_DSL_STATUS)) {
                this.diskHealthInfo = in.readOptionalWriteable(DiskHealthInfo::new);
                this.dslHealthInfo = in.readOptionalWriteable(DataStreamLifecycleHealthInfo::new);
            } else {
                // BWC for pre-8.12 the disk health info was mandatory. Evolving this request has proven tricky however we've made use of
                // waiting for all nodes to be on the {@link TransportVersions.HEALTH_INFO_ENRICHED_WITH_DSL_STATUS} transport version
                // before sending any requests to update the health info that'd break the pre HEALTH_INFO_ENRICHED_WITH_DSL_STATUS
                // transport invariant of always having a disk health information in the request
                this.diskHealthInfo = new DiskHealthInfo(in);
                this.dslHealthInfo = null;
            }
        }

        public String getNodeId() {
            return nodeId;
        }

        public DiskHealthInfo getDiskHealthInfo() {
            return diskHealthInfo;
        }

        public DataStreamLifecycleHealthInfo getDslHealthInfo() {
            return dslHealthInfo;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
            if (out.getTransportVersion().onOrAfter(TransportVersions.HEALTH_INFO_ENRICHED_WITH_DSL_STATUS)) {
                out.writeOptionalWriteable(diskHealthInfo);
                out.writeOptionalWriteable(dslHealthInfo);
            } else {
                // BWC for pre-8.12 the disk health info was mandatory. Evolving this request has proven tricky however we've made use of
                // waiting for all nodes to be on the {@link TransportVersions.HEALTH_INFO_ENRICHED_WITH_DSL_STATUS} transport version
                // before sending any requests to update the health info that'd break the pre HEALTH_INFO_ENRICHED_WITH_DSL_STATUS
                // transport invariant of always having a disk health information in the request
                diskHealthInfo.writeTo(out);
            }
        }

        @Override
        public String getDescription() {
            return "Update health info cache for node ["
                + nodeId
                + "] with disk health info ["
                + diskHealthInfo
                + "] and DSL health info"
                + " ["
                + dslHealthInfo
                + "].";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Request request = (Request) o;
            return Objects.equals(nodeId, request.nodeId)
                && Objects.equals(diskHealthInfo, request.diskHealthInfo)
                && Objects.equals(dslHealthInfo, request.dslHealthInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, diskHealthInfo, dslHealthInfo);
        }
    }

    public static final UpdateHealthInfoCacheAction INSTANCE = new UpdateHealthInfoCacheAction();
    public static final String NAME = "cluster:monitor/update/health/info";

    private UpdateHealthInfoCacheAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class TransportAction extends TransportHealthNodeAction<Request, AcknowledgedResponse> {
        private final HealthInfoCache nodeHealthOverview;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            HealthInfoCache nodeHealthOverview
        ) {
            super(
                UpdateHealthInfoCacheAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                UpdateHealthInfoCacheAction.Request::new,
                AcknowledgedResponse::readFrom,
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
            );
            this.nodeHealthOverview = nodeHealthOverview;
        }

        @Override
        protected void healthOperation(
            Task task,
            Request request,
            ClusterState clusterState,
            ActionListener<AcknowledgedResponse> listener
        ) {
            nodeHealthOverview.updateNodeHealth(request.getNodeId(), request.getDiskHealthInfo(), request.getDslHealthInfo());
            listener.onResponse(AcknowledgedResponse.of(true));
        }
    }
}
