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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.node.action.HealthNodeRequest;
import org.elasticsearch.health.node.action.TransportHealthNodeAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Locale;
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
        @Nullable
        private final RepositoriesHealthInfo repositoriesHealthInfo;

        public Request(
            String nodeId,
            DiskHealthInfo diskHealthInfo,
            DataStreamLifecycleHealthInfo dslHealthInfo,
            RepositoriesHealthInfo repositoriesHealthInfo
        ) {
            this.nodeId = nodeId;
            this.diskHealthInfo = diskHealthInfo;
            this.dslHealthInfo = dslHealthInfo;
            this.repositoriesHealthInfo = repositoriesHealthInfo;
        }

        public Request(String nodeId, DataStreamLifecycleHealthInfo dslHealthInfo) {
            this.nodeId = nodeId;
            this.diskHealthInfo = null;
            this.repositoriesHealthInfo = null;
            this.dslHealthInfo = dslHealthInfo;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.nodeId = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                this.diskHealthInfo = in.readOptionalWriteable(DiskHealthInfo::new);
                this.dslHealthInfo = in.readOptionalWriteable(DataStreamLifecycleHealthInfo::new);
                this.repositoriesHealthInfo = in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)
                    ? in.readOptionalWriteable(RepositoriesHealthInfo::new)
                    : null;
            } else {
                // BWC for pre-8.12 the disk health info was mandatory. Evolving this request has proven tricky however we've made use of
                // waiting for all nodes to be on the {@link TransportVersions.HEALTH_INFO_ENRICHED_WITH_DSL_STATUS} transport version
                // before sending any requests to update the health info that'd break the pre HEALTH_INFO_ENRICHED_WITH_DSL_STATUS
                // transport invariant of always having a disk health information in the request
                this.diskHealthInfo = new DiskHealthInfo(in);
                this.dslHealthInfo = null;
                this.repositoriesHealthInfo = null;
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

        public RepositoriesHealthInfo getRepositoriesHealthInfo() {
            return repositoriesHealthInfo;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeOptionalWriteable(diskHealthInfo);
                out.writeOptionalWriteable(dslHealthInfo);
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                    out.writeOptionalWriteable(repositoriesHealthInfo);
                }
            } else {
                // BWC for pre-8.12 the disk health info was mandatory. Evolving this request has proven tricky however we've made use of
                // waiting for all nodes to be on the {@link TransportVersions.V_8_12_0} transport version
                // before sending any requests to update the health info that'd break the pre-8.12
                // transport invariant of always having a disk health information in the request
                diskHealthInfo.writeTo(out);
            }
        }

        @Override
        public String getDescription() {
            return String.format(
                Locale.ROOT,
                "Update health info cache for node [%s] with disk health info [%s], DSL health info [%s], repositories health info [%s].",
                nodeId,
                diskHealthInfo,
                dslHealthInfo,
                repositoriesHealthInfo
            );
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
                && Objects.equals(dslHealthInfo, request.dslHealthInfo)
                && Objects.equals(repositoriesHealthInfo, request.repositoriesHealthInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, diskHealthInfo, dslHealthInfo, repositoriesHealthInfo);
        }

        public static class Builder {
            private String nodeId;
            private DiskHealthInfo diskHealthInfo;
            private RepositoriesHealthInfo repositoriesHealthInfo;
            private DataStreamLifecycleHealthInfo dslHealthInfo;

            public Builder nodeId(String nodeId) {
                this.nodeId = nodeId;
                return this;
            }

            public Builder diskHealthInfo(DiskHealthInfo diskHealthInfo) {
                this.diskHealthInfo = diskHealthInfo;
                return this;
            }

            public Builder repositoriesHealthInfo(RepositoriesHealthInfo repositoriesHealthInfo) {
                this.repositoriesHealthInfo = repositoriesHealthInfo;
                return this;
            }

            public Builder dslHealthInfo(DataStreamLifecycleHealthInfo dslHealthInfo) {
                this.dslHealthInfo = dslHealthInfo;
                return this;
            }

            public Request build() {
                return new Request(nodeId, diskHealthInfo, dslHealthInfo, repositoriesHealthInfo);
            }
        }
    }

    public static final UpdateHealthInfoCacheAction INSTANCE = new UpdateHealthInfoCacheAction();
    public static final String NAME = "cluster:monitor/update/health/info";

    private UpdateHealthInfoCacheAction() {
        super(NAME);
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
            nodeHealthOverview.updateNodeHealth(
                request.getNodeId(),
                request.getDiskHealthInfo(),
                request.getDslHealthInfo(),
                request.getRepositoriesHealthInfo()
            );
            listener.onResponse(AcknowledgedResponse.of(true));
        }
    }
}
