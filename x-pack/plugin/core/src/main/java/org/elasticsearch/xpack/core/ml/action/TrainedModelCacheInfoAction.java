/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TrainedModelCacheInfoAction extends ActionType<TrainedModelCacheInfoAction.Response> {

    public static final TrainedModelCacheInfoAction INSTANCE = new TrainedModelCacheInfoAction();
    public static final String NAME = "cluster:internal/xpack/ml/trained_models/cache/info";

    private TrainedModelCacheInfoAction() {
        super(NAME, Response::new);
    }

    public static class Request extends BaseNodesRequest<Request> {

        public Request(DiscoveryNode... concreteNodes) {
            super(concreteNodes);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(concreteNodes());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Arrays.deepEquals(concreteNodes(), other.concreteNodes());
        }
    }

    public static class Response extends BaseNodesResponse<Response.CacheInfo> {

        public static class CacheInfo extends BaseNodeResponse implements Writeable {

            private final ByteSizeValue jvmInferenceMax;
            private final ByteSizeValue jvmInference;

            public CacheInfo(DiscoveryNode node, ByteSizeValue jvmInferenceMax, ByteSizeValue jvmInference) {
                super(node);
                this.jvmInferenceMax = Objects.requireNonNull(jvmInferenceMax);
                this.jvmInference = Objects.requireNonNull(jvmInference);
            }

            public CacheInfo(StreamInput in) throws IOException {
                super(in);
                jvmInferenceMax = new ByteSizeValue(in);
                jvmInference = new ByteSizeValue(in);
            }

            public ByteSizeValue getJvmInferenceMax() {
                return jvmInferenceMax;
            }

            public ByteSizeValue getJvmInference() {
                return jvmInference;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                jvmInferenceMax.writeTo(out);
                jvmInference.writeTo(out);
            }

            @Override
            public int hashCode() {
                return Objects.hash(getNode(), jvmInferenceMax, jvmInference);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                CacheInfo cacheInfo = (CacheInfo) o;
                return Objects.equals(getNode(), cacheInfo.getNode())
                    && Objects.equals(jvmInferenceMax, cacheInfo.jvmInferenceMax)
                    && Objects.equals(jvmInference, cacheInfo.jvmInference);
            }
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<CacheInfo> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<CacheInfo> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(CacheInfo::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<CacheInfo> nodes) throws IOException {
            out.writeList(nodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNodes());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(getNodes(), other.getNodes());
        }
    }
}
