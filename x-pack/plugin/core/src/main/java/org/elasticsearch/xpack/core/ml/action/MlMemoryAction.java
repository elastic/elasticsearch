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
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MlMemoryAction extends ActionType<MlMemoryAction.Response> {

    public static final MlMemoryAction INSTANCE = new MlMemoryAction();
    public static final String NAME = "cluster:monitor/xpack/ml/memory/stats/get";

    static final String MEM = "mem";
    static final String TOTAL = "total";
    static final String TOTAL_IN_BYTES = "total_in_bytes";
    static final String ADJUSTED_TOTAL = "adjusted_total";
    static final String ADJUSTED_TOTAL_IN_BYTES = "adjusted_total_in_bytes";
    static final String ML = "ml";
    static final String MAX = "max";
    static final String MAX_IN_BYTES = "max_in_bytes";
    static final String NATIVE_CODE_OVERHEAD = "native_code_overhead";
    static final String NATIVE_CODE_OVERHEAD_IN_BYTES = "native_code_overhead_in_bytes";
    static final String ANOMALY_DETECTORS = "anomaly_detectors";
    static final String ANOMALY_DETECTORS_IN_BYTES = "anomaly_detectors_in_bytes";
    static final String DATA_FRAME_ANALYTICS = "data_frame_analytics";
    static final String DATA_FRAME_ANALYTICS_IN_BYTES = "data_frame_analytics_in_bytes";
    static final String NATIVE_INFERENCE = "native_inference";
    static final String NATIVE_INFERENCE_IN_BYTES = "native_inference_in_bytes";
    static final String JVM = "jvm";
    static final String HEAP_MAX = "heap_max";
    static final String HEAP_MAX_IN_BYTES = "heap_max_in_bytes";
    static final String JAVA_INFERENCE_MAX = "java_inference_max";
    static final String JAVA_INFERENCE_MAX_IN_BYTES = "java_inference_max_in_bytes";
    static final String JAVA_INFERENCE = "java_inference";
    static final String JAVA_INFERENCE_IN_BYTES = "java_inference_in_bytes";

    private MlMemoryAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String nodeId;

        public Request(String nodeId) {
            this.nodeId = ExceptionsHelper.requireNonNull(nodeId, "nodeId");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            nodeId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
        }

        public String getNodeId() {
            return nodeId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId);
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
            return Objects.equals(nodeId, other.nodeId);
        }
    }

    public static class Response extends BaseNodesResponse<Response.MlMemoryStats> implements ToXContentFragment {

        public static class MlMemoryStats extends BaseNodeResponse implements ToXContent, Writeable {

            private final ByteSizeValue memTotal;
            private final ByteSizeValue memAdjustedTotal;
            private final ByteSizeValue mlMax;
            private final ByteSizeValue mlNativeCodeOverhead;
            private final ByteSizeValue mlAnomalyDetectors;
            private final ByteSizeValue mlDataFrameAnalytics;
            private final ByteSizeValue mlNativeInference;
            private final ByteSizeValue jvmHeapMax;
            private final ByteSizeValue jvmInferenceMax;
            private final ByteSizeValue jvmInference;

            public MlMemoryStats(
                DiscoveryNode node,
                ByteSizeValue memTotal,
                ByteSizeValue memAdjustedTotal,
                ByteSizeValue mlMax,
                ByteSizeValue mlNativeCodeOverhead,
                ByteSizeValue mlAnomalyDetectors,
                ByteSizeValue mlDataFrameAnalytics,
                ByteSizeValue mlNativeInference,
                ByteSizeValue jvmHeapMax,
                ByteSizeValue jvmInferenceMax,
                ByteSizeValue jvmInference
            ) {
                super(node);
                this.memTotal = Objects.requireNonNull(memTotal);
                this.memAdjustedTotal = Objects.requireNonNull(memAdjustedTotal);
                this.mlMax = Objects.requireNonNull(mlMax);
                this.mlNativeCodeOverhead = Objects.requireNonNull(mlNativeCodeOverhead);
                this.mlAnomalyDetectors = Objects.requireNonNull(mlAnomalyDetectors);
                this.mlDataFrameAnalytics = Objects.requireNonNull(mlDataFrameAnalytics);
                this.mlNativeInference = Objects.requireNonNull(mlNativeInference);
                this.jvmHeapMax = Objects.requireNonNull(jvmHeapMax);
                this.jvmInferenceMax = Objects.requireNonNull(jvmInferenceMax);
                this.jvmInference = Objects.requireNonNull(jvmInference);
            }

            public MlMemoryStats(StreamInput in) throws IOException {
                super(in);
                memTotal = new ByteSizeValue(in);
                memAdjustedTotal = new ByteSizeValue(in);
                mlMax = new ByteSizeValue(in);
                mlNativeCodeOverhead = new ByteSizeValue(in);
                mlAnomalyDetectors = new ByteSizeValue(in);
                mlDataFrameAnalytics = new ByteSizeValue(in);
                mlNativeInference = new ByteSizeValue(in);
                jvmHeapMax = new ByteSizeValue(in);
                jvmInferenceMax = new ByteSizeValue(in);
                jvmInference = new ByteSizeValue(in);
            }

            public ByteSizeValue getMemTotal() {
                return memTotal;
            }

            public ByteSizeValue getMemAdjustedTotal() {
                return memAdjustedTotal;
            }

            public ByteSizeValue getMlMax() {
                return mlMax;
            }

            public ByteSizeValue getMlNativeCodeOverhead() {
                return mlNativeCodeOverhead;
            }

            public ByteSizeValue getMlAnomalyDetectors() {
                return mlAnomalyDetectors;
            }

            public ByteSizeValue getMlDataFrameAnalytics() {
                return mlDataFrameAnalytics;
            }

            public ByteSizeValue getMlNativeInference() {
                return mlNativeInference;
            }

            public ByteSizeValue getJvmHeapMax() {
                return jvmHeapMax;
            }

            public ByteSizeValue getJvmInferenceMax() {
                return jvmInferenceMax;
            }

            public ByteSizeValue getJvmInference() {
                return jvmInference;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                DiscoveryNode node = getNode();
                builder.startObject(node.getId());
                builder.field("name", node.getName());
                builder.field("ephemeral_id", node.getEphemeralId());
                builder.field("transport_address", node.getAddress().toString());

                builder.startObject("attributes");
                for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                    if (entry.getKey().startsWith("ml.")) {
                        builder.field(entry.getKey(), entry.getValue());
                    }
                }
                builder.endObject();

                builder.startArray("roles");
                for (DiscoveryNodeRole role : node.getRoles()) {
                    builder.value(role.roleName());
                }
                builder.endArray();

                builder.startObject(MEM);

                builder.humanReadableField(TOTAL_IN_BYTES, TOTAL, memTotal);
                builder.humanReadableField(ADJUSTED_TOTAL_IN_BYTES, ADJUSTED_TOTAL, memAdjustedTotal);

                builder.startObject(ML);
                builder.humanReadableField(MAX_IN_BYTES, MAX, mlMax);
                builder.humanReadableField(NATIVE_CODE_OVERHEAD_IN_BYTES, NATIVE_CODE_OVERHEAD, mlNativeCodeOverhead);
                builder.humanReadableField(ANOMALY_DETECTORS_IN_BYTES, ANOMALY_DETECTORS, mlAnomalyDetectors);
                builder.humanReadableField(DATA_FRAME_ANALYTICS_IN_BYTES, DATA_FRAME_ANALYTICS, mlDataFrameAnalytics);
                builder.humanReadableField(NATIVE_INFERENCE_IN_BYTES, NATIVE_INFERENCE, mlNativeInference);
                builder.endObject();

                builder.endObject(); // end mem

                builder.startObject(JVM);
                builder.humanReadableField(HEAP_MAX_IN_BYTES, HEAP_MAX, jvmHeapMax);
                builder.humanReadableField(JAVA_INFERENCE_MAX_IN_BYTES, JAVA_INFERENCE_MAX, jvmInferenceMax);
                builder.humanReadableField(JAVA_INFERENCE_IN_BYTES, JAVA_INFERENCE, jvmInference);
                builder.endObject();

                builder.endObject(); // end node
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                memTotal.writeTo(out);
                memAdjustedTotal.writeTo(out);
                mlMax.writeTo(out);
                mlNativeCodeOverhead.writeTo(out);
                mlAnomalyDetectors.writeTo(out);
                mlDataFrameAnalytics.writeTo(out);
                mlNativeInference.writeTo(out);
                jvmHeapMax.writeTo(out);
                jvmInferenceMax.writeTo(out);
                jvmInference.writeTo(out);
            }

            @Override
            public int hashCode() {
                return Objects.hash(
                    getNode(),
                    memTotal,
                    memAdjustedTotal,
                    mlMax,
                    mlNativeCodeOverhead,
                    mlAnomalyDetectors,
                    mlDataFrameAnalytics,
                    mlNativeInference,
                    jvmHeapMax,
                    jvmInferenceMax,
                    jvmInference
                );
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                MlMemoryStats that = (MlMemoryStats) o;
                return Objects.equals(getNode(), that.getNode())
                    && Objects.equals(memTotal, that.memTotal)
                    && Objects.equals(memAdjustedTotal, that.memAdjustedTotal)
                    && Objects.equals(mlMax, that.mlMax)
                    && Objects.equals(mlNativeCodeOverhead, that.mlNativeCodeOverhead)
                    && Objects.equals(mlAnomalyDetectors, that.mlAnomalyDetectors)
                    && Objects.equals(mlDataFrameAnalytics, that.mlDataFrameAnalytics)
                    && Objects.equals(mlNativeInference, that.mlNativeInference)
                    && Objects.equals(jvmHeapMax, that.jvmHeapMax)
                    && Objects.equals(jvmInferenceMax, that.jvmInferenceMax)
                    && Objects.equals(jvmInference, that.jvmInference);
            }

            @Override
            public String toString() {
                return Strings.toString(this);
            }
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<MlMemoryStats> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<MlMemoryStats> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(MlMemoryStats::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<MlMemoryStats> nodes) throws IOException {
            out.writeList(nodes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("nodes");
            for (MlMemoryStats mlMemoryStats : getNodes()) {
                mlMemoryStats.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
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

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
