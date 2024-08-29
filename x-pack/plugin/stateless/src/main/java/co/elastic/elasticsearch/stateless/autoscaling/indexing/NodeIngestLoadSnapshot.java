/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions;
import co.elastic.elasticsearch.stateless.autoscaling.AutoscalingMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record NodeIngestLoadSnapshot(String nodeId, String nodeName, double load, MetricQuality metricQuality)
    implements
        AutoscalingMetrics {
    public NodeIngestLoadSnapshot(StreamInput in) throws IOException {
        // TODO: Remove version BWC once all nodes are on newer version
        this(maybeReadString(in), maybeReadString(in), in.readDouble(), MetricQuality.readFrom(in));
    }

    public NodeIngestLoadSnapshot(double load, MetricQuality metricQuality) {
        this("", "", load, metricQuality);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: Remove version BWC once all nodes are on newer version
        if (out.getTransportVersion().onOrAfter(ServerlessTransportVersions.NODE_NAME_IN_PUBLISH_INGEST_LOAD_REQUEST)) {
            out.writeString(nodeId);
            out.writeString(nodeName);
        }
        out.writeDouble(load);
        metricQuality.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        serializeMetric(builder, load, metricQuality);
        return builder;
    }

    private static String maybeReadString(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.NODE_NAME_IN_PUBLISH_INGEST_LOAD_REQUEST)) {
            return in.readString();
        } else {
            return "";
        }
    }
}
