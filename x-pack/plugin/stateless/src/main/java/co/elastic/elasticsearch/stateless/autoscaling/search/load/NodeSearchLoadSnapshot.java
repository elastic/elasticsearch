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

package co.elastic.elasticsearch.stateless.autoscaling.search.load;

import co.elastic.elasticsearch.stateless.autoscaling.AutoscalingMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record NodeSearchLoadSnapshot(String nodeId, double load, MetricQuality metricQuality) implements AutoscalingMetrics {
    public NodeSearchLoadSnapshot(StreamInput in) throws IOException {
        this(in.readString(), in.readDouble(), MetricQuality.readFrom(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeDouble(load);
        metricQuality.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        serializeMetric(builder, nodeId, load, metricQuality);
        return builder;
    }
}
