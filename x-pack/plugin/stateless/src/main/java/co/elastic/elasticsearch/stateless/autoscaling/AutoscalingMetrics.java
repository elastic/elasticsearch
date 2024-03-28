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

package co.elastic.elasticsearch.stateless.autoscaling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public interface AutoscalingMetrics extends ToXContentObject, Writeable {
    default XContentBuilder serializeMetric(XContentBuilder builder, long value, MetricQuality quality) throws IOException {
        builder.startObject();
        builder.field("value", value);
        builder.field("quality", quality.getLabel());
        builder.endObject();
        return builder;
    }

    default XContentBuilder serializeMetric(XContentBuilder builder, double value, MetricQuality quality) throws IOException {
        builder.startObject();
        builder.field("value", value);
        builder.field("quality", quality.getLabel());
        builder.endObject();
        return builder;
    }

    default XContentBuilder serializeMetric(XContentBuilder builder, String name, long value, MetricQuality quality) throws IOException {
        builder.startObject(name);
        builder.field("value", value);
        builder.field("quality", quality.getLabel());
        builder.endObject();
        return builder;
    }

    default XContentBuilder serializeMetric(XContentBuilder builder, String nodeID, double value, MetricQuality quality)
        throws IOException {
        builder.startObject();
        builder.field("nodeID", nodeID);
        builder.field("value", value);
        builder.field("quality", quality.getLabel());
        builder.endObject();
        return builder;
    }
}
