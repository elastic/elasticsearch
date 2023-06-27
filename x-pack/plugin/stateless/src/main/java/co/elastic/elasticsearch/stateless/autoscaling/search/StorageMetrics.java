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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.stateless.autoscaling.AutoscalingMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record StorageMetrics(
    long maxInteractiveSizeInBytes,
    long totalInteractiveDataSizeInBytes,
    long totalDataSizeInBytes,
    MetricQuality quality
) implements AutoscalingMetrics {

    public StorageMetrics(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong(), in.readLong(), MetricQuality.readFrom(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(maxInteractiveSizeInBytes);
        out.writeLong(totalInteractiveDataSizeInBytes);
        out.writeLong(totalDataSizeInBytes);
        quality.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("max_interactive_data_size_in_bytes");
        serializeMetric(builder, maxInteractiveSizeInBytes, quality);

        builder.field("total_interactive_data_size_in_bytes");
        serializeMetric(builder, totalInteractiveDataSizeInBytes, quality);

        builder.field("total_data_size_in_bytes");
        serializeMetric(builder, totalDataSizeInBytes, quality);
        return builder;
    }
}
