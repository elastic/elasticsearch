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

package co.elastic.elasticsearch.stateless.autoscaling.model;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public sealed interface Metric extends ToXContentObject, Writeable {

    static Metric readFrom(StreamInput in) throws IOException {
        byte id = in.readByte();
        return switch (id) {
            case SingularMetric.ID -> new SingularMetric(in);
            case ArrayMetric.ID -> new ArrayMetric(in);
            default -> throw new IllegalStateException("No metric for [" + id + "]");
        };
    }

    static SingularMetric minimum(Object value) {
        return new SingularMetric(value, MetricQuality.MINIMUM);
    }

    static SingularMetric exact(Object value) {
        return new SingularMetric(value, MetricQuality.EXACT);
    }

    static ArrayMetric array(SingularMetric... metrics) {
        return new ArrayMetric(List.of(metrics));
    }

    record SingularMetric(Object value, MetricQuality quality) implements Metric {

        private static final byte ID = 0;

        public SingularMetric(StreamInput in) throws IOException {
            this(in.readGenericValue(), MetricQuality.fromId(in.readByte()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(ID);
            writeMetricTo(out);
        }

        public void writeMetricTo(StreamOutput out) throws IOException {
            out.writeGenericValue(value);
            out.writeByte(quality.getId());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("value", value);
            builder.field("quality", quality.getLabel());
            builder.endObject();
            return builder;
        }
    }

    record ArrayMetric(List<SingularMetric> metrics) implements Metric {

        private static final byte ID = 1;

        public ArrayMetric(StreamInput in) throws IOException {
            this(in.readImmutableList(SingularMetric::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(ID);
            out.writeCollection(metrics, (o, m) -> m.writeMetricTo(o));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (SingularMetric metric : metrics) {
                metric.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }
    }
}
