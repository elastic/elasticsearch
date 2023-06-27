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

import co.elastic.elasticsearch.stateless.autoscaling.AutoscalingMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public record IndexTierMetrics(List<NodeIngestLoadSnapshot> nodesLoad, MemoryMetrics memoryMetrics) implements AutoscalingMetrics {

    public IndexTierMetrics(StreamInput in) throws IOException {
        this(in.readList(NodeIngestLoadSnapshot::new), new MemoryMetrics(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(nodesLoad);
        memoryMetrics.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.object("metrics", (objectBuilder) -> {
            objectBuilder.xContentList("indexing_load", nodesLoad);
            memoryMetrics.toXContent(objectBuilder, params);
        });

        builder.endObject();
        return builder;
    }
}
