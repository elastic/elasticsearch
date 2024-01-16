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

import co.elastic.elasticsearch.stateless.autoscaling.AbstractBaseTierMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.AutoscalingMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetrics;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class IndexTierMetrics extends AbstractBaseTierMetrics implements AutoscalingMetrics {

    private final List<NodeIngestLoadSnapshot> nodesLoad;
    private final MemoryMetrics memoryMetrics;

    public IndexTierMetrics(List<NodeIngestLoadSnapshot> nodesLoad, MemoryMetrics memoryMetrics) {
        this.nodesLoad = nodesLoad;
        this.memoryMetrics = memoryMetrics;
    }

    public IndexTierMetrics(String reason, ElasticsearchException exception) {
        super(reason, exception);
        this.nodesLoad = null;
        this.memoryMetrics = null;
    }

    public IndexTierMetrics(StreamInput in) throws IOException {
        super(in);
        this.nodesLoad = in.readOptionalCollectionAsList(NodeIngestLoadSnapshot::new);
        this.memoryMetrics = in.readOptionalWriteable(MemoryMetrics::new);
    }

    public List<NodeIngestLoadSnapshot> getNodesLoad() {
        return nodesLoad;
    }

    public MemoryMetrics getMemoryMetrics() {
        return memoryMetrics;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalCollection(nodesLoad);
        out.writeOptionalWriteable(memoryMetrics);
    }

    public XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.object("metrics", (objectBuilder) -> {
            objectBuilder.xContentList("indexing_load", nodesLoad);
            memoryMetrics.toXContent(objectBuilder, params);
        });
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final IndexTierMetrics that = (IndexTierMetrics) other;

        return Objects.equals(this.nodesLoad, that.nodesLoad) && Objects.equals(this.memoryMetrics, that.memoryMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodesLoad, memoryMetrics);
    }
}
