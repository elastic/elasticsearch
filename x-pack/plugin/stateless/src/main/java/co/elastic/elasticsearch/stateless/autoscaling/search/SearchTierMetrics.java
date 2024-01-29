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

import co.elastic.elasticsearch.stateless.autoscaling.AbstractBaseTierMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.AutoscalingMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetrics;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SearchTierMetrics extends AbstractBaseTierMetrics implements AutoscalingMetrics {
    private final MemoryMetrics memoryMetrics;
    private final MaxShardCopies maxShardCopies;
    private final StorageMetrics storageMetrics;

    public SearchTierMetrics(MemoryMetrics memoryMetrics, MaxShardCopies maxShardCopies, StorageMetrics storageMetrics) {
        super();
        this.memoryMetrics = memoryMetrics;
        this.maxShardCopies = maxShardCopies;
        this.storageMetrics = storageMetrics;
    }

    public SearchTierMetrics(String reason, ElasticsearchException exception) {
        super(reason, exception);
        this.memoryMetrics = null;
        this.maxShardCopies = null;
        this.storageMetrics = null;
    }

    public SearchTierMetrics(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(TransportVersions.V_8_11_X)) {
            this.memoryMetrics = new MemoryMetrics(in);
            this.maxShardCopies = new MaxShardCopies(in);
            this.storageMetrics = new StorageMetrics(in);
            return;
        }

        this.memoryMetrics = in.readOptionalWriteable(MemoryMetrics::new);
        this.maxShardCopies = in.readOptionalWriteable(MaxShardCopies::new);
        this.storageMetrics = in.readOptionalWriteable(StorageMetrics::new);
    }

    public MemoryMetrics getMemoryMetrics() {
        return memoryMetrics;
    }

    public MaxShardCopies getMaxShardCopies() {
        return maxShardCopies;
    }

    public StorageMetrics getStorageMetrics() {
        return storageMetrics;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.V_8_11_X)) {
            memoryMetrics.writeTo(out);
            maxShardCopies.writeTo(out);
            storageMetrics.writeTo(out);
            return;
        }
        out.writeOptionalWriteable(memoryMetrics);
        out.writeOptionalWriteable(maxShardCopies);
        out.writeOptionalWriteable(storageMetrics);
    }

    public XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.object("metrics", (objectBuilder) -> {
            memoryMetrics.toXContent(objectBuilder, params);
            maxShardCopies.toXContent(objectBuilder, params);
            storageMetrics.toXContent(objectBuilder, params);
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

        final SearchTierMetrics that = (SearchTierMetrics) other;

        return Objects.equals(this.memoryMetrics, that.memoryMetrics)
            && Objects.equals(this.maxShardCopies, that.maxShardCopies)
            && Objects.equals(this.storageMetrics, that.storageMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memoryMetrics, maxShardCopies, storageMetrics);
    }
}
