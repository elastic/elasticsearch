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

package org.elasticsearch.xpack.stateless.autoscaling.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.stateless.autoscaling.AbstractBaseTierMetrics;
import org.elasticsearch.xpack.stateless.autoscaling.AutoscalingMetrics;

import java.io.IOException;
import java.util.Objects;

/**
 * Metrics related to linked projects in the context of autoscaling search tiers.
 * Supports a success form (total linked memory and data size) and a failure form (reason + exception).
 */
public class LinkedProjectsMetrics extends AbstractBaseTierMetrics implements AutoscalingMetrics {

    private final Long totalLinkedMemoryInBytes;
    private final Long totalLinkedDataSizeInBytes;

    public LinkedProjectsMetrics(long totalLinkedMemoryInBytes, long totalLinkedDataSizeInBytes) {
        super();
        this.totalLinkedMemoryInBytes = totalLinkedMemoryInBytes;
        this.totalLinkedDataSizeInBytes = totalLinkedDataSizeInBytes;
    }

    public LinkedProjectsMetrics(String reason, ElasticsearchException exception) {
        super(reason, exception);
        this.totalLinkedMemoryInBytes = null;
        this.totalLinkedDataSizeInBytes = null;
    }

    public LinkedProjectsMetrics(StreamInput in) throws IOException {
        super(in);
        this.totalLinkedMemoryInBytes = in.readOptionalLong();
        this.totalLinkedDataSizeInBytes = in.readOptionalLong();
    }

    public Long getTotalLinkedMemoryInBytes() {
        return totalLinkedMemoryInBytes;
    }

    public Long getTotalLinkedDataSizeInBytes() {
        return totalLinkedDataSizeInBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalLong(totalLinkedMemoryInBytes);
        out.writeOptionalLong(totalLinkedDataSizeInBytes);
    }

    @Override
    protected XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("total_linked_memory_in_bytes", totalLinkedMemoryInBytes);
        builder.field("total_linked_data_size_in_bytes", totalLinkedDataSizeInBytes);
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
        LinkedProjectsMetrics that = (LinkedProjectsMetrics) other;
        return Objects.equals(totalLinkedMemoryInBytes, that.totalLinkedMemoryInBytes)
            && Objects.equals(totalLinkedDataSizeInBytes, that.totalLinkedDataSizeInBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalLinkedMemoryInBytes, totalLinkedDataSizeInBytes);
    }

    @Override
    public String toString() {
        return "LinkedProjectMetrics{"
            + "totalLinkedMemoryInBytes="
            + totalLinkedMemoryInBytes
            + ", totalLinkedDataSizeInBytes="
            + totalLinkedDataSizeInBytes
            + '}';
    }
}
