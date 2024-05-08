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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class PublishHeapMemoryMetricsRequest extends MasterNodeRequest<PublishHeapMemoryMetricsRequest> {

    private final HeapMemoryUsage heapMemoryUsage;

    public PublishHeapMemoryMetricsRequest(final HeapMemoryUsage heapMemoryUsage) {
        super(TimeValue.MINUS_ONE);
        this.heapMemoryUsage = heapMemoryUsage;
    }

    public HeapMemoryUsage getHeapMemoryUsage() {
        return heapMemoryUsage;
    }

    public PublishHeapMemoryMetricsRequest(StreamInput in) throws IOException {
        super(in);
        heapMemoryUsage = new HeapMemoryUsage(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        heapMemoryUsage.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PublishHeapMemoryMetricsRequest that = (PublishHeapMemoryMetricsRequest) o;
        return this.heapMemoryUsage.equals(that.heapMemoryUsage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(heapMemoryUsage);
    }

    @Override
    public String toString() {
        return "PublishHeapMemoryMetricsRequest{heapMemoryMetrics='" + heapMemoryUsage + "}'";
    }
}
