/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

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
        heapMemoryUsage = HeapMemoryUsage.from(in);
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
