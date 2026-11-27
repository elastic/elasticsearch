/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.timeseries;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

final class TsidPrefixHistogramRequest extends BroadcastRequest<TsidPrefixHistogramRequest> {
    final OriginalIndices originalIndices;
    final String[] concreteIndices;
    final List<String> metricFields;
    final long minTimestamp;
    final long maxTimestamp;
    final long bucketMillis;

    TsidPrefixHistogramRequest(
        OriginalIndices originalIndices,
        String[] concreteIndices,
        List<String> metricFields,
        long minTimestamp,
        long maxTimestamp,
        long bucketMillis
    ) {
        super(originalIndices.indices(), originalIndices.indicesOptions());
        this.originalIndices = originalIndices;
        this.concreteIndices = concreteIndices;
        this.metricFields = metricFields;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.bucketMillis = bucketMillis;
    }

    TsidPrefixHistogramRequest(StreamInput in) throws IOException {
        super(in);
        this.originalIndices = OriginalIndices.readOriginalIndices(in);
        this.concreteIndices = in.readStringArray();
        this.metricFields = in.readStringCollectionAsList();
        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
        this.bucketMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeStringArray(concreteIndices);
        out.writeStringCollection(metricFields);
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
        out.writeVLong(bucketMillis);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (metricFields.isEmpty()) {
            validationException = addValidationError("metric fields are required", validationException);
        }
        return validationException;
    }

    @Override
    public CancellableTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "concrete_indices="
                    + Arrays.toString(concreteIndices)
                    + ", metric_fields="
                    + metricFields
                    + ", time_range=["
                    + minTimestamp
                    + ", "
                    + maxTimestamp
                    + "], bucket_millis="
                    + bucketMillis;
            }
        };
    }
}
