/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.timeseries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Map;

final class TsidPrefixHistogramNodeResponse extends TransportResponse {
    final TsidPrefixHistogram histogram;
    final Map<ShardId, Exception> failures;

    TsidPrefixHistogramNodeResponse(TsidPrefixHistogram histogram, Map<ShardId, Exception> failures) {
        this.histogram = histogram;
        this.failures = failures;
    }

    TsidPrefixHistogramNodeResponse(StreamInput in) throws IOException {
        this.histogram = new TsidPrefixHistogram(in);
        this.failures = in.readMap(ShardId::new, StreamInput::readException);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        histogram.writeTo(out);
        out.writeMap(failures, StreamOutput::writeWriteable, StreamOutput::writeException);
    }
}
